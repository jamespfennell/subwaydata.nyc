package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jamespfennell/subwaydata.nyc/etl/config"
	"github.com/jamespfennell/subwaydata.nyc/metadata"
)

// Client writes data to all configured buckets.
//
// The metadata in the primary bucket is the source of truth for which days have been processed.
// All writes, including metadata updates, go to every bucket and fail if any bucket fails.
type Client struct {
	metadataPath  string
	primary       bucket
	secondaries   []bucket
	metadataMutex sync.Mutex
}

// bucket is a single object storage bucket.
type bucket interface {
	// put writes the bytes to the object at the provided path.
	put(ctx context.Context, b []byte, remotePath string) error
	// get reads the object at the provided path. It returns errNotFound if the object does not exist.
	get(ctx context.Context, remotePath string) ([]byte, error)
}

var errNotFound = errors.New("object not found")

func NewClient(ec *config.Config) (*Client, error) {
	var primary bucket
	var secondaries []bucket
	for _, bc := range ec.Buckets {
		b, err := newS3Bucket(bc)
		if err != nil {
			return nil, err
		}
		if bc.Primary {
			primary = b
		} else {
			secondaries = append(secondaries, b)
		}
	}
	if primary == nil {
		return nil, fmt.Errorf("no primary bucket configured")
	}
	return newClient(ec.MetadataPath, primary, secondaries), nil
}

func newClient(metadataPath string, primary bucket, secondaries []bucket) *Client {
	return &Client{
		metadataPath: metadataPath,
		primary:      primary,
		secondaries:  secondaries,
	}
}

// Write writes the bytes to the provided path in all buckets.
func (c *Client) Write(ctx context.Context, b []byte, remotePath string) error {
	return putAll(ctx, append([]bucket{c.primary}, c.secondaries...), b, remotePath)
}

// putAll writes the bytes to all of the buckets in parallel.
func putAll(ctx context.Context, buckets []bucket, b []byte, remotePath string) error {
	errs := make([]error, len(buckets))
	var wg sync.WaitGroup
	for i, bkt := range buckets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = bkt.put(ctx, b, remotePath)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

// GetMetadata reads the metadata from the primary bucket.
func (c *Client) GetMetadata(ctx context.Context) (*metadata.Metadata, error) {
	b, err := c.primary.get(ctx, c.metadataPath)
	if errors.Is(err, errNotFound) {
		return &metadata.Metadata{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata from object storage: %w", err)
	}
	var m metadata.Metadata
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

type UpdateMetadataFunc func(*metadata.Metadata) bool

// UpdateMetadata updates the metadata stored in the object storage.
//
// The metadata is read from the primary bucket and written to all buckets.
// The secondary buckets are written first, so that if any write fails the primary
// bucket still reflects the old state and the update will be retried.
func (c *Client) UpdateMetadata(ctx context.Context, f UpdateMetadataFunc) error {
	c.metadataMutex.Lock()
	defer c.metadataMutex.Unlock()
	m, err := c.GetMetadata(ctx)
	if err != nil {
		return err
	}
	if commit := f(m); !commit {
		return nil
	}
	sort.Sort(sort.Reverse(byDay(m.ProcessedDays)))
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	if err := putAll(ctx, c.secondaries, b, c.metadataPath); err != nil {
		return fmt.Errorf("failed to write metadata to secondary buckets: %w", err)
	}
	return c.primary.put(ctx, b, c.metadataPath)
}

type s3Bucket struct {
	bc config.Bucket
	sc *s3.S3
}

func newS3Bucket(bc config.Bucket) (*s3Bucket, error) {
	region := bc.Region
	if region == "" {
		region = "us-east-1"
	}
	s3Config := &aws.Config{
		Credentials: credentials.NewStaticCredentials(bc.AccessKey, bc.SecretKey, ""),
		Endpoint:    aws.String(bc.Url),
		Region:      aws.String(region),
	}
	newSession, err := session.NewSession(s3Config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize object storage client for bucket %s: %w", bc.Name, err)
	}
	return &s3Bucket{bc: bc, sc: s3.New(newSession)}, nil
}

func (b *s3Bucket) put(ctx context.Context, data []byte, remotePath string) error {
	ctx, cancel := context.WithDeadline(ctx, time.Now().UTC().Add(5*60*time.Second))
	defer cancel()
	_, err := b.sc.PutObjectWithContext(ctx, b.putObjectInput(data, remotePath))
	if err != nil {
		return fmt.Errorf("failed to copy bytes to bucket %s: %w", b.bc.Name, err)
	}
	return nil
}

func (b *s3Bucket) putObjectInput(data []byte, remotePath string) *s3.PutObjectInput {
	input := &s3.PutObjectInput{
		Bucket: aws.String(b.bc.Name),
		Key:    aws.String(filepath.Join(b.bc.Prefix, remotePath)),
		Body:   bytes.NewReader(data),
	}
	if b.bc.Acl != "" {
		input.ACL = aws.String(b.bc.Acl)
	}
	return input
}

func (b *s3Bucket) get(ctx context.Context, remotePath string) ([]byte, error) {
	ctx, cancel := context.WithDeadline(ctx, time.Now().UTC().Add(5*60*time.Second))
	defer cancel()
	o, err := b.sc.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bc.Name),
		Key:    aws.String(filepath.Join(b.bc.Prefix, remotePath)),
	})
	if err != nil {
		if a, ok := err.(awserr.Error); ok && a.Code() == s3.ErrCodeNoSuchKey {
			return nil, errNotFound
		}
		return nil, fmt.Errorf("failed to read from bucket %s: %w", b.bc.Name, err)
	}
	defer o.Body.Close()
	return io.ReadAll(o.Body)
}

type byDay []metadata.ProcessedDay

func (b byDay) Len() int {
	return len(b)
}

func (b byDay) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b byDay) Less(i, j int) bool {
	return b[i].Day.Before(b[j].Day)
}
