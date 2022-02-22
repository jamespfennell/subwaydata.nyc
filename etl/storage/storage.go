package storage

import (
	"bytes"
	"context"
	"encoding/json"
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

type Client struct {
	ec            *config.Config
	sc            *s3.S3
	metadataMutex sync.RWMutex
}

func NewClient(ec *config.Config) (*Client, error) {
	s3Config := &aws.Config{
		Credentials: credentials.NewStaticCredentials(ec.BucketAccessKey, ec.BucketSecretKey, ""),
		Endpoint:    aws.String(ec.BucketUrl),
		Region:      aws.String("us-east-1"),
	}

	newSession, err := session.NewSession(s3Config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize object storage client: %w", err)
	}
	return &Client{ec: ec, sc: s3.New(newSession)}, nil
}

func (c *Client) Write(ctx context.Context, b []byte, remotePath string) error {
	ctx, cancel := context.WithDeadline(ctx, time.Now().UTC().Add(5*60*time.Second))
	defer cancel()
	object := s3.PutObjectInput{
		Bucket: aws.String(c.ec.BucketName),
		Key:    aws.String(filepath.Join(c.ec.BucketPrefix, remotePath)),
		Body:   bytes.NewReader(b),
		ACL:    aws.String("public-read"),
	}
	_, err := c.sc.PutObjectWithContext(ctx, &object)
	if err != nil {
		return fmt.Errorf("failed to copy bytes to object storage: %w", err)
	}
	return nil
}

func (c *Client) GetMetadata(ctx context.Context) (*metadata.Metadata, error) {
	c.metadataMutex.RLock()
	defer c.metadataMutex.RUnlock()
	ctx, cancel := context.WithDeadline(ctx, time.Now().UTC().Add(5*60*time.Second))
	defer cancel()
	o, err := c.sc.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.ec.BucketName),
		Key:    aws.String(filepath.Join(c.ec.BucketPrefix, c.ec.MetadataPath)),
	})
	if err != nil {
		if a, ok := err.(awserr.Error); ok {
			if a.Code() == s3.ErrCodeNoSuchKey {
				return &metadata.Metadata{}, nil
			}
		}
	}
	defer o.Body.Close()
	b, err := io.ReadAll(o.Body)
	if err != nil {
		return nil, err
	}
	var m metadata.Metadata
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

type UpdateMetadataFunc func(*metadata.Metadata) bool

// UpdateMetadata updates the metadata stored in the object storage.
func (c *Client) UpdateMetadata(ctx context.Context, f UpdateMetadataFunc) error {
	m, err := c.GetMetadata(ctx)
	if err != nil {
		return err
	}
	c.metadataMutex.Lock()
	defer c.metadataMutex.Unlock()
	if commit := f(m); !commit {
		return nil
	}
	sort.Sort(sort.Reverse(byDay(m.ProcessedDays)))
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return c.Write(ctx, b, c.ec.MetadataPath)
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
