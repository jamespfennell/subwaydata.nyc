package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"time"

	"github.com/jamespfennell/subwaydata.nyc/etl/config"
	"github.com/jamespfennell/subwaydata.nyc/metadata"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Client struct {
	ec *config.Config
	sc *minio.Client
}

func NewClient(ec *config.Config) (*Client, error) {
	sc, err := minio.New(ec.BucketUrl, &minio.Options{
		Creds:  credentials.NewStaticV4(ec.BucketAccessKey, ec.BucketSecretKey, ""),
		Secure: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize object storage client: %w", err)
	}
	return &Client{ec: ec, sc: sc}, nil
}

func (c *Client) Write(b []byte, remotePath string) error {
	// TODO: accept a context in the function
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().UTC().Add(30*time.Second))
	defer cancel()
	if _, err := c.sc.PutObject(
		ctx,
		c.ec.BucketName,
		filepath.Join(c.ec.BucketPrefix, remotePath),
		bytes.NewReader(b),
		int64(len(b)),
		minio.PutObjectOptions{
			PartSize: 1024 * 1024 * 30, // TODO: good choice here?
		},
	); err != nil {
		return fmt.Errorf("failed to copy bytes to object storage: %w", err)
	}
	return nil
}

func (c *Client) GetMetadata() (*metadata.Metadata, error) {
	// TODO: accept a context in the function
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().UTC().Add(30*time.Second))
	defer cancel()
	o, err := c.sc.GetObject(
		ctx,
		c.ec.BucketName,
		filepath.Join(c.ec.BucketPrefix, c.ec.MetadataPath),
		minio.GetObjectOptions{},
	)
	if err != nil {
		return nil, err
	}
	defer o.Close()
	b, err := io.ReadAll(o)
	if err != nil {
		if r, ok := err.(minio.ErrorResponse); ok {
			if r.StatusCode == http.StatusNotFound {
				return &metadata.Metadata{}, nil
			}
		}
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
func (c *Client) UpdateMetadata(f UpdateMetadataFunc) error {
	m, err := c.GetMetadata()
	if err != nil {
		return err
	}
	if commit := f(m); !commit {
		return nil
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return c.Write(b, c.ec.MetadataPath)
}
