// Package s3 is the production blob store: any S3-compatible endpoint
// (MinIO in the compose stack). One bucket, keys namespaced as
// {namespace}/{location}.
package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/graphene-ci/graphene/internal/infrastructure/blob"
)

// Options configure the store.
type Options struct {
	Endpoint  string // host[:port]
	Bucket    string
	AccessKey string
	SecretKey string
	UseSSL    bool
}

// Store implements blob.Store over S3.
type Store struct {
	client *minio.Client
	bucket string
}

// New dials the endpoint and ensures the bucket exists.
func New(ctx context.Context, opts Options) (*Store, error) {
	client, err := minio.New(opts.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(opts.AccessKey, opts.SecretKey, ""),
		Secure: opts.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("s3: %w", err)
	}
	exists, err := client.BucketExists(ctx, opts.Bucket)
	if err != nil {
		return nil, fmt.Errorf("s3 bucket: %w", err)
	}
	if !exists {
		if err := client.MakeBucket(ctx, opts.Bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, fmt.Errorf("s3 make bucket: %w", err)
		}
	}
	return &Store{client: client, bucket: opts.Bucket}, nil
}

// Put writes the blob; locations are content-addressed upstream, so a
// repeated Put of the same key is harmless.
func (s *Store) Put(ctx context.Context, namespace, location string, r io.Reader) (int64, error) {
	k, err := key(namespace, location)
	if err != nil {
		return 0, err
	}
	info, err := s.client.PutObject(ctx, s.bucket, k, r, -1, minio.PutObjectOptions{})
	if err != nil {
		return 0, err
	}
	return info.Size, nil
}

// Get opens the blob for reading.
func (s *Store) Get(ctx context.Context, namespace, location string) (io.ReadCloser, error) {
	k, err := key(namespace, location)
	if err != nil {
		return nil, err
	}
	obj, err := s.client.GetObject(ctx, s.bucket, k, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	// GetObject is lazy: probe so absence surfaces here, typed.
	if _, err := obj.Stat(); err != nil {
		_ = obj.Close()
		if notFound(err) {
			return nil, blob.ErrNotFound
		}
		return nil, err
	}
	return obj, nil
}

// Exists reports presence.
func (s *Store) Exists(ctx context.Context, namespace, location string) (bool, error) {
	k, err := key(namespace, location)
	if err != nil {
		return false, err
	}
	if _, err := s.client.StatObject(ctx, s.bucket, k, minio.StatObjectOptions{}); err != nil {
		if notFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Delete removes the bytes; absence is fine.
func (s *Store) Delete(ctx context.Context, namespace, location string) error {
	k, err := key(namespace, location)
	if err != nil {
		return err
	}
	return s.client.RemoveObject(ctx, s.bucket, k, minio.RemoveObjectOptions{})
}

func key(namespace, location string) (string, error) {
	loc := strings.TrimPrefix(location, "/")
	if namespace == "" || loc == "" || strings.Contains(loc, "..") || strings.ContainsAny(namespace, "/\\") {
		return "", fmt.Errorf("bad blob address %q/%q", namespace, location)
	}
	return namespace + "/" + loc, nil
}

func notFound(err error) bool {
	var resp minio.ErrorResponse
	if errors.As(err, &resp) {
		return resp.Code == "NoSuchKey" || resp.StatusCode == 404
	}
	return false
}
