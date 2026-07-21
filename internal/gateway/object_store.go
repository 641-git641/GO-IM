package gateway

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/im/configs"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// ObjectStore is the interface for file/blob storage (MinIO/S3 or in-memory).
type ObjectStore interface {
	// Put stores data at key with the given content type.
	Put(ctx context.Context, key string, data []byte, contentType string) error

	// Get retrieves data and content type for a key.
	// Returns an error if the key does not exist.
	Get(ctx context.Context, key string) (data []byte, contentType string, err error)

	// Delete removes a key from the store.
	Delete(ctx context.Context, key string) error
}

// storedObject holds file data in memory.
type storedObject struct {
	data        []byte
	contentType string
}

// InMemoryObjectStore is an in-memory ObjectStore for testing and development.
// Data is lost on restart.
type InMemoryObjectStore struct {
	mu    sync.RWMutex
	items map[string]*storedObject
}

// NewInMemoryObjectStore creates an empty InMemoryObjectStore.
func NewInMemoryObjectStore() *InMemoryObjectStore {
	return &InMemoryObjectStore{
		items: make(map[string]*storedObject),
	}
}

// Put stores data in memory.
func (s *InMemoryObjectStore) Put(_ context.Context, key string, data []byte, contentType string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Copy data so caller can reuse the slice.
	cp := make([]byte, len(data))
	copy(cp, data)

	s.items[key] = &storedObject{
		data:        cp,
		contentType: contentType,
	}
	return nil
}

// Get retrieves data from memory.
func (s *InMemoryObjectStore) Get(_ context.Context, key string) ([]byte, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	obj, ok := s.items[key]
	if !ok {
		return nil, "", fmt.Errorf("key %q not found", key)
	}

	// Return a copy so caller can't mutate internal state.
	cp := make([]byte, len(obj.data))
	copy(cp, obj.data)
	return cp, obj.contentType, nil
}

// Delete removes a key from memory.
func (s *InMemoryObjectStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, key)
	return nil
}

// MinioStore is a MinIO/S3-backed ObjectStore.
type MinioStore struct {
	client *minio.Client
	bucket string
}

// NewMinioStore creates a MinioStore and ensures the bucket exists.
func NewMinioStore(ctx context.Context, cfg configs.ObjectStorageConfig) (*MinioStore, error) {
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("minio client: %w", err)
	}

	bucket := cfg.Bucket
	if bucket == "" {
		bucket = "im-files"
	}

	// Ensure bucket exists.
	exists, err := client.BucketExists(ctx, bucket)
	if err != nil {
		return nil, fmt.Errorf("minio bucket check: %w", err)
	}
	if !exists {
		if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, fmt.Errorf("minio make bucket: %w", err)
		}
		log.Printf("[minio] created bucket %q", bucket)
	}

	log.Printf("[minio] connected to %s, bucket=%s", cfg.Endpoint, bucket)
	return &MinioStore{client: client, bucket: bucket}, nil
}

// Put uploads data to MinIO.
func (s *MinioStore) Put(ctx context.Context, key string, data []byte, contentType string) error {
	reader := bytes.NewReader(data)
	_, err := s.client.PutObject(ctx, s.bucket, key, reader, int64(len(data)), minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return fmt.Errorf("minio put %q: %w", key, err)
	}
	return nil
}

// Get downloads data from MinIO.
func (s *MinioStore) Get(ctx context.Context, key string) ([]byte, string, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, "", fmt.Errorf("minio get %q: %w", key, err)
	}
	defer obj.Close()

	stat, err := obj.Stat()
	if err != nil {
		return nil, "", fmt.Errorf("minio stat %q: %w", key, err)
	}

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, "", fmt.Errorf("minio read %q: %w", key, err)
	}

	return data, stat.ContentType, nil
}

// Ping checks MinIO connectivity by verifying the bucket exists.
func (s *MinioStore) Ping(ctx context.Context) error {
	_, err := s.client.BucketExists(ctx, s.bucket)
	return err
}

// Delete removes an object from MinIO.
func (s *MinioStore) Delete(ctx context.Context, key string) error {
	err := s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
	if err != nil {
		return fmt.Errorf("minio delete %q: %w", key, err)
	}
	return nil
}
