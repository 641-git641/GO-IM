package gateway

import (
	"context"
	"testing"
)

func TestInMemoryObjectStorePutGet(t *testing.T) {
	s := NewInMemoryObjectStore()
	ctx := context.Background()

	err := s.Put(ctx, "test-key", []byte("hello world"), "text/plain")
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	data, ct, err := s.Get(ctx, "test-key")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(data) != "hello world" {
		t.Errorf("expected 'hello world', got %q", string(data))
	}
	if ct != "text/plain" {
		t.Errorf("expected 'text/plain', got %q", ct)
	}
}

func TestInMemoryObjectStoreGetNotFound(t *testing.T) {
	s := NewInMemoryObjectStore()
	ctx := context.Background()

	_, _, err := s.Get(ctx, "nonexistent")
	if err == nil {
		t.Error("expected error for missing key")
	}
}

func TestInMemoryObjectStoreDelete(t *testing.T) {
	s := NewInMemoryObjectStore()
	ctx := context.Background()

	s.Put(ctx, "k", []byte("v"), "text/plain")
	if err := s.Delete(ctx, "k"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	_, _, err := s.Get(ctx, "k")
	if err == nil {
		t.Error("expected error after delete")
	}
}

func TestInMemoryObjectStoreDataIsolation(t *testing.T) {
	s := NewInMemoryObjectStore()
	ctx := context.Background()

	orig := []byte("original")
	s.Put(ctx, "k", orig, "text/plain")

	// 修改原始切片 —— 存储不应受影响。
	orig[0] = 'X'

	data, _, _ := s.Get(ctx, "k")
	if string(data) != "original" {
		t.Errorf("expected 'original', got %q — data not isolated from caller mutation", string(data))
	}

	// 修改返回的切片 —— 存储不应受影响。
	data[0] = 'Y'

	data2, _, _ := s.Get(ctx, "k")
	if string(data2) != "original" {
		t.Errorf("expected 'original', got %q — Get should return a copy", string(data2))
	}
}

func TestInMemoryObjectStoreLargeData(t *testing.T) {
	s := NewInMemoryObjectStore()
	ctx := context.Background()

	large := make([]byte, 1024*1024) // 1 MB
	for i := range large {
		large[i] = byte(i % 256)
	}

	if err := s.Put(ctx, "large", large, "application/octet-stream"); err != nil {
		t.Fatalf("put large: %v", err)
	}

	data, ct, err := s.Get(ctx, "large")
	if err != nil {
		t.Fatalf("get large: %v", err)
	}
	if len(data) != len(large) {
		t.Errorf("expected %d bytes, got %d", len(large), len(data))
	}
	if ct != "application/octet-stream" {
		t.Errorf("expected 'application/octet-stream', got %q", ct)
	}
	for i := range data {
		if data[i] != large[i] {
			t.Errorf("data mismatch at byte %d", i)
			break
		}
	}
}

func TestInMemoryObjectStoreConcurrent(t *testing.T) {
	s := NewInMemoryObjectStore()
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			s.Put(ctx, "key", []byte("val"), "text/plain")
			s.Get(ctx, "key")
			s.Delete(ctx, "key")
		}
		close(done)
	}()

	for i := 0; i < 100; i++ {
		s.Put(ctx, "other", []byte("x"), "text/plain")
		s.Get(ctx, "other")
	}

	<-done
}
