package gateway

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// dedupEntry holds a dedup record with its creation time for TTL-based eviction.
type dedupEntry struct {
	msgID   int64
	created time.Time
}

// DedupCache prevents duplicate message delivery when clients retry with the same seq.
// Key format: "fromUID:seq" → msgID.
//
// The primary store is an in-memory map for low-latency lookups on the hot path.
// When an optional Redis backend is configured via SetRedis, Mark also persists to
// Redis (fire-and-forget) so the dedup state survives Gateway restarts. On a local
// miss (e.g. after restart), IsDuplicate falls back to querying Redis.
type DedupCache struct {
	mu        sync.Mutex
	seen      map[string]dedupEntry
	ttl       time.Duration
	done      chan struct{} // closed by Stop() to signal cleanup goroutine
	closeOnce sync.Once

	// Redis durability (optional — nil means memory-only).
	redis      *redis.Client
	redisKeyPF string // key prefix, default "im:dedup:"
	redisSem   chan struct{} // bounds concurrent Redis async writes, default 64
}

// NewDedupCache creates a DedupCache that periodically removes entries older than TTL.
// Call SetRedis later to enable Redis-backed durability.
func NewDedupCache(ttl time.Duration) *DedupCache {
	d := &DedupCache{
		seen:       make(map[string]dedupEntry),
		ttl:        ttl,
		done:       make(chan struct{}),
		redisKeyPF: "im:dedup:",
		redisSem:   make(chan struct{}, 64),
	}
	go d.cleanupLoop(ttl / 2) // clean up twice per TTL window
	return d
}

// SetRedis enables Redis-backed durability for the dedup cache. Safe to call at
// most once during startup. When set, Mark asynchronously persists to Redis and
// IsDuplicate falls back to Redis on local miss.
func (d *DedupCache) SetRedis(rdb *redis.Client) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.redis = rdb
}

// key builds the lookup key for a (from, seq) pair.
func (d *DedupCache) key(from string, seq int64) string {
	return fmt.Sprintf("%s:%d", from, seq)
}

// IsDuplicate checks whether a (from, seq) pair has already been seen.
// Returns (true, assignedMsgID) if duplicate, (false, 0) otherwise.
// Only call when seq > 0.
//
// Hot path: checks in-memory first. On miss with Redis enabled, falls back to
// Redis (covers the restart case where the in-memory cache is empty).
func (d *DedupCache) IsDuplicate(from string, seq int64) (bool, int64) {
	d.mu.Lock()
	key := d.key(from, seq)
	if entry, ok := d.seen[key]; ok {
		d.mu.Unlock()
		return true, entry.msgID
	}
	d.mu.Unlock()

	// Cold-path: check Redis with a short timeout to avoid blocking the hot
	// path on Redis failure (only useful after a restart that cleared memory).
	if d.redis != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		val, err := d.redis.Get(ctx, d.redisKeyPF+key).Int64()
		cancel()
		if err == nil {
			// Found in Redis — rehydrate local cache so next lookup is fast.
			d.mu.Lock()
			d.seen[key] = dedupEntry{msgID: val, created: time.Now()}
			d.mu.Unlock()
			return true, val
		}
	}

	return false, 0
}

// Mark records a (from, seq) pair with its assigned MsgID.
// When Redis is configured, also persists asynchronously (fire-and-forget).
func (d *DedupCache) Mark(from string, seq int64, msgID int64) {
	key := d.key(from, seq)

	d.mu.Lock()
	d.seen[key] = dedupEntry{msgID: msgID, created: time.Now()}
	d.mu.Unlock()

	// Persist to Redis asynchronously — failures are logged but never block
	// the hot path. The TTL is doubled so the Redis entry outlives the
	// in-memory entry, covering the restart window.
	// A semaphore bounds concurrent Redis writes to prevent unbounded goroutine
	// growth under high message throughput.
	if d.redis != nil {
		go func() {
			d.redisSem <- struct{}{}
			defer func() { <-d.redisSem }()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			expire := d.ttl * 2
			if err := d.redis.Set(ctx, d.redisKeyPF+key, msgID, expire).Err(); err != nil {
				// Non-fatal — duplicate detection is best-effort.
				log.Printf("[dedup] redis set error: %v", err)
			}
		}()
	}
}

// cleanupLoop periodically evicts entries older than the TTL.
func (d *DedupCache) cleanupLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-d.done:
			return
		case <-ticker.C:
			d.mu.Lock()
			cutoff := time.Now().Add(-d.ttl)
			for key, entry := range d.seen {
				if entry.created.Before(cutoff) {
					delete(d.seen, key)
				}
			}
			d.mu.Unlock()
		}
	}
}

// Stop signals the cleanup goroutine to exit. Safe to call multiple times.
func (d *DedupCache) Stop() {
	d.closeOnce.Do(func() {
		close(d.done)
	})
}
