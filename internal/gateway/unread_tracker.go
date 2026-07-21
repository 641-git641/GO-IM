package gateway

import (
	"context"
	"sync"
)

// UnreadTracker tracks per-user unread message counts from each peer.
// It is safe for concurrent use.
type UnreadTracker interface {
	// Increment adds 1 to the unread count that toUID has from fromUID.
	// Self-increments (toUID == fromUID) are silently ignored.
	Increment(ctx context.Context, toUID, fromUID string)

	// MarkRead clears the unread count that readerUID has from peerUID.
	// Idempotent: if the count is already zero or the reader has no entries, this is a no-op.
	MarkRead(ctx context.Context, readerUID, peerUID string)

	// GetCounts returns all per-peer unread counts for uid.
	// Returns an empty map (not nil) when there are no unread messages.
	GetCounts(ctx context.Context, uid string) map[string]int64

	// GetCount returns the unread count for uid from a specific peerUID.
	// Returns 0 if the uid has never received messages from peerUID.
	GetCount(ctx context.Context, uid, peerUID string) int64
}

// InMemoryUnreadTracker is an in-memory implementation of UnreadTracker.
type InMemoryUnreadTracker struct {
	mu     sync.RWMutex
	counts map[string]map[string]int64 // uid -> {peerUID -> unread count}
}

// NewInMemoryUnreadTracker creates a new InMemoryUnreadTracker.
func NewInMemoryUnreadTracker() *InMemoryUnreadTracker {
	return &InMemoryUnreadTracker{
		counts: make(map[string]map[string]int64),
	}
}

// Increment adds 1 to the unread count that toUID has from fromUID.
func (t *InMemoryUnreadTracker) Increment(_ context.Context, toUID, fromUID string) {
	if toUID == fromUID {
		return // self-messages do not create unread counts
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	inner, ok := t.counts[toUID]
	if !ok {
		inner = make(map[string]int64)
		t.counts[toUID] = inner
	}
	inner[fromUID]++
}

// MarkRead clears the unread count that readerUID has from peerUID.
func (t *InMemoryUnreadTracker) MarkRead(_ context.Context, readerUID, peerUID string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	inner, ok := t.counts[readerUID]
	if !ok {
		return // nothing to clear
	}
	delete(inner, peerUID)

	// Clean up outer map entry if no unread counts remain for this reader.
	if len(inner) == 0 {
		delete(t.counts, readerUID)
	}
}

// GetCounts returns all per-peer unread counts for uid.
func (t *InMemoryUnreadTracker) GetCounts(_ context.Context, uid string) map[string]int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	inner, ok := t.counts[uid]
	if !ok {
		return map[string]int64{}
	}

	// Return a copy to avoid data races with concurrent modifications.
	result := make(map[string]int64, len(inner))
	for k, v := range inner {
		result[k] = v
	}
	return result
}

// GetCount returns the unread count for uid from a specific peerUID.
func (t *InMemoryUnreadTracker) GetCount(_ context.Context, uid, peerUID string) int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	inner, ok := t.counts[uid]
	if !ok {
		return 0
	}
	return inner[peerUID]
}
