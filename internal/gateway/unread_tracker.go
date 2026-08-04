package gateway

import (
	"context"
	"sync"
)

// UnreadTracker 跟踪每个用户从各会话收到的未读消息数。
// 可安全地并发使用。
type UnreadTracker interface {
	// Increment 将 toUID 从 fromUID 收到的未读计数加 1。
	// 自增(toUID == fromUID)会被静默忽略。
	Increment(ctx context.Context, toUID, fromUID string)

	// MarkRead 清除 readerUID 从 peerUID 收到的未读计数。
	// 幂等:如果计数已为零或该用户没有记录,则为无操作。
	MarkRead(ctx context.Context, readerUID, peerUID string)

	// GetCounts 返回 uid 的所有会话未读计数。
	// 没有未读消息时返回空映射(而非 nil)。
	GetCounts(ctx context.Context, uid string) map[string]int64

	// GetCount 返回 uid 从指定 peerUID 收到的未读计数。
	// 如果 uid 从未收到过 peerUID 的消息,则返回 0。
	GetCount(ctx context.Context, uid, peerUID string) int64
}

// InMemoryUnreadTracker 是 UnreadTracker 的内存实现。
type InMemoryUnreadTracker struct {
	mu     sync.RWMutex
	counts map[string]map[string]int64 // uid -> {peerUID -> 未读计数}
}

// NewInMemoryUnreadTracker 创建一个新的 InMemoryUnreadTracker。
func NewInMemoryUnreadTracker() *InMemoryUnreadTracker {
	return &InMemoryUnreadTracker{
		counts: make(map[string]map[string]int64),
	}
}

// Increment 将 toUID 从 fromUID 收到的未读计数加 1。
func (t *InMemoryUnreadTracker) Increment(_ context.Context, toUID, fromUID string) {
	if toUID == fromUID {
		return // 自己发给自己的消息不产生未读计数
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

// MarkRead 清除 readerUID 从 peerUID 收到的未读计数。
func (t *InMemoryUnreadTracker) MarkRead(_ context.Context, readerUID, peerUID string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	inner, ok := t.counts[readerUID]
	if !ok {
		return // 没有可清除的记录
	}
	delete(inner, peerUID)

	// 如果该用户已无未读计数,则清理外层映射条目。
	if len(inner) == 0 {
		delete(t.counts, readerUID)
	}
}

// GetCounts 返回 uid 的所有会话未读计数。
func (t *InMemoryUnreadTracker) GetCounts(_ context.Context, uid string) map[string]int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	inner, ok := t.counts[uid]
	if !ok {
		return map[string]int64{}
	}

	// 返回副本,避免与并发修改产生数据竞争。
	result := make(map[string]int64, len(inner))
	for k, v := range inner {
		result[k] = v
	}
	return result
}

// GetCount 返回 uid 从指定 peerUID 收到的未读计数。
func (t *InMemoryUnreadTracker) GetCount(_ context.Context, uid, peerUID string) int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	inner, ok := t.counts[uid]
	if !ok {
		return 0
	}
	return inner[peerUID]
}
