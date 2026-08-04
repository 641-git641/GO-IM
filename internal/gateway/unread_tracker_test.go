package gateway

import (
	"context"
	"testing"
)

func newTestUnreadTracker(t *testing.T) *InMemoryUnreadTracker {
	t.Helper()
	return NewInMemoryUnreadTracker()
}

func TestUnreadTrackerIncrement(t *testing.T) {
	ut := newTestUnreadTracker(t)
	ctx := context.Background()

	ut.Increment(ctx, "bob", "alice")
	if c := ut.GetCount(ctx, "bob", "alice"); c != 1 {
		t.Errorf("expected count 1, got %d", c)
	}

	ut.Increment(ctx, "bob", "alice")
	if c := ut.GetCount(ctx, "bob", "alice"); c != 2 {
		t.Errorf("expected count 2, got %d", c)
	}
}

func TestUnreadTrackerMarkRead(t *testing.T) {
	ut := newTestUnreadTracker(t)
	ctx := context.Background()

	ut.Increment(ctx, "bob", "alice")
	ut.Increment(ctx, "bob", "alice")
	ut.Increment(ctx, "bob", "alice")

	ut.MarkRead(ctx, "bob", "alice")
	if c := ut.GetCount(ctx, "bob", "alice"); c != 0 {
		t.Errorf("expected count 0 after MarkRead, got %d", c)
	}
}

func TestUnreadTrackerMarkReadIdempotent(t *testing.T) {
	ut := newTestUnreadTracker(t)
	ctx := context.Background()

	// 对不存在的用户调用 MarkRead 不应 panic 或创建条目。
	ut.MarkRead(ctx, "nonexistent", "peer")
	if c := ut.GetCount(ctx, "nonexistent", "peer"); c != 0 {
		t.Errorf("expected count 0, got %d", c)
	}

	// 对零条目调用 MarkRead 后应保持为零。
	ut.Increment(ctx, "bob", "alice")
	ut.MarkRead(ctx, "bob", "alice")
	ut.MarkRead(ctx, "bob", "alice") // 第二次 MarkRead
	if c := ut.GetCount(ctx, "bob", "alice"); c != 0 {
		t.Errorf("expected count 0 after double MarkRead, got %d", c)
	}
}

func TestUnreadTrackerGetCounts(t *testing.T) {
	ut := newTestUnreadTracker(t)
	ctx := context.Background()

	ut.Increment(ctx, "bob", "alice")
	ut.Increment(ctx, "bob", "alice")
	ut.Increment(ctx, "bob", "carol")

	counts := ut.GetCounts(ctx, "bob")
	if len(counts) != 2 {
		t.Errorf("expected 2 peers, got %d", len(counts))
	}
	if counts["alice"] != 2 {
		t.Errorf("expected alice count 2, got %d", counts["alice"])
	}
	if counts["carol"] != 1 {
		t.Errorf("expected carol count 1, got %d", counts["carol"])
	}
}

func TestUnreadTrackerGetCountsEmpty(t *testing.T) {
	ut := newTestUnreadTracker(t)
	ctx := context.Background()

	counts := ut.GetCounts(ctx, "unknown")
	if counts == nil {
		t.Error("GetCounts should return non-nil empty map")
	}
	if len(counts) != 0 {
		t.Errorf("expected empty map, got %d entries", len(counts))
	}
}

func TestUnreadTrackerGetCount(t *testing.T) {
	ut := newTestUnreadTracker(t)
	ctx := context.Background()

	// 未知 uid 返回 0。
	if c := ut.GetCount(ctx, "unknown", "alice"); c != 0 {
		t.Errorf("expected 0 for unknown uid, got %d", c)
	}

	// 已知 uid、未知 peer 返回 0。
	ut.Increment(ctx, "bob", "alice")
	if c := ut.GetCount(ctx, "bob", "carol"); c != 0 {
		t.Errorf("expected 0 for unknown peer, got %d", c)
	}
}

func TestUnreadTrackerSelfIncrement(t *testing.T) {
	ut := newTestUnreadTracker(t)
	ctx := context.Background()

	ut.Increment(ctx, "alice", "alice")
	if c := ut.GetCount(ctx, "alice", "alice"); c != 0 {
		t.Errorf("expected 0 for self-increment, got %d", c)
	}

	// GetCounts 不应包含空条目。
	counts := ut.GetCounts(ctx, "alice")
	if len(counts) != 0 {
		t.Errorf("expected empty counts after self-increment, got %d", len(counts))
	}
}

func TestUnreadTrackerConcurrentAccess(t *testing.T) {
	ut := newTestUnreadTracker(t)
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			ut.Increment(ctx, "bob", "alice")
			ut.MarkRead(ctx, "bob", "alice")
		}
		close(done)
	}()

	for i := 0; i < 100; i++ {
		_ = ut.GetCounts(ctx, "bob")
		_ = ut.GetCount(ctx, "bob", "alice")
		ut.Increment(ctx, "carol", "dave")
	}

	<-done
}

func TestUnreadTrackerCleanup(t *testing.T) {
	ut := newTestUnreadTracker(t)
	ctx := context.Background()

	// 先添加再清除 —— 外层 map 条目应被移除。
	ut.Increment(ctx, "bob", "alice")
	ut.MarkRead(ctx, "bob", "alice")

	counts := ut.GetCounts(ctx, "bob")
	if len(counts) != 0 {
		t.Errorf("expected empty counts after cleanup, got %d entries", len(counts))
	}

	// 验证内部状态不会导致 panic（外层 map 条目已被清理）。
	ut.Increment(ctx, "bob", "carol")
	if c := ut.GetCount(ctx, "bob", "carol"); c != 1 {
		t.Errorf("expected count 1 after re-increment, got %d", c)
	}
}
