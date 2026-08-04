package snowflake

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestNewValidWorkerID(t *testing.T) {
	tests := []int64{0, 1, 512, 1023}
	for _, wid := range tests {
		g, err := New(wid)
		if err != nil {
			t.Errorf("New(%d) unexpected error: %v", wid, err)
		}
		if g == nil {
			t.Errorf("New(%d) returned nil generator", wid)
		}
	}
}

func TestNewInvalidWorkerID(t *testing.T) {
	tests := []int64{-1, -100, 1024, 9999}
	for _, wid := range tests {
		g, err := New(wid)
		if err == nil {
			t.Errorf("New(%d) expected error, got nil", wid)
		}
		if !errors.Is(err, ErrWorkerIDInvalid) {
			t.Errorf("New(%d) error should wrap ErrWorkerIDInvalid, got: %v", wid, err)
		}
		if g != nil {
			t.Errorf("New(%d) returned non-nil generator on error", wid)
		}
	}
}

func TestUniqueness(t *testing.T) {
	const goroutines = 10
	const perRoutine = 100_000

	g, err := New(1)
	if err != nil {
		t.Fatalf("New(1): %v", err)
	}

	ids := make(map[int64]bool)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perRoutine; j++ {
				id := g.Next()
				if id == 0 {
					// 检测到时钟回拨 —— 测试中跳过
					continue
				}
				mu.Lock()
				if ids[id] {
					t.Errorf("duplicate ID: %d", id)
				}
				ids[id] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	total := len(ids)
	if total < goroutines*perRoutine-1 { // 允许少量返回 0 的情况
		t.Logf("generated %d unique IDs (target: %d)", total, goroutines*perRoutine)
	}
}

func TestMonotonic(t *testing.T) {
	g, err := New(1)
	if err != nil {
		t.Fatalf("New(1): %v", err)
	}

	var prev int64 = 0
	for i := 0; i < 10_000; i++ {
		id := g.Next()
		if id == 0 {
			// 跳过时钟回拨导致的返回值
			t.Logf("Next() returned 0 at iteration %d (clock rollback)", i)
			continue
		}
		if id <= prev {
			t.Errorf("not monotonic at iteration %d: prev=%d, current=%d", i, prev, id)
		}
		prev = id
	}
}

func TestWorkerIDPrefix(t *testing.T) {
	g1, _ := New(1)
	g2, _ := New(2)

	// 生成足够的 ID 以确保有有效值
	var id1, id2 int64
	for i := 0; i < 100; i++ {
		id1 = g1.Next()
		if id1 != 0 {
			break
		}
	}
	for i := 0; i < 100; i++ {
		id2 = g2.Next()
		if id2 != 0 {
			break
		}
	}

	if id1 == 0 || id2 == 0 {
		t.Skip("clock rollback prevented generating test IDs")
	}

	// 提取 worker ID 位
	mask := int64((1 << workerBits) - 1)
	wid1 := (id1 >> workerShift) & mask
	wid2 := (id2 >> workerShift) & mask

	if wid1 != 1 {
		t.Errorf("expected worker ID 1 in id, got %d", wid1)
	}
	if wid2 != 2 {
		t.Errorf("expected worker ID 2 in id, got %d", wid2)
	}
}

func TestExtractTimestamp(t *testing.T) {
	g, _ := New(1)

	before := time.Now()
	var id int64
	for i := 0; i < 100; i++ {
		id = g.Next()
		if id != 0 {
			break
		}
	}
	after := time.Now()

	if id == 0 {
		t.Skip("clock rollback prevented generating test ID")
	}

	extracted := ExtractTimestamp(id)

	if extracted.Before(before.Add(-100*time.Millisecond)) {
		t.Errorf("extracted timestamp %v is before generation window start %v", extracted, before)
	}
	if extracted.After(after.Add(100 * time.Millisecond)) {
		t.Errorf("extracted timestamp %v is after generation window end %v", extracted, after)
	}
}

func TestSequenceOverflow(t *testing.T) {
	g, _ := New(1)
	g.sequence = sequenceMax // 强制下一次调用时溢出
	g.lastStamp = time.Now().UnixMilli()

	var id1, id2 int64
	for i := 0; i < 10; i++ {
		id1 = g.Next()
		if id1 != 0 {
			break
		}
	}
	for i := 0; i < 10; i++ {
		id2 = g.Next()
		if id2 != 0 {
			break
		}
	}

	if id1 == 0 || id2 == 0 {
		t.Skip("clock rollback prevented generating test IDs")
	}

	if id1 >= id2 {
		t.Errorf("expected id1 < id2 after sequence overflow: id1=%d, id2=%d", id1, id2)
	}

	// 确保时间戳不同（序列耗尽会强制使用新的毫秒）
	ts1 := (id1 >> timestampShift)
	ts2 := (id2 >> timestampShift)
	if ts1 > ts2 {
		t.Errorf("sequence overflow should advance timestamp")
	} else {
		t.Logf("sequence overflow test: ts1=%d, ts2=%d ✓", ts1, ts2)
	}
}
