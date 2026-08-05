package gateway

import (
	"testing"
	"time"
)

// TestRateLimiterCounters 验证 Allow 的累计放行/拒绝计数(供 /metrics 暴露)。
func TestRateLimiterCounters(t *testing.T) {
	rl := NewRateLimiter(1, 1, time.Minute) // 1 msg/s,burst 1
	defer rl.Stop()

	if !rl.Allow("uid1") {
		t.Fatal("first Allow should be true (burst token)")
	}
	if rl.Allow("uid1") {
		t.Fatal("second Allow within same second should be rejected")
	}
	// 不同 UID 独立限流,应放行。
	if !rl.Allow("uid2") {
		t.Fatal("Allow for a different uid should be true")
	}

	if got := rl.Allowed(); got != 2 {
		t.Errorf("Allowed: expected 2, got %d", got)
	}
	if got := rl.Rejected(); got != 1 {
		t.Errorf("Rejected: expected 1, got %d", got)
	}
}

// TestGnetPoolDrops 验证 worker 池满时丢弃计数递增。
func TestGnetPoolDrops(t *testing.T) {
	before := GnetPoolDrops()
	wp := NewWorkerPool(1) // 1 worker,任务缓冲 = size*256 = 256
	defer wp.Close()

	// 用永久阻塞的任务占住唯一 worker。
	gate := make(chan struct{})
	done := make(chan struct{})
	wp.Submit(func() { <-gate; close(done) })

	// 灌满任务缓冲(256),超出部分必然被丢弃。
	for i := 0; i < 300; i++ {
		wp.Submit(func() {})
	}
	wp.Submit(func() {})
	if got := GnetPoolDrops(); got <= before {
		t.Errorf("expected pool drops to increase, before=%d after=%d", before, got)
	}

	close(gate)
	<-done
}
