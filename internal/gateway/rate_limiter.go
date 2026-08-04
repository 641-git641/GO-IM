package gateway

import (
	"sync"
	"time"
)

// RateLimiter 提供按 UID 的令牌桶限流。
type RateLimiter struct {
	mu        sync.Mutex
	buckets   map[string]*rateBucket
	rate      float64       // 每秒令牌数
	burst     int           // 最大令牌数
	interval  time.Duration // 清理间隔
	done      chan struct{}
	closeOnce sync.Once
}

type rateBucket struct {
	tokens   float64
	lastTime time.Time
}

// NewRateLimiter 用给定的速率和突发值创建一个限流器。
// 后台 goroutine 定期清理过期桶,防止内存增长。
// cleanupInterval 控制过期桶的扫描频率(0 表示默认为 5 分钟)。
func NewRateLimiter(rate, burst int, cleanupInterval time.Duration) *RateLimiter {
	if cleanupInterval <= 0 {
		cleanupInterval = 5 * time.Minute
	}
	rl := &RateLimiter{
		buckets:  make(map[string]*rateBucket),
		rate:     float64(rate),
		burst:    burst,
		done:     make(chan struct{}),
		interval: cleanupInterval,
	}
	go rl.cleanupLoop()
	return rl
}

// cleanupLoop 定期移除最近未使用的桶。
func (rl *RateLimiter) cleanupLoop() {
	ticker := time.NewTicker(rl.interval)
	defer ticker.Stop()
	for {
		select {
		case <-rl.done:
			return
		case <-ticker.C:
			rl.mu.Lock()
			now := time.Now()
			for uid, b := range rl.buckets {
				if now.Sub(b.lastTime) > rl.interval {
					delete(rl.buckets, uid)
				}
			}
			rl.mu.Unlock()
		}
	}
}

// Stop 通知清理 goroutine 退出。可安全地多次调用。
func (rl *RateLimiter) Stop() {
	rl.closeOnce.Do(func() {
		close(rl.done)
	})
}

// Allow 检查某个 UID 是否在限流阈值内。
// 请求被允许则返回 true,否则返回 false。
func (rl *RateLimiter) Allow(uid string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	b, ok := rl.buckets[uid]
	now := time.Now()
	if !ok {
		rl.buckets[uid] = &rateBucket{tokens: float64(rl.burst) - 1, lastTime: now}
		return true
	}

	elapsed := now.Sub(b.lastTime).Seconds()
	b.tokens += elapsed * rl.rate
	if b.tokens > float64(rl.burst) {
		b.tokens = float64(rl.burst)
	}
	b.lastTime = now
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}
