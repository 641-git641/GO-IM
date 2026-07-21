package gateway

import (
	"sync"
	"time"
)

// RateLimiter provides per-UID token-bucket rate limiting.
type RateLimiter struct {
	mu        sync.Mutex
	buckets   map[string]*rateBucket
	rate      float64       // tokens per second
	burst     int           // max tokens
	interval  time.Duration // cleanup interval
	done      chan struct{}
	closeOnce sync.Once
}

type rateBucket struct {
	tokens   float64
	lastTime time.Time
}

// NewRateLimiter creates a rate limiter with the given rate and burst.
// A background goroutine periodically cleans up stale buckets to prevent memory growth.
// cleanupInterval controls how often stale buckets are scanned (0 defaults to 5 minutes).
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

// cleanupLoop periodically removes buckets that haven't been used recently.
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

// Stop signals the cleanup goroutine to exit. Safe to call multiple times.
func (rl *RateLimiter) Stop() {
	rl.closeOnce.Do(func() {
		close(rl.done)
	})
}

// Allow checks whether a UID is within its rate limit.
// Returns true if the request is allowed, false otherwise.
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
