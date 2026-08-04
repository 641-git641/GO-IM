package gateway

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// dedupEntry 保存一条去重记录及其创建时间,用于基于 TTL 的淘汰。
type dedupEntry struct {
	msgID   int64
	created time.Time
}

// DedupCache 防止客户端用相同 seq 重试时重复投递消息。
// 键格式:"fromUID:seq" → msgID。
//
// 主存储是内存映射,保证热路径上的低延迟查询。
// 通过 SetRedis 配置了可选的 Redis 后端时,Mark 也会持久化到 Redis
// (即发即忘),使去重状态在 Gateway 重启后依然保留。本地未命中时
// (例如重启后),IsDuplicate 会回退到查询 Redis。
type DedupCache struct {
	mu        sync.Mutex
	seen      map[string]dedupEntry
	ttl       time.Duration
	done      chan struct{} // 由 Stop() 关闭,用于通知清理 goroutine 退出
	closeOnce sync.Once

	// Redis 持久化(可选 —— nil 表示仅内存)。
	redis      *redis.Client
	redisKeyPF string        // 键前缀,默认为 "im:dedup:"
	redisSem   chan struct{} // 限制并发 Redis 异步写入数量,默认为 64
}

// NewDedupCache 创建一个 DedupCache,它会定期移除超过 TTL 的记录。
// 之后调用 SetRedis 可启用基于 Redis 的持久化。
func NewDedupCache(ttl time.Duration) *DedupCache {
	d := &DedupCache{
		seen:       make(map[string]dedupEntry),
		ttl:        ttl,
		done:       make(chan struct{}),
		redisKeyPF: "im:dedup:",
		redisSem:   make(chan struct{}, 64),
	}
	go d.cleanupLoop(ttl / 2) // 每个 TTL 窗口内清理两次
	return d
}

// SetRedis 为去重缓存启用基于 Redis 的持久化。最多只能在启动期间调用一次。
// 设置后,Mark 会异步持久化到 Redis,而 IsDuplicate 在本地未命中时回退到 Redis。
func (d *DedupCache) SetRedis(rdb *redis.Client) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.redis = rdb
}

// key 为 (from, seq) 组合构建查找键。
func (d *DedupCache) key(from string, seq int64) string {
	return fmt.Sprintf("%s:%d", from, seq)
}

// IsDuplicate 检查 (from, seq) 组合是否已被处理过。
// 若重复则返回 (true, assignedMsgID),否则返回 (false, 0)。
// 仅在 seq > 0 时调用。
//
// 热路径:先检查内存。启用 Redis 且未命中时,回退到 Redis
// (覆盖重启后内存缓存为空的情况)。
func (d *DedupCache) IsDuplicate(from string, seq int64) (bool, int64) {
	d.mu.Lock()
	key := d.key(from, seq)
	if entry, ok := d.seen[key]; ok {
		d.mu.Unlock()
		return true, entry.msgID
	}
	d.mu.Unlock()

	// 冷路径:用短超时检查 Redis,避免 Redis 故障时阻塞热路径
	// (仅在重启清空内存后有用)。
	if d.redis != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		val, err := d.redis.Get(ctx, d.redisKeyPF+key).Int64()
		cancel()
		if err == nil {
			// 在 Redis 中找到 —— 回填本地缓存,使下次查找更快。
			d.mu.Lock()
			d.seen[key] = dedupEntry{msgID: val, created: time.Now()}
			d.mu.Unlock()
			return true, val
		}
	}

	return false, 0
}

// Mark 记录 (from, seq) 组合及其分配的 MsgID。
// 配置了 Redis 时,还会异步持久化(即发即忘)。
func (d *DedupCache) Mark(from string, seq int64, msgID int64) {
	key := d.key(from, seq)

	d.mu.Lock()
	d.seen[key] = dedupEntry{msgID: msgID, created: time.Now()}
	d.mu.Unlock()

	// 异步持久化到 Redis —— 失败仅记录日志,绝不阻塞热路径。
	// TTL 加倍,使 Redis 中的记录比内存中的更长寿,覆盖重启窗口。
	// 信号量限制并发 Redis 写入,防止高消息吞吐下 goroutine 无限增长。
	if d.redis != nil {
		go func() {
			d.redisSem <- struct{}{}
			defer func() { <-d.redisSem }()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			expire := d.ttl * 2
			if err := d.redis.Set(ctx, d.redisKeyPF+key, msgID, expire).Err(); err != nil {
				// 非致命 —— 去重检测是尽力而为的。
				log.Printf("[dedup] redis set error: %v", err)
			}
		}()
	}
}

// cleanupLoop 定期淘汰超过 TTL 的记录。
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

// Stop 通知清理 goroutine 退出。可安全地多次调用。
func (d *DedupCache) Stop() {
	d.closeOnce.Do(func() {
		close(d.done)
	})
}
