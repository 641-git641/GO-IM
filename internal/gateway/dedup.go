package gateway

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
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
	// redisTasks 是待持久化的任务队列,由固定数量的 worker goroutine 消费。
	// 使用 worker 池而不是每 Mark 一个 goroutine —— 后者在高吞吐下会导致
	// goroutine 无限堆积在信号量上(信号量只限制并发,不限制排队数)。
	redis      *redis.Client
	redisKeyPF string         // 键前缀,默认为 "im:dedup:"
	redisTasks chan redisTask // 待写 Redis 的任务队列(有缓冲)
	redisWG    sync.WaitGroup // 等待 Redis worker 退出

	// marks 是本进程已 Mark 的消息总数。
	// 用于判断是否需要从 Redis 回退查询:只有冷启动(进程尚未处理过任何
	// 消息,内存为空)时才需要。一旦进程处理过消息,内存已通过 Mark 与
	// Redis 同步,再对每个新消息同步查 Redis 会拖垮高吞吐热路径。
	marks atomic.Int64
}

// redisTask 是一次 Redis 持久化的参数。
type redisTask struct {
	key    string
	msgID  int64
	expire time.Duration
}

// Redis 持久化任务队列的缓冲容量。允许短暂积压但不无限增长;
// 队列满时新任务被丢弃(即发即忘语义)。
const redisTaskQueueCap = 1024

// redisWorkers 常驻 Redis 写 worker 的数量。
const redisWorkers = 16

// NewDedupCache 创建一个 DedupCache,它会定期移除超过 TTL 的记录。
// 之后调用 SetRedis 可启用基于 Redis 的持久化。
func NewDedupCache(ttl time.Duration) *DedupCache {
	d := &DedupCache{
		seen:       make(map[string]dedupEntry),
		ttl:        ttl,
		done:       make(chan struct{}),
		redisKeyPF: "im:dedup:",
		redisTasks: make(chan redisTask, redisTaskQueueCap),
	}
	go d.cleanupLoop(ttl / 2) // 每个 TTL 窗口内清理两次
	// 启动常驻 Redis 写 worker。
	for i := 0; i < redisWorkers; i++ {
		d.redisWG.Add(1)
		go d.redisWorker()
	}
	return d
}

// redisWorker 常驻消费 Redis 写任务队列。
func (d *DedupCache) redisWorker() {
	defer d.redisWG.Done()
	for {
		select {
		case <-d.done:
			return
		case t := <-d.redisTasks:
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			if err := d.redis.Set(ctx, d.redisKeyPF+t.key, t.msgID, t.expire).Err(); err != nil {
				// 非致命 —— 去重检测是尽力而为的。
				log.Printf("[dedup] redis set error: %v", err)
			}
			cancel()
		}
	}
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
// 热路径:只查内存。仅在冷启动(本进程尚未处理过任何消息,即重启后
// 内存为空)时回退到 Redis,以恢复重启前的去重状态。
// 一旦进程处理过消息,内存已通过 Mark 与 Redis 同步,此时对每个新消息
// 再同步查询 Redis 是纯浪费 —— 新消息在 Redis 里必然不存在,却会在高
// 吞吐下(每秒上万次)把热路径拖垮。
func (d *DedupCache) IsDuplicate(from string, seq int64) (bool, int64) {
	d.mu.Lock()
	key := d.key(from, seq)
	if entry, ok := d.seen[key]; ok {
		d.mu.Unlock()
		return true, entry.msgID
	}
	d.mu.Unlock()

	// 冷启动恢复:仅当进程尚未 Mark 过任何消息(重启后首个消息)时,
	// 用短超时检查 Redis,避免 Redis 故障时阻塞热路径。
	if d.redis != nil && d.marks.Load() == 0 {
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
	d.marks.Add(1)

	// 异步持久化到 Redis —— 失败仅记录日志,绝不阻塞热路径。
	// TTL 加倍,使 Redis 中的记录比内存中的更长寿,覆盖重启窗口。
	// 通过固定 worker 池写入,高吞吐下 goroutine 数保持恒定。
	if d.redis != nil {
		t := redisTask{key: key, msgID: msgID, expire: d.ttl * 2}
		select {
		case d.redisTasks <- t:
			// 已入队,由常驻 worker 异步执行。
		default:
			// 队列满 —— 丢弃。去重持久化是尽力而为的。
			log.Printf("[dedup] redis queue full, dropping key=%s (persistence best-effort)", key)
		}
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

// Stop 通知清理与 Redis 写 goroutine 退出。可安全地多次调用。
func (d *DedupCache) Stop() {
	d.closeOnce.Do(func() {
		close(d.done)
		d.redisWG.Wait()
	})
}
