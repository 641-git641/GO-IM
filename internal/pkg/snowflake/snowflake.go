// Package snowflake 生成全局唯一、按时间可排序的 ID。
package snowflake

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

// Epoch 是自定义起始时间（2024-01-01 00:00:00 UTC，单位毫秒）。
const Epoch int64 = 1704067200000

const (
	workerBits     = 10
	sequenceBits   = 12
	workerMax      = -1 ^ (-1 << workerBits)
	sequenceMax    = -1 ^ (-1 << sequenceBits)
	workerShift    = sequenceBits
	timestampShift = sequenceBits + workerBits
)

// maxClockBackoff 是通过等待可容忍的最大时钟回拨量。
const maxClockBackoff = 10 * time.Millisecond

// 当 worker ID 超出范围时返回 ErrWorkerIDInvalid。
var ErrWorkerIDInvalid = errors.New("worker ID out of range")

// Generator 用于生成 snowflake ID。
type Generator struct {
	mu        sync.Mutex
	workerID  int64
	sequence  int64
	lastStamp int64
}

// New 创建一个新的 snowflake Generator。
// 如果 workerID 超出有效范围 [0, 1023] 则返回错误。
func New(workerID int64) (*Generator, error) {
	if workerID < 0 || workerID > workerMax {
		return nil, fmt.Errorf("%w: %d (must be 0-%d)", ErrWorkerIDInvalid, workerID, workerMax)
	}
	return &Generator{workerID: workerID}, nil
}

// Next 生成下一个唯一 ID。
// 遇到严重时钟回拨（>10ms）时记录错误并返回 0。
func (g *Generator) Next() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := time.Now().UnixMilli()
	if now < g.lastStamp {
		backoff := time.Duration(g.lastStamp-now) * time.Millisecond
		if backoff > maxClockBackoff {
			log.Printf("[snowflake] SEVERE clock rollback detected: %v (last=%d, now=%d)", backoff, g.lastStamp, now)
			return 0 // 调用方应将 MsgID==0 视为错误信号
		}
		// 轻微回拨 —— 等待时钟追上
		for now < g.lastStamp {
			time.Sleep(time.Microsecond * 100)
			now = time.Now().UnixMilli()
		}
	}

	if now == g.lastStamp {
		g.sequence = (g.sequence + 1) & sequenceMax
		if g.sequence == 0 {
			// 序列耗尽；等待下一个毫秒
			for now <= g.lastStamp {
				now = time.Now().UnixMilli()
			}
		}
	} else {
		g.sequence = 0
	}

	g.lastStamp = now
	return (now-Epoch)<<timestampShift | (g.workerID << workerShift) | g.sequence
}

// ExtractTimestamp 从 snowflake ID 中提取创建时间。
// 这对于调试消息排序问题很有用。
func ExtractTimestamp(id int64) time.Time {
	return time.UnixMilli((id >> timestampShift) + Epoch)
}
