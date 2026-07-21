// Package snowflake generates globally unique, time-sortable IDs.
package snowflake

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

// Epoch is the custom start time (2024-01-01 00:00:00 UTC in milliseconds).
const Epoch int64 = 1704067200000

const (
	workerBits     = 10
	sequenceBits   = 12
	workerMax      = -1 ^ (-1 << workerBits)
	sequenceMax    = -1 ^ (-1 << sequenceBits)
	workerShift    = sequenceBits
	timestampShift = sequenceBits + workerBits
)

// maxClockBackoff is the maximum clock rollback to tolerate by waiting.
const maxClockBackoff = 10 * time.Millisecond

// ErrWorkerIDInvalid is returned when the worker ID is out of range.
var ErrWorkerIDInvalid = errors.New("worker ID out of range")

// Generator produces snowflake IDs.
type Generator struct {
	mu        sync.Mutex
	workerID  int64
	sequence  int64
	lastStamp int64
}

// New creates a new snowflake Generator.
// Returns an error if workerID is out of the valid range [0, 1023].
func New(workerID int64) (*Generator, error) {
	if workerID < 0 || workerID > workerMax {
		return nil, fmt.Errorf("%w: %d (must be 0-%d)", ErrWorkerIDInvalid, workerID, workerMax)
	}
	return &Generator{workerID: workerID}, nil
}

// Next generates the next unique ID.
// Returns 0 on severe clock rollback (>10ms) after logging an error.
func (g *Generator) Next() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := time.Now().UnixMilli()
	if now < g.lastStamp {
		backoff := time.Duration(g.lastStamp-now) * time.Millisecond
		if backoff > maxClockBackoff {
			log.Printf("[snowflake] SEVERE clock rollback detected: %v (last=%d, now=%d)", backoff, g.lastStamp, now)
			return 0 // caller should treat MsgID==0 as an error signal
		}
		// Small rollback — wait for clock to catch up
		for now < g.lastStamp {
			time.Sleep(time.Microsecond * 100)
			now = time.Now().UnixMilli()
		}
	}

	if now == g.lastStamp {
		g.sequence = (g.sequence + 1) & sequenceMax
		if g.sequence == 0 {
			// sequence exhausted; wait for next millisecond
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

// ExtractTimestamp extracts the creation time from a snowflake ID.
// This is useful for debugging message ordering issues.
func ExtractTimestamp(id int64) time.Time {
	return time.UnixMilli((id >> timestampShift) + Epoch)
}
