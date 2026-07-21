package mq

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/im/api/proto"
	"github.com/im/internal/repo"
	"github.com/segmentio/kafka-go"
	pb "google.golang.org/protobuf/proto"
)

// bufferedMsg pairs a deserialized proto.Message with its Kafka message
// so we can commit the offset only after the store write succeeds.
type bufferedMsg struct {
	msg *proto.Message
	km  kafka.Message
}

// Consumer reads messages from a Kafka topic and batch-writes them to a
// repo.MessageStore. Offsets are committed only after successful writes —
// this ensures at-least-once semantics: a crash between write and commit
// causes re-delivery, and INSERT IGNORE handles the duplicate.
type Consumer struct {
	reader *kafka.Reader
	store  repo.MessageStore // interface — allows mocking in tests

	// batch buffers writes to reduce round-trips.
	mu      sync.Mutex
	buffer  []bufferedMsg
	flushCh chan struct{}
	done    chan struct{}
	wg      sync.WaitGroup // tracks flushLoop goroutine
}

// ConsumerConfig holds settings for the Kafka consumer.
type ConsumerConfig struct {
	Brokers []string
	Topic   string
	GroupID string // consumer group for coordinated consumption
}

// Batch constants.
const (
	defaultBatchSize    = 100
	defaultFlushTimeout = 1 * time.Second
	maxBufferSize       = 10000 // safety cap to prevent unbounded memory growth under store failure
)

// NewConsumer creates a Kafka consumer that batch-writes messages via the
// provided MessageStore (typically a *repo.MySQLStore). The store may be nil
// only for testing — a nil store will cause flushAll to skip writes.
func NewConsumer(cfg ConsumerConfig, store repo.MessageStore) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.Brokers,
		Topic:       cfg.Topic,
		GroupID:     cfg.GroupID,
		MinBytes:    1,    // don't wait for a full batch from Kafka
		MaxBytes:    10e6, // 10 MB
		StartOffset: kafka.LastOffset,
	})

	return &Consumer{
		reader:  reader,
		store:   store,
		buffer:  make([]bufferedMsg, 0, defaultBatchSize),
		flushCh: make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
}

// Run starts the consume loop. Blocks until ctx is cancelled.
// Call in a goroutine.
func (c *Consumer) Run(ctx context.Context) {
	log.Printf("[mq] consumer starting (topic=%s group=%s)", c.reader.Config().Topic, c.reader.Config().GroupID)

	// Background flush ticker.
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.flushLoop(ctx)
	}()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[mq] consumer shutting down...")
			c.flushAll()
			close(c.done)
			return
		default:
		}

		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				continue // ctx cancelled, loop will exit on next select
			}
			log.Printf("[mq] fetch error: %v", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Deserialize protobuf.
		var pm proto.Message
		if err := pb.Unmarshal(msg.Value, &pm); err != nil {
			log.Printf("[mq] unmarshal error: %v (offset=%d)", err, msg.Offset)
			// Commit and skip malformed messages so we don't get stuck.
			// Use an independent context so shutdown doesn't prevent
			// committing a malformed message that arrived just before cancel.
			commitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := c.reader.CommitMessages(commitCtx, msg); err != nil {
				log.Printf("[mq] commit error for malformed msg: %v", err)
			}
			continue
		}

		// Batch it — track the Kafka message for offset commit after store flush.
		c.mu.Lock()
		// Safety cap: if MySQL is down for an extended period, don't let the buffer
		// grow unbounded. Drop and commit the oldest buffered message to make room.
		if len(c.buffer) >= maxBufferSize {
			log.Printf("[mq] buffer at capacity (%d), dropping oldest message (msgId=%d offset=%d)",
				maxBufferSize, c.buffer[0].msg.MsgId, c.buffer[0].km.Offset)
			// Commit the dropped message so it's not re-delivered — it's lost.
			// This is an extreme scenario (MySQL down for minutes); the alternative
			// (OOM crash) is worse.
			dropped := c.buffer[0]
			c.buffer = c.buffer[1:]
			go func(km kafka.Message) {
				commitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				if err := c.reader.CommitMessages(commitCtx, km); err != nil {
					log.Printf("[mq] commit error for dropped msg: %v", err)
				}
			}(dropped.km)
		}
		c.buffer = append(c.buffer, bufferedMsg{msg: &pm, km: msg})
		shouldFlush := len(c.buffer) >= defaultBatchSize
		c.mu.Unlock()

		if shouldFlush {
			select {
			case c.flushCh <- struct{}{}:
			default:
				// Flush already pending; don't block.
			}
		}
		// NOTE: offset is committed in flushAll, not here.
		// This prevents data loss if the process crashes between
		// buffering and store write.
	}
}

// Wait blocks until the consumer has fully shut down (run loop exited and flush loop finished).
func (c *Consumer) Wait() {
	<-c.done
	c.wg.Wait() // ensure flushLoop has fully exited
}

// Close closes the underlying Kafka reader.
func (c *Consumer) Close() error {
	return c.reader.Close()
}

// flushLoop periodically flushes buffered messages to the store.
func (c *Consumer) flushLoop(ctx context.Context) {
	ticker := time.NewTicker(defaultFlushTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.flushAll()
		case <-c.flushCh:
			c.flushAll()
		}
	}
}

// flushAll writes all buffered messages to the store and then commits Kafka offsets.
// Offsets are committed AFTER successful writes so a crash never causes
// data loss (at-least-once semantics; INSERT IGNORE handles replays).
func (c *Consumer) flushAll() {
	c.mu.Lock()
	if len(c.buffer) == 0 {
		c.mu.Unlock()
		return
	}
	batch := c.buffer
	c.buffer = make([]bufferedMsg, 0, defaultBatchSize)
	c.mu.Unlock()

	// Use a context with timeout so shutdown doesn't hang indefinitely.
	writeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var succeeded []kafka.Message
	if c.store == nil {
		// No store configured (test mode) — commit all offsets to avoid
		// getting stuck on the same messages.
		for _, bm := range batch {
			succeeded = append(succeeded, bm.km)
		}
	} else {
		for _, bm := range batch {
			if err := c.store.Save(writeCtx, bm.msg); err != nil {
				log.Printf("[mq] store save error (msgId=%d): %v", bm.msg.MsgId, err)
				// Don't commit this offset — message will be redelivered.
			} else {
				succeeded = append(succeeded, bm.km)
			}
		}
	}

	// Commit Kafka offsets only for messages successfully written.
	if len(succeeded) > 0 {
		commitCtx, commitCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer commitCancel()
		if err := c.reader.CommitMessages(commitCtx, succeeded...); err != nil {
			log.Printf("[mq] commit error: %v", err)
			// Messages will be redelivered on restart — INSERT IGNORE
			// will silently skip duplicates.
		}
	}

	if len(succeeded) > 0 {
		log.Printf("[mq] flushed %d/%d messages to store", len(succeeded), len(batch))
	}
}
