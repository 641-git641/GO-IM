// Package mq provides Kafka producer and consumer components for async
// message persistence. The producer publishes messages to Kafka (fire-and-forget
// from the caller's perspective), and the consumer batch-writes them to a
// repo.MessageStore (typically MySQL) with at-least-once semantics.
package mq

import (
	"context"
	"encoding/binary"
	"log"
	"time"

	"github.com/im/api/proto"
	"github.com/segmentio/kafka-go"
	pb "google.golang.org/protobuf/proto"
)

// Producer publishes messages to a Kafka topic for async persistence.
// It is intentionally fire-and-forget: publish failures are logged but never
// block or fail the hot message-delivery path.
type Producer struct {
	writer *kafka.Writer
}

// ProducerConfig holds settings for creating a Producer.
type ProducerConfig struct {
	Brokers []string
	Topic   string
}

// NewProducer creates a Kafka producer. Uses lazy connection (segmentio/kafka-go),
// so this call always succeeds even if brokers are unreachable.
// Errors surface later in Publish calls.
func NewProducer(cfg ProducerConfig) (*Producer, error) {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(cfg.Brokers...),
		Topic:    cfg.Topic,
		Balancer: &kafka.Hash{}, // hash by key for deterministic partitioning
		// Minimize latency overhead on the hot path.
		BatchSize:    1,
		BatchTimeout: 10 * time.Millisecond,
		// Best-effort: don't block forever.
		WriteTimeout: 2 * time.Second,
		ReadTimeout:  2 * time.Second,
		RequiredAcks: kafka.RequireOne,
		// Synchronous writes: each WriteMessages blocks until acked.
		// Callers use goroutines so the hot path is never blocked.
		Async: false,
	}

	log.Printf("[mq] producer ready (brokers=%v topic=%s)", cfg.Brokers, cfg.Topic)
	return &Producer{writer: writer}, nil
}

// Publish sends a message to Kafka. This is fire-and-forget on the hot path:
// callers should invoke it in a goroutine. Errors are logged, not returned
// (we never want Kafka failures to break message delivery).
func (p *Producer) Publish(ctx context.Context, msg *proto.Message) {
	if msg == nil {
		return
	}

	// Serialize the message as protobuf binary.
	data, err := pb.Marshal(msg)
	if err != nil {
		log.Printf("[mq] marshal error for msgId=%d: %v", msg.MsgId, err)
		return
	}

	// Use MsgId (snowflake, 8 bytes) as the message key for partitioning.
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(msg.MsgId))

	km := kafka.Message{
		Key:   key,
		Value: data,
	}

	// Use a child context with timeout so a stuck broker doesn't leak goroutines.
	writeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := p.writer.WriteMessages(writeCtx, km); err != nil {
		log.Printf("[mq] publish error for msgId=%d: %v", msg.MsgId, err)
	}
}

// Close flushes and closes the underlying Kafka writer.
func (p *Producer) Close() error {
	log.Printf("[mq] producer closing...")
	return p.writer.Close()
}
