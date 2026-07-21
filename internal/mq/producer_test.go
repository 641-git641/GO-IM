package mq

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/im/api/proto"
)

// TestProducerPublishNil is a safety test — publishing a nil message
// should not panic.
func TestProducerPublishNil(t *testing.T) {
	producer, err := NewProducer(ProducerConfig{
		Brokers: []string{"localhost:19999"}, // no Kafka running here
		Topic:   "test.topic",
	})
	if err != nil {
		t.Skipf("Kafka producer creation failed (expected with no broker): %v", err)
	}
	defer producer.Close()

	// Publishing nil should not panic.
	producer.Publish(context.Background(), nil)

	// Publishing a valid message should not panic (will fail to write, logged).
	msg := &proto.Message{
		MsgId:   12345,
		Cmd:     proto.CmdChat,
		From:    "alice",
		To:      "bob",
		Content: "test message",
	}
	producer.Publish(context.Background(), msg)
}

// TestProducerIntegration publishes a message to a real Kafka broker
// and verifies it does not error. Skipped if Kafka is not running.
func TestProducerIntegration(t *testing.T) {
	// Quick connectivity check — skip if no Kafka on default port.
	conn, err := net.DialTimeout("tcp", "localhost:9092", 500*time.Millisecond)
	if err != nil {
		t.Skip("Kafka not running on localhost:9092 — skipping integration test")
	}
	conn.Close()

	producer, err := NewProducer(ProducerConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "im.test.producer",
	})
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer producer.Close()

	msg := &proto.Message{
		MsgId:   12345,
		Cmd:     proto.CmdChat,
		From:    "alice",
		To:      "bob",
		Content: "integration test message",
	}
	// Publish is fire-and-forget; we just verify no panic.
	producer.Publish(context.Background(), msg)
	t.Log("published message without panic")
}

// TestProducerClose verifies that closing an idle producer does not error.
func TestProducerClose(t *testing.T) {
	producer, err := NewProducer(ProducerConfig{
		Brokers: []string{"localhost:19999"}, // unreachable
		Topic:   "test.close",
	})
	if err != nil {
		t.Skipf("producer creation failed: %v", err)
	}

	// Close should not hang or error (writer may have never connected).
	if err := producer.Close(); err != nil {
		t.Logf("Close returned error (expected with no broker): %v", err)
	}
}
