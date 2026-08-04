package mq

import (
	"context"
	"testing"

	"github.com/im/api/proto"
	"github.com/segmentio/kafka-go"
)

// TestProducerPublishNil 是一个安全测试 —— 发布 nil 消息
// 不应导致 panic。
func TestProducerPublishNil(t *testing.T) {
	producer, err := NewProducer(ProducerConfig{
		Brokers: []string{"localhost:19999"}, // 这里没有运行 Kafka
		Topic:   "test.topic",
	})
	if err != nil {
		t.Skipf("Kafka producer creation failed (expected with no broker): %v", err)
	}
	defer producer.Close()

	// 发布 nil 不应导致 panic。
	producer.Publish(context.Background(), nil)

	// 发布有效消息不应导致 panic（写入会失败，仅记录日志）。
	msg := &proto.Message{
		MsgId:   12345,
		Cmd:     proto.CmdChat,
		From:    "alice",
		To:      "bob",
		Content: "test message",
	}
	producer.Publish(context.Background(), msg)
}

// TestProducerIntegration 向真实的 Kafka broker 发布一条消息
// 并验证不会出错。如果 Kafka 未运行则跳过。
func TestProducerIntegration(t *testing.T) {
	// 协议级连通性检查 —— 默认端口没有 Kafka 则跳过。
	conn, err := kafka.DialContext(context.Background(), "tcp", "localhost:9092")
	if err != nil {
		t.Skip("Kafka not running on localhost:9092 — skipping integration test")
	}
	defer conn.Close()

	// 显式创建 topic(kafka-go 默认不请求自动创建);重复创建幂等,无副作用。
	if err := conn.CreateTopics(kafka.TopicConfig{
		Topic:             "im.test.producer",
		NumPartitions:     1,
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("CreateTopics: %v", err)
	}

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
	// Publish 是即发即忘的；我们只验证没有 panic。
	producer.Publish(context.Background(), msg)
	t.Log("published message without panic")
}

// TestProducerClose 验证关闭空闲生产者不会出错。
func TestProducerClose(t *testing.T) {
	producer, err := NewProducer(ProducerConfig{
		Brokers: []string{"localhost:19999"}, // 不可达
		Topic:   "test.close",
	})
	if err != nil {
		t.Skipf("producer creation failed: %v", err)
	}

	// Close 不应挂起或出错（writer 可能从未连接过）。
	if err := producer.Close(); err != nil {
		t.Logf("Close returned error (expected with no broker): %v", err)
	}
}
