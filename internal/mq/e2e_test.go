package mq

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/im/api/proto"
	"github.com/segmentio/kafka-go"
)

// TestProducerConsumerEndToEnd 验证完整的 Kafka 持久化链路:
//
//	Producer 发布 → Kafka → Consumer 消费 → MessageStore 落库
//
// 需要真实 Kafka(CI 的 kafka job 会起 apache/kafka 服务容器),
// 本机未运行 Kafka 时跳过(与 TestProducerIntegration 的处理一致)。
func TestProducerConsumerEndToEnd(t *testing.T) {
	// 建立到 broker 的协议级连接(比纯 TCP 探测更可靠),本机无 Kafka 时跳过。
	conn, err := kafka.DialContext(context.Background(), "tcp", "localhost:9092")
	if err != nil {
		t.Skipf("Kafka not running on localhost:9092 — skipping e2e test: %v", err)
	}
	defer conn.Close()

	// 每次运行使用唯一 topic 和消费组,避免上次提交的 offset 干扰本次消费。
	topic := fmt.Sprintf("im.e2e.%d", time.Now().UnixNano())
	group := fmt.Sprintf("im-e2e-%d", time.Now().UnixNano())

	// 显式创建 topic:kafka-go 生产者的 AllowAutoTopicCreation 默认为 false,
	// 客户端不会请求 broker 自动建 topic,因此必须先手动创建。
	if err := conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("CreateTopics(%s): %v", topic, err)
	}

	producer, err := NewProducer(ProducerConfig{Brokers: []string{"localhost:9092"}, Topic: topic})
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer producer.Close()

	store := &mockMessageStore{}
	consumer := NewConsumer(ConsumerConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   topic,
		GroupID: group,
	}, store)

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		consumer.Wait() // 等消费循环退出并做最后一次 flush
		consumer.Close()
	}()

	go consumer.Run(ctx)

	// 阶段 1:确认消费者已连上。
	// Consumer 以 StartOffset=LastOffset 启动,先于其连接快照落位的消息会被跳过,
	// 因此反复发探针消息,直到某条被消费 —— 一旦消费者连上,之后发布的消息必然被读到。
	probeID := int64(1000000)
	var probeSeen bool
	for i := 0; i < 8; i++ {
		probeID++
		producer.Publish(ctx, &proto.Message{
			MsgId: probeID, Cmd: proto.CmdChat, From: "alice", To: "bob", Content: "probe",
		})
		if waitForSaved(store, probeID, 5*time.Second) {
			probeSeen = true
			break
		}
	}
	if !probeSeen {
		t.Fatal("consumer never consumed probe — Kafka pipeline not working")
	}

	// 阶段 2:消费者已就绪,发布正式批次并等待全部落库。
	const total = 20
	for i := int64(1); i <= total; i++ {
		producer.Publish(ctx, &proto.Message{
			MsgId:   i,
			Cmd:     proto.CmdChat,
			From:    "alice",
			To:      "bob",
			Content: fmt.Sprintf("e2e message %d", i),
		})
	}

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if savedBatchCount(store, total) >= total {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got := savedBatchCount(store, total); got < total {
		saved := store.savedMessages()
		t.Fatalf("expected %d messages saved, got %d: %+v", total, got, saved)
	}
}

// waitForSaved 轮询 mock 存储,直到指定 MsgId 的消息被保存或超时。
func waitForSaved(store *mockMessageStore, msgID int64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, m := range store.savedMessages() {
			if m.MsgId == msgID {
				return true
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// savedBatchCount 统计已保存的正式批次消息数(MsgId 在 [1, total] 区间内)。
func savedBatchCount(store *mockMessageStore, total int64) int {
	n := 0
	for _, m := range store.savedMessages() {
		if m.MsgId >= 1 && m.MsgId <= total {
			n++
		}
	}
	return n
}
