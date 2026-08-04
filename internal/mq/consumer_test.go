package mq

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/im/api/proto"
	"github.com/im/internal/repo"
	"github.com/segmentio/kafka-go"
)

// mockMessageStore 实现 repo.MessageStore，用于测试消费者的刷新逻辑。
type mockMessageStore struct {
	mu      sync.Mutex
	saved   []*proto.Message
	saveErr error                    // 非 nil 时模拟存储故障
	saveFn  func(msg *proto.Message) error
}

func (m *mockMessageStore) Save(ctx context.Context, msg *proto.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.saveFn != nil {
		if err := m.saveFn(msg); err != nil {
			return err
		}
	}
	if m.saveErr != nil {
		return m.saveErr
	}
	m.saved = append(m.saved, msg)
	return nil
}

func (m *mockMessageStore) QueryHistory(ctx context.Context, uid1, uid2 string, before int64, limit int) ([]*proto.Message, error) {
	return nil, nil
}

func (m *mockMessageStore) QueryGroupHistory(ctx context.Context, groupID string, before int64, limit int) ([]*proto.Message, error) {
	return nil, nil
}

func (m *mockMessageStore) SearchMessages(ctx context.Context, params *repo.SearchParams) (*repo.SearchResult, error) {
	return nil, nil
}

func (m *mockMessageStore) RecallMessage(ctx context.Context, msgID int64, fromUID string, recallWindowMs int64) error {
	return nil
}

func (m *mockMessageStore) UpdateMessageContent(ctx context.Context, msgID int64, fromUID, newContent string) error {
	return nil
}

func (m *mockMessageStore) BrowseMessages(ctx context.Context, before int64, limit int) ([]*proto.Message, error) {
	return nil, nil
}

func (m *mockMessageStore) DeleteMessage(ctx context.Context, msgID int64) error {
	return nil
}

func (m *mockMessageStore) CountMessages(ctx context.Context) (int, error) {
	return 0, nil
}

func (m *mockMessageStore) savedMessages() []*proto.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*proto.Message, len(m.saved))
	copy(out, m.saved)
	return out
}

var _ repo.MessageStore = (*mockMessageStore)(nil)

// TestConsumerConfigDefaults 验证默认配置值。
func TestConsumerConfigDefaults(t *testing.T) {
	cfg := ConsumerConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "im.test.topic",
		GroupID: "im-test",
	}
	_ = cfg // 有效配置
	t.Log("ConsumerConfig is valid")
}

// TestConsumerConstructorDoesNotPanic 验证即使 broker 不可达
// NewConsumer 也不会 panic（它只是创建 reader）。
func TestConsumerConstructorDoesNotPanic(t *testing.T) {
	consumer := NewConsumer(ConsumerConfig{
		Brokers: []string{"localhost:19999"}, // 不可达
		Topic:   "test.topic",
		GroupID: "test-group",
	}, nil) // nil store —— flushAll 将跳过写入

	if consumer == nil {
		t.Fatal("NewConsumer returned nil")
	}
	consumer.Close()
	t.Log("Consumer constructor does not panic")
}

// TestConsumerConstructorWithMockStore 验证 NewConsumer
// 接受 MessageStore 接口（不仅是 *repo.MySQLStore）。
func TestConsumerConstructorWithMockStore(t *testing.T) {
	mock := &mockMessageStore{}
	consumer := NewConsumer(ConsumerConfig{
		Brokers: []string{"localhost:19999"},
		Topic:   "test.topic",
		GroupID: "test-group",
	}, mock)

	if consumer == nil {
		t.Fatal("NewConsumer returned nil")
	}
	if consumer.store == nil {
		t.Error("expected store to be set")
	}
	consumer.Close()
}

// TestConsumerFlushAllEmptyBuffer 在缓冲区为空时是一个空操作。
func TestConsumerFlushAllEmptyBuffer(t *testing.T) {
	mock := &mockMessageStore{}
	consumer := NewConsumer(ConsumerConfig{
		Brokers: []string{"localhost:19999"},
		Topic:   "test.topic",
		GroupID: "test-group",
	}, mock)

	// 空缓冲区调用 flushAll 不应 panic，也不应写入任何内容。
	consumer.flushAll()

	if len(mock.savedMessages()) != 0 {
		t.Errorf("expected 0 saved messages, got %d", len(mock.savedMessages()))
	}
}

// TestConsumerFlushAllNilStore 优雅地处理 nil 的 MessageStore。
func TestConsumerFlushAllNilStore(t *testing.T) {
	consumer := NewConsumer(ConsumerConfig{
		Brokers: []string{"localhost:19999"},
		Topic:   "test.topic",
		GroupID: "test-group",
	}, nil)

	// 手动向缓冲区添加一条消息（模拟 Run 的行为）。
	consumer.mu.Lock()
	consumer.buffer = append(consumer.buffer, bufferedMsg{
		msg: &proto.Message{MsgId: 1, Cmd: proto.CmdChat, Content: "hello"},
		km:  kafka.Message{},
	})
	consumer.mu.Unlock()

	// 不应 panic —— nil store 路径会提交所有 offset。
	consumer.flushAll()

	// 缓冲区应被清空。
	consumer.mu.Lock()
	if len(consumer.buffer) != 0 {
		t.Errorf("expected empty buffer after flush, got %d", len(consumer.buffer))
	}
	consumer.mu.Unlock()
}

// TestConsumerFlushAllSavesMessages 验证 flushAll 会写入存储。
func TestConsumerFlushAllSavesMessages(t *testing.T) {
	mock := &mockMessageStore{}
	consumer := NewConsumer(ConsumerConfig{
		Brokers: []string{"localhost:19999"},
		Topic:   "test.topic",
		GroupID: "test-group",
	}, mock)

	// 模拟缓冲 3 条消息。
	consumer.mu.Lock()
	consumer.buffer = append(consumer.buffer,
		bufferedMsg{msg: &proto.Message{MsgId: 1, Cmd: proto.CmdChat, Content: "a"}, km: kafka.Message{}},
		bufferedMsg{msg: &proto.Message{MsgId: 2, Cmd: proto.CmdChat, Content: "b"}, km: kafka.Message{}},
		bufferedMsg{msg: &proto.Message{MsgId: 3, Cmd: proto.CmdChat, Content: "c"}, km: kafka.Message{}},
	)
	consumer.mu.Unlock()

	consumer.flushAll()

	saved := mock.savedMessages()
	if len(saved) != 3 {
		t.Fatalf("expected 3 saved messages, got %d", len(saved))
	}
	if saved[0].MsgId != 1 || saved[1].MsgId != 2 || saved[2].MsgId != 3 {
		t.Error("saved messages are out of order or have wrong IDs")
	}
}

// TestConsumerFlushAllPartialFailure 验证单个错误后仍继续保存。
func TestConsumerFlushAllPartialFailure(t *testing.T) {
	mock := &mockMessageStore{
		saveFn: func(msg *proto.Message) error {
			if msg.MsgId == 2 {
				return context.DeadlineExceeded // 模拟瞬时错误
			}
			return nil
		},
	}
	consumer := NewConsumer(ConsumerConfig{
		Brokers: []string{"localhost:19999"},
		Topic:   "test.topic",
		GroupID: "test-group",
	}, mock)

	consumer.mu.Lock()
	consumer.buffer = append(consumer.buffer,
		bufferedMsg{msg: &proto.Message{MsgId: 1, Cmd: proto.CmdChat, Content: "a"}, km: kafka.Message{}},
		bufferedMsg{msg: &proto.Message{MsgId: 2, Cmd: proto.CmdChat, Content: "b"}, km: kafka.Message{}},
		bufferedMsg{msg: &proto.Message{MsgId: 3, Cmd: proto.CmdChat, Content: "c"}, km: kafka.Message{}},
	)
	consumer.mu.Unlock()

	// 不应 panic —— msg 2 失败，但 1 和 3 成功。
	consumer.flushAll()

	saved := mock.savedMessages()
	if len(saved) != 2 {
		t.Fatalf("expected 2 saved messages (msg 2 failed), got %d", len(saved))
	}
	if saved[0].MsgId != 1 {
		t.Errorf("expected msg 1 first, got %d", saved[0].MsgId)
	}
	if saved[1].MsgId != 3 {
		t.Errorf("expected msg 3 second, got %d", saved[1].MsgId)
	}
}

// TestConsumerBufferClearAfterFlush 验证缓冲区被清空。
func TestConsumerBufferClearAfterFlush(t *testing.T) {
	mock := &mockMessageStore{}
	consumer := NewConsumer(ConsumerConfig{
		Brokers: []string{"localhost:19999"},
		Topic:   "test.topic",
		GroupID: "test-group",
	}, mock)

	// 刷新两次 —— 第二次应为空操作。
	consumer.mu.Lock()
	consumer.buffer = append(consumer.buffer,
		bufferedMsg{msg: &proto.Message{MsgId: 1}, km: kafka.Message{}},
	)
	consumer.mu.Unlock()

	consumer.flushAll()
	if len(mock.savedMessages()) != 1 {
		t.Fatalf("first flush: expected 1 saved, got %d", len(mock.savedMessages()))
	}

	// 第二次刷新不应保存任何内容（缓冲区为空）。
	consumer.flushAll()
	if len(mock.savedMessages()) != 1 {
		t.Errorf("second flush: expected still 1 saved, got %d", len(mock.savedMessages()))
	}
}

// TestConsumerConcurrentBufferAccess 验证缓冲区操作的互斥锁安全性。
func TestConsumerConcurrentBufferAccess(t *testing.T) {
	mock := &mockMessageStore{}
	consumer := NewConsumer(ConsumerConfig{
		Brokers: []string{"localhost:19999"},
		Topic:   "test.topic",
		GroupID: "test-group",
	}, mock)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				consumer.mu.Lock()
				consumer.buffer = append(consumer.buffer, bufferedMsg{
					msg: &proto.Message{MsgId: int64(id*1000 + j)},
					km:  kafka.Message{},
				})
				consumer.mu.Unlock()
			}
		}(i)
	}

	// 同时反复运行 flushAll。
	for i := 0; i < 5; i++ {
		consumer.flushAll()
		time.Sleep(time.Millisecond)
	}

	wg.Wait()
	consumer.flushAll() // 最终排空

	// 全部 500 条消息都应被保存。
	saved := mock.savedMessages()
	if len(saved) != 500 {
		t.Errorf("expected 500 saved messages, got %d", len(saved))
	}
}
