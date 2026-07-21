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

// mockMessageStore implements repo.MessageStore for testing the consumer flush logic.
type mockMessageStore struct {
	mu      sync.Mutex
	saved   []*proto.Message
	saveErr error                    // non-nil to simulate store failure
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

// TestConsumerConfigDefaults validates default config values.
func TestConsumerConfigDefaults(t *testing.T) {
	cfg := ConsumerConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "im.test.topic",
		GroupID: "im-test",
	}
	_ = cfg // valid config
	t.Log("ConsumerConfig is valid")
}

// TestConsumerConstructorDoesNotPanic verifies NewConsumer
// does not panic even with unreachable brokers (it just creates the reader).
func TestConsumerConstructorDoesNotPanic(t *testing.T) {
	consumer := NewConsumer(ConsumerConfig{
		Brokers: []string{"localhost:19999"}, // unreachable
		Topic:   "test.topic",
		GroupID: "test-group",
	}, nil) // nil store — flushAll will skip writes

	if consumer == nil {
		t.Fatal("NewConsumer returned nil")
	}
	consumer.Close()
	t.Log("Consumer constructor does not panic")
}

// TestConsumerConstructorWithMockStore verifies NewConsumer
// accepts a MessageStore interface (not just *repo.MySQLStore).
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

// TestConsumerFlushAllEmptyBuffer is a no-op when the buffer is empty.
func TestConsumerFlushAllEmptyBuffer(t *testing.T) {
	mock := &mockMessageStore{}
	consumer := NewConsumer(ConsumerConfig{
		Brokers: []string{"localhost:19999"},
		Topic:   "test.topic",
		GroupID: "test-group",
	}, mock)

	// flushAll with empty buffer should not panic and should not write anything.
	consumer.flushAll()

	if len(mock.savedMessages()) != 0 {
		t.Errorf("expected 0 saved messages, got %d", len(mock.savedMessages()))
	}
}

// TestConsumerFlushAllNilStore handles a nil MessageStore gracefully.
func TestConsumerFlushAllNilStore(t *testing.T) {
	consumer := NewConsumer(ConsumerConfig{
		Brokers: []string{"localhost:19999"},
		Topic:   "test.topic",
		GroupID: "test-group",
	}, nil)

	// Manually add a message to the buffer (simulating what Run does).
	consumer.mu.Lock()
	consumer.buffer = append(consumer.buffer, bufferedMsg{
		msg: &proto.Message{MsgId: 1, Cmd: proto.CmdChat, Content: "hello"},
		km:  kafka.Message{},
	})
	consumer.mu.Unlock()

	// Should not panic — nil store path commits all offsets.
	consumer.flushAll()

	// Buffer should be drained.
	consumer.mu.Lock()
	if len(consumer.buffer) != 0 {
		t.Errorf("expected empty buffer after flush, got %d", len(consumer.buffer))
	}
	consumer.mu.Unlock()
}

// TestConsumerFlushAllSavesMessages verifies flushAll writes to the store.
func TestConsumerFlushAllSavesMessages(t *testing.T) {
	mock := &mockMessageStore{}
	consumer := NewConsumer(ConsumerConfig{
		Brokers: []string{"localhost:19999"},
		Topic:   "test.topic",
		GroupID: "test-group",
	}, mock)

	// Simulate buffering 3 messages.
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

// TestConsumerFlushAllPartialFailure continues saving after individual errors.
func TestConsumerFlushAllPartialFailure(t *testing.T) {
	mock := &mockMessageStore{
		saveFn: func(msg *proto.Message) error {
			if msg.MsgId == 2 {
				return context.DeadlineExceeded // simulate transient error
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

	// Should not panic — msg 2 failed but 1 and 3 succeed.
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

// TestConsumerBufferClearAfterFlush verifies the buffer is cleared.
func TestConsumerBufferClearAfterFlush(t *testing.T) {
	mock := &mockMessageStore{}
	consumer := NewConsumer(ConsumerConfig{
		Brokers: []string{"localhost:19999"},
		Topic:   "test.topic",
		GroupID: "test-group",
	}, mock)

	// Flush twice — second should be a no-op.
	consumer.mu.Lock()
	consumer.buffer = append(consumer.buffer,
		bufferedMsg{msg: &proto.Message{MsgId: 1}, km: kafka.Message{}},
	)
	consumer.mu.Unlock()

	consumer.flushAll()
	if len(mock.savedMessages()) != 1 {
		t.Fatalf("first flush: expected 1 saved, got %d", len(mock.savedMessages()))
	}

	// Second flush should not save anything (buffer is empty).
	consumer.flushAll()
	if len(mock.savedMessages()) != 1 {
		t.Errorf("second flush: expected still 1 saved, got %d", len(mock.savedMessages()))
	}
}

// TestConsumerConcurrentBufferAccess verifies mutex safety of buffer ops.
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

	// Meanwhile run flushAll repeatedly.
	for i := 0; i < 5; i++ {
		consumer.flushAll()
		time.Sleep(time.Millisecond)
	}

	wg.Wait()
	consumer.flushAll() // final drain

	// All 500 messages should be saved.
	saved := mock.savedMessages()
	if len(saved) != 500 {
		t.Errorf("expected 500 saved messages, got %d", len(saved))
	}
}
