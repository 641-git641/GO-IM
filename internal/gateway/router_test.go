package gateway

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/im/api/proto"
	"github.com/im/internal/pkg/snowflake"
	"github.com/im/internal/repo"
	pb "google.golang.org/protobuf/proto"
)

// readFromChan reads a single []byte from a channel with a timeout.
func readFromChan(t *testing.T, ch <-chan []byte) []byte {
	t.Helper()
	select {
	case data := <-ch:
		return data
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for channel read")
		return nil
	}
}

// readMessageFromChan reads and unmarshals a proto.Message from a channel.
func readMessageFromChan(t *testing.T, ch <-chan []byte) *proto.Message {
	t.Helper()
	raw := readFromChan(t, ch)
	msg := &proto.Message{}
	if err := pb.Unmarshal(raw, msg); err != nil {
		t.Fatalf("unmarshal message: %v (raw=%s)", err, string(raw))
	}
	return msg
}

// assertChanEmpty verifies no message is waiting on the channel.
func assertChanEmpty(t *testing.T, ch <-chan []byte) {
	t.Helper()
	select {
	case data := <-ch:
		t.Errorf("expected empty channel, got: %s", string(data))
	default:
		// expected
	}
}

// ---------- Heartbeat ----------

func TestRouteHeartbeat(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd: proto.CmdHeartbeat,
	})

	resp := readMessageFromChan(t, sender.send)
	if resp.Cmd != proto.CmdHeartbeat {
		t.Errorf("expected CmdHeartbeat response, got cmd=%d", resp.Cmd)
	}
	if resp.MsgId == 0 {
		t.Error("expected non-zero MsgID in heartbeat response")
	}
}

// ---------- Chat — Online delivery ----------

func TestRouteChatOnline(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())

	// Register target in hub
	target := newTestClient(t, "bob", "Bob")
	h.Register(context.Background(), target)

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:      proto.CmdChat,
		From:     "alice", // set by readPump in production, set manually in test
		To:       "bob",
		ChatType: proto.ChatTypeSingle,
		MsgType:  proto.MsgTypeText,
		Content:  "Hello Bob!",
		NeedAck:  true,
		Seq:      1,
	})

	// Target should receive the message
	delivered := readMessageFromChan(t, target.send)
	if delivered.Cmd != proto.CmdChat {
		t.Errorf("expected CmdChat, got cmd=%d", delivered.Cmd)
	}
	if delivered.From != "alice" {
		t.Errorf("expected From=alice, got %s", delivered.From)
	}
	if delivered.Content != "Hello Bob!" {
		t.Errorf("expected Content='Hello Bob!', got '%s'", delivered.Content)
	}
	if delivered.MsgId == 0 {
		t.Error("expected non-zero MsgID")
	}

	// Sender should receive ACK
	ack := readMessageFromChan(t, sender.send)
	if ack.Cmd != proto.CmdAck {
		t.Errorf("expected CmdAck, got cmd=%d", ack.Cmd)
	}
	if ack.MsgId != delivered.MsgId {
		t.Errorf("ACK MsgID mismatch: ack=%d delivered=%d", ack.MsgId, delivered.MsgId)
	}
	if ack.Seq != 1 {
		t.Errorf("ACK Seq mismatch: expected 1, got %d", ack.Seq)
	}
}

// ---------- Chat — Offline storage ----------

func TestRouteChatOffline(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())

	sender := newTestClient(t, "alice", "Alice")
	// Bob is NOT registered in hub (offline)

	r.Route(context.Background(), sender, &proto.Message{
		Cmd:      proto.CmdChat,
		From:     "alice", // set by readPump in production, set manually in test
		To:       "bob",
		ChatType: proto.ChatTypeSingle,
		MsgType:  proto.MsgTypeText,
		Content:  "Are you there?",
		NeedAck:  true,
		Seq:      1,
	})

	// Sender should get ACK even though target is offline
	ack := readMessageFromChan(t, sender.send)
	if ack.Cmd != proto.CmdAck {
		t.Errorf("expected CmdAck, got cmd=%d", ack.Cmd)
	}

	// Message should be in offline store
	offlineMsgs := h.DrainOffline(context.Background(),"bob")
	if len(offlineMsgs) != 1 {
		t.Fatalf("expected 1 offline message, got %d", len(offlineMsgs))
	}
	if offlineMsgs[0].Content != "Are you there?" {
		t.Errorf("expected 'Are you there?', got '%s'", offlineMsgs[0].Content)
	}
	if offlineMsgs[0].From != "alice" {
		t.Errorf("expected From=alice, got %s", offlineMsgs[0].From)
	}
}

// ---------- Chat — Send buffer full fallback ----------

func TestRouteChatSendBufferFull(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())

	// Create target with tiny send buffer
	target := &Client{
		UID:      "bob",
		Username: "Bob",
		send:     make(chan []byte, 1), // buffer of 1
		closed:   make(chan struct{}),
	}
	h.Register(context.Background(), target)

	sender := newTestClient(t, "alice", "Alice")

	// Pre-fill the send buffer so the next Send() returns ErrSendBufferFull
	target.send <- []byte(`{}`)

	// Now send a chat message — Send should fail (buffer full), fallback to offline
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:      proto.CmdChat,
		To:       "bob",
		ChatType: proto.ChatTypeSingle,
		MsgType:  proto.MsgTypeText,
		Content:  "Buffer test",
		NeedAck:  true,
		Seq:      1,
	})

	// Sender should still get ACK
	ack := readMessageFromChan(t, sender.send)
	if ack.Cmd != proto.CmdAck {
		t.Errorf("expected CmdAck, got cmd=%d", ack.Cmd)
	}

	// Message should be in offline store (send failed → fallback)
	offlineMsgs := h.DrainOffline(context.Background(),"bob")
	if len(offlineMsgs) != 1 {
		t.Fatalf("expected 1 offline message (send buffer full fallback), got %d", len(offlineMsgs))
	}
	if offlineMsgs[0].Content != "Buffer test" {
		t.Errorf("expected 'Buffer test', got '%s'", offlineMsgs[0].Content)
	}
}

// ---------- Deduplication ----------

func TestRouteDuplicate(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())

	target := newTestClient(t, "bob", "Bob")
	h.Register(context.Background(), target)

	sender := newTestClient(t, "alice", "Alice")

	// First message
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:      proto.CmdChat,
		To:       "bob",
		ChatType: proto.ChatTypeSingle,
		MsgType:  proto.MsgTypeText,
		Content:  "Original",
		NeedAck:  true,
		Seq:      42,
	})

	// Drain first delivery and ACK
	firstDelivered := readMessageFromChan(t, target.send)
	_ = readMessageFromChan(t, sender.send) // first ACK
	originalMsgID := firstDelivered.MsgId

	t.Logf("First delivery: msgId=%d", originalMsgID)

	// Second message with same Seq — should be deduplicated
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:      proto.CmdChat,
		To:       "bob",
		ChatType: proto.ChatTypeSingle,
		MsgType:  proto.MsgTypeText,
		Content:  "Duplicate retry",
		NeedAck:  true,
		Seq:      42, // same Seq
	})

	// Sender should get a replayed ACK with the original MsgId
	replayAck := readMessageFromChan(t, sender.send)
	if replayAck.Cmd != proto.CmdAck {
		t.Errorf("expected CmdAck replay, got cmd=%d", replayAck.Cmd)
	}
	if replayAck.MsgId != originalMsgID {
		t.Errorf("replay ACK should have original MsgId=%d, got %d", originalMsgID, replayAck.MsgId)
	}

	// Target should NOT receive a duplicate
	assertChanEmpty(t, target.send)

	t.Logf("ACK replay verified: msgId=%d matches original ✓", replayAck.MsgId)
}

// ---------- Unknown / Invalid commands ----------

func TestRouteCmdNone(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())

	sender := newTestClient(t, "alice", "Alice")
	// CmdNone should not panic or send anything
	r.Route(context.Background(), sender, &proto.Message{Cmd: proto.CmdNone})

	assertChanEmpty(t, sender.send)
}

func TestRouteUnknownCmd(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())

	sender := newTestClient(t, "alice", "Alice")
	// Unknown command should not panic
	r.Route(context.Background(), sender, &proto.Message{Cmd: 999})

	assertChanEmpty(t, sender.send)
}

// ---------- Offline drain ----------

func TestRouteOfflineDrain(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())

	h.StoreOffline(context.Background(),"alice", &proto.Message{
		Cmd: proto.CmdChat, MsgId: 100, Content: "stored msg",
	})

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{Cmd: proto.CmdOffline})

	// Should receive the stored message
	delivered := readMessageFromChan(t, sender.send)
	if delivered.Cmd != proto.CmdChat || delivered.MsgId != 100 {
		t.Errorf("expected offline message MsgID=100, got cmd=%d MsgID=%d", delivered.Cmd, delivered.MsgId)
	}

	// Queue should be drained
	remaining := h.DrainOffline(context.Background(),"alice")
	if len(remaining) != 0 {
		t.Errorf("expected empty queue after drain, got %d", len(remaining))
	}
}

func TestRouteOfflineDrainEmpty(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{Cmd: proto.CmdOffline})

	// Should receive an empty CmdOffline response
	resp := readMessageFromChan(t, sender.send)
	if resp.Cmd != proto.CmdOffline {
		t.Errorf("expected CmdOffline response for empty queue, got cmd=%d", resp.Cmd)
	}
}

// ---------- ACK not requested ----------

func TestRouteChatNoAck(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())

	target := newTestClient(t, "bob", "Bob")
	h.Register(context.Background(), target)

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:      proto.CmdChat,
		To:       "bob",
		ChatType: proto.ChatTypeSingle,
		MsgType:  proto.MsgTypeText,
		Content:  "No ACK needed",
		NeedAck:  false, // no ACK requested
	})

	// Target should receive the message
	delivered := readMessageFromChan(t, target.send)
	if delivered.Content != "No ACK needed" {
		t.Errorf("expected 'No ACK needed', got '%s'", delivered.Content)
	}

	// Sender should NOT get an ACK
	assertChanEmpty(t, sender.send)
}

// ---------- Invalid chat (missing target) ----------

func TestRouteChatInvalidTarget(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())

	sender := newTestClient(t, "alice", "Alice")
	// Missing To field — should be ignored (Validate fails)
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:      proto.CmdChat,
		To:       "", // missing target
		ChatType: proto.ChatTypeSingle,
		MsgType:  proto.MsgTypeText,
		Content:  "No target",
	})

	// No ACK, no delivery
	assertChanEmpty(t, sender.send)
}

// ---------- History ----------

// testMsgStore is a minimal in-memory MessageStore for testing handleHistory.
type testMsgStore struct {
	msgs []*proto.Message
}

func (s *testMsgStore) Save(ctx context.Context, msg *proto.Message) error {
	s.msgs = append(s.msgs, msg)
	return nil
}

func (s *testMsgStore) QueryHistory(ctx context.Context, uid1, uid2 string, before int64, limit int) ([]*proto.Message, error) {
	// Filter messages between uid1 and uid2, newest first.
	var matches []*proto.Message
	for _, m := range s.msgs {
		if m.Timestamp < before &&
			((m.From == uid1 && m.To == uid2) || (m.From == uid2 && m.To == uid1)) {
			matches = append(matches, m)
		}
	}
	// Sort newest first.
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].Timestamp > matches[j].Timestamp
	})
	if len(matches) > limit {
		matches = matches[:limit]
	}
	return matches, nil
}

func (s *testMsgStore) QueryGroupHistory(ctx context.Context, groupID string, before int64, limit int) ([]*proto.Message, error) {
	// Filter messages sent to the group, newest first.
	var matches []*proto.Message
	for _, m := range s.msgs {
		if m.Timestamp < before && m.To == groupID && m.ChatType == 2 {
			matches = append(matches, m)
		}
	}
	// Sort newest first.
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].Timestamp > matches[j].Timestamp
	})
	if len(matches) > limit {
		matches = matches[:limit]
	}
	return matches, nil
}

func (s *testMsgStore) SearchMessages(ctx context.Context, params *repo.SearchParams) (*repo.SearchResult, error) {
	return nil, nil
}

func (s *testMsgStore) RecallMessage(ctx context.Context, msgID int64, fromUID string, recallWindowMs int64) error {
	for _, m := range s.msgs {
		if m.MsgId == msgID && m.From == fromUID {
			m.Content = `{"recalled":true}`
			return nil
		}
	}
	return fmt.Errorf("message %d not found or not owned by %s", msgID, fromUID)
}

func (s *testMsgStore) UpdateMessageContent(ctx context.Context, msgID int64, fromUID, newContent string) error {
	for _, m := range s.msgs {
		if m.MsgId == msgID && m.From == fromUID {
			m.Content = newContent
			return nil
		}
	}
	return fmt.Errorf("message %d not found", msgID)
}

func (s *testMsgStore) BrowseMessages(ctx context.Context, before int64, limit int) ([]*proto.Message, error) {
	return nil, nil
}

func (s *testMsgStore) DeleteMessage(ctx context.Context, msgID int64) error {
	for i, m := range s.msgs {
		if m.MsgId == msgID {
			s.msgs = append(s.msgs[:i], s.msgs[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("message %d not found", msgID)
}

func (s *testMsgStore) CountMessages(ctx context.Context) (int, error) {
	return len(s.msgs), nil
}

func TestRouteHistoryNoStore(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig()) // msgStore = nil

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd: proto.CmdHistory,
		To:  "bob",
	})

	// Should receive CmdHistory completion with Seq=0.
	resp := readMessageFromChan(t, sender.send)
	if resp.Cmd != proto.CmdHistory {
		t.Errorf("expected CmdHistory response, got cmd=%d", resp.Cmd)
	}
	if resp.Seq != 0 {
		t.Errorf("expected Seq=0 (no messages), got Seq=%d", resp.Seq)
	}
	assertChanEmpty(t, sender.send)
}

func TestRouteHistoryInvalidTarget(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd: proto.CmdHistory,
		To:  "", // missing target
	})

	// No response — Validate fails.
	assertChanEmpty(t, sender.send)
}

func TestRouteHistorySuccess(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	store := &testMsgStore{}

	now := int64(1721318400000)
	// Pre-populate 3 messages between alice and bob.
	for i := 0; i < 3; i++ {
		from := "alice"
		to := "bob"
		if i%2 == 1 {
			from, to = to, from
		}
		store.msgs = append(store.msgs, &proto.Message{
			MsgId:     int64(5000 + i),
			Cmd:       proto.CmdChat,
			From:      from,
			To:        to,
			ChatType:  proto.ChatTypeSingle,
			MsgType:   proto.MsgTypeText,
			Content:   fmt.Sprintf("history msg %d", i),
			Timestamp: now + int64(i*1000),
		})
	}

	r := NewRouter(h, h, sg, store, DefaultRouterConfig())

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:       proto.CmdHistory,
		To:        "bob",
		Timestamp: now + 10000, // before this time
		Seq:       50,          // limit
	})

	// Should receive 3 messages (newest first) then a completion.
	msg1 := readMessageFromChan(t, sender.send)
	if msg1.Cmd != proto.CmdChat || msg1.MsgId != 5002 {
		t.Errorf("msg1: expected CmdChat MsgId=5002 (newest), got cmd=%d MsgId=%d", msg1.Cmd, msg1.MsgId)
	}

	msg2 := readMessageFromChan(t, sender.send)
	if msg2.Cmd != proto.CmdChat || msg2.MsgId != 5001 {
		t.Errorf("msg2: expected CmdChat MsgId=5001, got cmd=%d MsgId=%d", msg2.Cmd, msg2.MsgId)
	}

	msg3 := readMessageFromChan(t, sender.send)
	if msg3.Cmd != proto.CmdChat || msg3.MsgId != 5000 {
		t.Errorf("msg3: expected CmdChat MsgId=5000 (oldest), got cmd=%d MsgId=%d", msg3.Cmd, msg3.MsgId)
	}

	// Completion marker.
	done := readMessageFromChan(t, sender.send)
	if done.Cmd != proto.CmdHistory {
		t.Errorf("completion: expected CmdHistory, got cmd=%d", done.Cmd)
	}
	if done.Seq != 3 {
		t.Errorf("completion Seq: expected 3, got %d", done.Seq)
	}

	assertChanEmpty(t, sender.send)
}

func TestRouteHistoryEmpty(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	store := &testMsgStore{} // empty store

	r := NewRouter(h, h, sg, store, DefaultRouterConfig())

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:       proto.CmdHistory,
		To:        "bob",
		Timestamp: 9999999999999,
		Seq:       30,
	})

	// Only the completion marker.
	done := readMessageFromChan(t, sender.send)
	if done.Cmd != proto.CmdHistory {
		t.Errorf("expected CmdHistory completion, got cmd=%d", done.Cmd)
	}
	if done.Seq != 0 {
		t.Errorf("expected Seq=0, got %d", done.Seq)
	}
	assertChanEmpty(t, sender.send)
}

func TestRouteHistoryPagination(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	store := &testMsgStore{}

	now := int64(1721318400000)
	// Pre-populate 5 messages.
	for i := 0; i < 5; i++ {
		store.msgs = append(store.msgs, &proto.Message{
			MsgId:     int64(6000 + i),
			Cmd:       proto.CmdChat,
			From:      "alice",
			To:        "bob",
			ChatType:  proto.ChatTypeSingle,
			MsgType:   proto.MsgTypeText,
			Content:   fmt.Sprintf("p%d", i),
			Timestamp: now + int64(i*1000),
		})
	}

	r := NewRouter(h, h, sg, store, DefaultRouterConfig())

	sender := newTestClient(t, "alice", "Alice")
	// Request with limit=2.
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:       proto.CmdHistory,
		To:        "bob",
		Timestamp: now + 10000, // before this time
		Seq:       2,           // limit 2
	})

	// Should get only 2 newest messages.
	msg1 := readMessageFromChan(t, sender.send)
	if msg1.MsgId != 6004 {
		t.Errorf("msg1: expected MsgId=6004, got %d", msg1.MsgId)
	}
	msg2 := readMessageFromChan(t, sender.send)
	if msg2.MsgId != 6003 {
		t.Errorf("msg2: expected MsgId=6003, got %d", msg2.MsgId)
	}

	done := readMessageFromChan(t, sender.send)
	if done.Cmd != proto.CmdHistory || done.Seq != 2 {
		t.Errorf("completion: expected CmdHistory Seq=2, got cmd=%d Seq=%d", done.Cmd, done.Seq)
	}
	assertChanEmpty(t, sender.send)
}

func TestRouteHistoryOtherUserNotIncluded(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	store := &testMsgStore{}

	now := int64(1721318400000)
	// Alice-Bob message.
	store.msgs = append(store.msgs, &proto.Message{
		MsgId: 7000, Cmd: proto.CmdChat, From: "alice", To: "bob",
		ChatType: proto.ChatTypeSingle, MsgType: proto.MsgTypeText,
		Content: "AB", Timestamp: now,
	})
	// Alice-Carol message (should NOT appear in alice-bob query).
	store.msgs = append(store.msgs, &proto.Message{
		MsgId: 7001, Cmd: proto.CmdChat, From: "alice", To: "carol",
		ChatType: proto.ChatTypeSingle, MsgType: proto.MsgTypeText,
		Content: "AC", Timestamp: now + 1000,
	})

	r := NewRouter(h, h, sg, store, DefaultRouterConfig())

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:       proto.CmdHistory,
		To:        "bob",
		Timestamp: now + 10000,
		Seq:       50,
	})

	msg1 := readMessageFromChan(t, sender.send)
	if msg1.Content != "AB" {
		t.Errorf("expected 'AB', got '%s'", msg1.Content)
	}

	done := readMessageFromChan(t, sender.send)
	if done.Seq != 1 {
		t.Errorf("expected 1 message, got Seq=%d", done.Seq)
	}
	assertChanEmpty(t, sender.send)
}

func TestRouteHistoryDefaultLimit(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	store := &testMsgStore{}

	now := int64(1721318400000)
	for i := 0; i < 50; i++ {
		store.msgs = append(store.msgs, &proto.Message{
			MsgId:     int64(8000 + i),
			Cmd:       proto.CmdChat,
			From:      "alice",
			To:        "bob",
			ChatType:  proto.ChatTypeSingle,
			MsgType:   proto.MsgTypeText,
			Content:   fmt.Sprintf("m%d", i),
			Timestamp: now + int64(i*1000),
		})
	}

	r := NewRouter(h, h, sg, store, DefaultRouterConfig())

	sender := newTestClient(t, "alice", "Alice")
	// Seq=0 should trigger default limit of 30.
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:       proto.CmdHistory,
		To:        "bob",
		Timestamp: now + 50000,
		Seq:       0, // default limit
	})

	// Count messages.
	count := 0
	for {
		msg := readMessageFromChan(t, sender.send)
		if msg.Cmd == proto.CmdHistory {
			if msg.Seq != 30 {
				t.Errorf("expected 30 messages (default limit), got %d", msg.Seq)
			}
			break
		}
		count++
	}
	if count != 30 {
		t.Errorf("expected 30 messages, counted %d", count)
	}
}

	// ---------- Multi-Gateway Hash Ring Routing ----------

	// mockForwarder records all forwarded messages for test assertions.
	type mockForwarder struct {
		mu        sync.Mutex
		forwarded []forwardCall
		// Return values for Forward().
		delivered bool
		err       error
	}

	type forwardCall struct {
		UID string
		Msg *proto.Message
	}

	func (m *mockForwarder) Forward(ctx context.Context, uid string, msg *proto.Message) (bool, error) {
		m.mu.Lock()
		m.forwarded = append(m.forwarded, forwardCall{UID: uid, Msg: msg})
		m.mu.Unlock()
		return m.delivered, m.err
	}

	// makeHashRing creates a HashRing with the given nodes and returns it.
	func makeHashRing(nodeIDs ...string) *HashRing {
		hr := NewHashRing(150)
		for _, id := range nodeIDs {
			hr.Add(id)
		}
		return hr
	}

	func TestRouteChatWithHashRingLocalDelivery(t *testing.T) {
		h := NewHub(100)
		sg, _ := snowflake.New(1)
		r := NewRouter(h, h, sg, nil, DefaultRouterConfig())

		// Hash ring with gw-1, this is gw-1 → we own bob.
		hr := makeHashRing("gw-1", "gw-2")
		r.SetHashRing(hr)
		r.SetThisNodeID("gw-1")

		// Target is locally connected.
		target := newTestClient(t, "bob", "Bob")
		h.Register(context.Background(), target)

		sender := newTestClient(t, "alice", "Alice")
		r.Route(context.Background(), sender, &proto.Message{
			Cmd:      proto.CmdChat,
			From:     "alice",
			To:       "bob",
			ChatType: proto.ChatTypeSingle,
			MsgType:  proto.MsgTypeText,
			Content:  "hello via hash ring",
			NeedAck:  true,
			Seq:      1,
		})

		// Target should receive the message locally.
		delivered := readMessageFromChan(t, target.send)
		if delivered.Content != "hello via hash ring" {
			t.Errorf("expected content 'hello via hash ring', got '%s'", delivered.Content)
		}

		// Sender should receive ACK.
		ack := readMessageFromChan(t, sender.send)
		if ack.Cmd != proto.CmdAck {
			t.Errorf("expected ACK, got cmd=%d", ack.Cmd)
		}
		if ack.Seq != 1 {
			t.Errorf("expected ACK Seq=1, got Seq=%d", ack.Seq)
		}
	}

	func TestRouteChatWithHashRingPeerForward(t *testing.T) {
		h := NewHub(100)
		sg, _ := snowflake.New(1)
		r := NewRouter(h, h, sg, nil, DefaultRouterConfig())

		// Ring has only "gw-2" (not self) → all targets forward to gw-2.
		hr := makeHashRing("gw-2")
		r.SetHashRing(hr)
		r.SetThisNodeID("gw-1")

		fw := &mockForwarder{delivered: true}
		r.SetForwarder(fw)

		// Bob is NOT registered locally.
		sender := newTestClient(t, "alice", "Alice")
		r.Route(context.Background(), sender, &proto.Message{
			Cmd:      proto.CmdChat,
			From:     "alice",
			To:       "bob",
			ChatType: proto.ChatTypeSingle,
			MsgType:  proto.MsgTypeText,
			Content:  "forward me",
			NeedAck:  true,
			Seq:      2,
		})

		// Forwarder should have been called.
		if len(fw.forwarded) != 1 {
			t.Fatalf("expected 1 forwarded call, got %d", len(fw.forwarded))
		}
		if fw.forwarded[0].UID != "bob" {
			t.Errorf("forwarded to wrong UID: %s", fw.forwarded[0].UID)
		}
		if fw.forwarded[0].Msg.Content != "forward me" {
			t.Errorf("forwarded wrong content: %s", fw.forwarded[0].Msg.Content)
		}

		// Sender should receive ACK.
		ack := readMessageFromChan(t, sender.send)
		if ack.Cmd != proto.CmdAck {
			t.Errorf("expected ACK, got cmd=%d", ack.Cmd)
		}

		// No message stored in local offline store (peer handled it).
		offlineMsgs := h.DrainOffline(context.Background(), "bob")
		if len(offlineMsgs) != 0 {
			t.Errorf("expected 0 offline messages stored locally, got %d", len(offlineMsgs))
		}
	}

	func TestRouteChatWithHashRingSelfOffline(t *testing.T) {
		h := NewHub(100)
		sg, _ := snowflake.New(1)
		r := NewRouter(h, h, sg, nil, DefaultRouterConfig())

		// Ring has only "gw-1" (self) → all targets store offline locally.
		hr := makeHashRing("gw-1")
		r.SetHashRing(hr)
		r.SetThisNodeID("gw-1")

		fw := &mockForwarder{delivered: true}
		r.SetForwarder(fw)

		sender := newTestClient(t, "alice", "Alice")
		r.Route(context.Background(), sender, &proto.Message{
			Cmd:      proto.CmdChat,
			From:     "alice",
			To:       "bob",
			ChatType: proto.ChatTypeSingle,
			MsgType:  proto.MsgTypeText,
			Content:  "bob is offline",
			NeedAck:  true,
			Seq:      3,
		})

		// Forwarder should NOT have been called (this node owns everyone).
		if len(fw.forwarded) != 0 {
			t.Errorf("expected 0 forwarded calls, got %d", len(fw.forwarded))
		}

		// Message should be in local offline store.
		offlineMsgs := h.DrainOffline(context.Background(), "bob")
		if len(offlineMsgs) != 1 {
			t.Fatalf("expected 1 offline message, got %d", len(offlineMsgs))
		}
		if offlineMsgs[0].Content != "bob is offline" {
			t.Errorf("wrong offline content: %s", offlineMsgs[0].Content)
		}

		// Sender should receive ACK.
		ack := readMessageFromChan(t, sender.send)
		if ack.Cmd != proto.CmdAck {
			t.Errorf("expected ACK, got cmd=%d", ack.Cmd)
		}
	}

	func TestRouteChatForwardFailsFallback(t *testing.T) {
		h := NewHub(100)
		sg, _ := snowflake.New(1)
		r := NewRouter(h, h, sg, nil, DefaultRouterConfig())

		// Ring has only "gw-2" (not self) → will attempt forward.
		hr := makeHashRing("gw-2")
		r.SetHashRing(hr)
		r.SetThisNodeID("gw-1")

		// Forwarder returns an error (simulate network failure).
		fw := &mockForwarder{err: fmt.Errorf("connection refused")}
		r.SetForwarder(fw)

		sender := newTestClient(t, "alice", "Alice")
		r.Route(context.Background(), sender, &proto.Message{
			Cmd:      proto.CmdChat,
			From:     "alice",
			To:       "bob",
			ChatType: proto.ChatTypeSingle,
			MsgType:  proto.MsgTypeText,
			Content:  "forward will fail",
			NeedAck:  true,
			Seq:      4,
		})

		// Forwarder was called.
		if len(fw.forwarded) != 1 {
			t.Fatalf("expected 1 forwarded call, got %d", len(fw.forwarded))
		}

		// Fallback: message stored in local offline store.
		offlineMsgs := h.DrainOffline(context.Background(), "bob")
		if len(offlineMsgs) != 1 {
			t.Fatalf("expected 1 fallback offline message, got %d", len(offlineMsgs))
		}
		if offlineMsgs[0].Content != "forward will fail" {
			t.Errorf("wrong fallback content: %s", offlineMsgs[0].Content)
		}

		// Sender still receives ACK.
		ack := readMessageFromChan(t, sender.send)
		if ack.Cmd != proto.CmdAck {
			t.Errorf("expected ACK, got cmd=%d", ack.Cmd)
		}
	}

// ---------- Group Chat ----------

func TestGroupChatFanout(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	gs := NewInMemoryGroupStore(sg)

	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	r.SetGroupStore(gs)

	// Create group and add members.
	ctx := context.Background()
	g, err := gs.Create(ctx, "Test Group", "alice", nil)
	if err != nil {
		t.Fatalf("Create group: %v", err)
	}
	gs.AddMember(ctx, g.ID, "bob")
	gs.AddMember(ctx, g.ID, "carol")

	// Register bob and carol as online.
	bob := newTestClient(t, "bob", "Bob")
	carol := newTestClient(t, "carol", "Carol")
	h.Register(ctx, bob)
	h.Register(ctx, carol)

	// Alice sends a group message.
	sender := newTestClient(t, "alice", "Alice")
	r.Route(ctx, sender, &proto.Message{
		Cmd:      proto.CmdChat,
		From:     "alice",
		To:       g.ID,
		ChatType: proto.ChatTypeGroup,
		MsgType:  proto.MsgTypeText,
		Content:  "hello group",
		NeedAck:  true,
		Seq:      1,
	})

	// Bob and carol should receive the message.
	for _, c := range []*Client{bob, carol} {
		msg := readMessageFromChan(t, c.send)
		if msg.Content != "hello group" {
			t.Errorf("%s received wrong content: %q", c.UID, msg.Content)
		}
		if msg.ChatType != proto.ChatTypeGroup {
			t.Errorf("%s received wrong chat_type: %d", c.UID, msg.ChatType)
		}
	}

	// Alice should receive an ACK first (she has NeedAck=true), then nothing else.
	ack := readMessageFromChan(t, sender.send)
	if ack.Cmd != proto.CmdAck {
		t.Errorf("expected ACK, got cmd=%d", ack.Cmd)
	}
	// Alice (sender) should NOT receive her own group message after the ACK.
	assertChanEmpty(t, sender.send)
}

func TestGroupChatOfflineMember(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	gs := NewInMemoryGroupStore(sg)

	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	r.SetGroupStore(gs)

	ctx := context.Background()
	g, _ := gs.Create(ctx, "Dev Team", "alice", nil)
	gs.AddMember(ctx, g.ID, "bob") // bob is NOT registered → offline

	sender := newTestClient(t, "alice", "Alice")
	r.Route(ctx, sender, &proto.Message{
		Cmd:      proto.CmdChat,
		From:     "alice",
		To:       g.ID,
		ChatType: proto.ChatTypeGroup,
		MsgType:  proto.MsgTypeText,
		Content:  "bob is offline",
		Seq:      1,
	})

	// Bob should have the message stored offline.
	offlineMsgs := h.DrainOffline(ctx, "bob")
	if len(offlineMsgs) != 1 {
		t.Fatalf("expected 1 offline message for bob, got %d", len(offlineMsgs))
	}
	if offlineMsgs[0].Content != "bob is offline" {
		t.Errorf("wrong offline content: %q", offlineMsgs[0].Content)
	}
}

func TestGroupChatSenderExcluded(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	gs := NewInMemoryGroupStore(sg)

	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	r.SetGroupStore(gs)

	ctx := context.Background()
	g, _ := gs.Create(ctx, "Solo Group", "alice", nil)

	// Only alice is in the group. Register her.
	alice := newTestClient(t, "alice", "Alice")
	h.Register(ctx, alice)

	// Alice sends a group message — she is the only member.
	r.Route(ctx, alice, &proto.Message{
		Cmd:      proto.CmdChat,
		From:     "alice",
		To:       g.ID,
		ChatType: proto.ChatTypeGroup,
		MsgType:  proto.MsgTypeText,
		Content:  "talking to myself",
		Seq:      1,
	})

	// Alice should NOT receive her own message (sender excluded).
	assertChanEmpty(t, alice.send)
}

func TestGroupChatNoGroupStore(t *testing.T) {
	// When groupStore is nil, group messages fall back to single-chat behavior
	// (treated as direct message to msg.To).
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	// Do NOT call SetGroupStore — groupStore is nil.

	target := newTestClient(t, "bob", "Bob")
	sender := newTestClient(t, "alice", "Alice")
	h.Register(context.Background(), target)

	r.Route(context.Background(), sender, &proto.Message{
		Cmd:      proto.CmdChat,
		From:     "alice",
		To:       "bob",
		ChatType: proto.ChatTypeGroup,
		MsgType:  proto.MsgTypeText,
		Content:  "fallback to single",
		Seq:      1,
	})

	// Falls back to single chat: bob receives the message.
	msg := readMessageFromChan(t, target.send)
	if msg.Content != "fallback to single" {
		t.Errorf("expected 'fallback to single', got %q", msg.Content)
	}
}

func TestGroupChatNonexistentGroup(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	gs := NewInMemoryGroupStore(sg)

	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	r.SetGroupStore(gs)

	sender := newTestClient(t, "alice", "Alice")

	// Send to a group that does not exist — should not panic.
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:      proto.CmdChat,
		From:     "alice",
		To:       "g_nonexistent",
		ChatType: proto.ChatTypeGroup,
		MsgType:  proto.MsgTypeText,
		Content:  "nobody there",
		Seq:      1,
	})

	// No message delivered, no panic — just verify we are still alive.
	assertChanEmpty(t, sender.send)
}
