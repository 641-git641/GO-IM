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

// readFromChan 从通道读取一个 []byte，带超时。
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

// readMessageFromChan 从通道读取并反序列化 proto.Message。
func readMessageFromChan(t *testing.T, ch <-chan []byte) *proto.Message {
	t.Helper()
	raw := readFromChan(t, ch)
	msg := &proto.Message{}
	if err := pb.Unmarshal(raw, msg); err != nil {
		t.Fatalf("unmarshal message: %v (raw=%s)", err, string(raw))
	}
	return msg
}

// assertChanEmpty 验证通道上没有等待中的消息。
func assertChanEmpty(t *testing.T, ch <-chan []byte) {
	t.Helper()
	select {
	case data := <-ch:
		t.Errorf("expected empty channel, got: %s", string(data))
	default:
		// 预期行为
	}
}

// ---------- 心跳 ----------

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

// ---------- 聊天 —— 在线投递 ----------

func TestRouteChatOnline(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())

	// 在 hub 中注册目标客户端
	target := newTestClient(t, "bob", "Bob")
	h.Register(context.Background(), target)

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:      proto.CmdChat,
		From:     "alice", // 生产环境由 readPump 设置，测试中手动设置
		To:       "bob",
		ChatType: proto.ChatTypeSingle,
		MsgType:  proto.MsgTypeText,
		Content:  "Hello Bob!",
		NeedAck:  true,
		Seq:      1,
	})

	// 目标应收到消息
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

	// 发送者应收到 ACK
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

// ---------- 聊天 —— 离线存储 ----------

func TestRouteChatOffline(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())

	sender := newTestClient(t, "alice", "Alice")
	// Bob 未注册到 hub（离线）

	r.Route(context.Background(), sender, &proto.Message{
		Cmd:      proto.CmdChat,
		From:     "alice", // 生产环境由 readPump 设置，测试中手动设置
		To:       "bob",
		ChatType: proto.ChatTypeSingle,
		MsgType:  proto.MsgTypeText,
		Content:  "Are you there?",
		NeedAck:  true,
		Seq:      1,
	})

	// 即使目标离线，发送者也应收到 ACK
	ack := readMessageFromChan(t, sender.send)
	if ack.Cmd != proto.CmdAck {
		t.Errorf("expected CmdAck, got cmd=%d", ack.Cmd)
	}

	// 消息应存储在离线存储中
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

// ---------- 聊天 —— 发送缓冲区已满的回退 ----------

func TestRouteChatSendBufferFull(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())

	// 创建发送缓冲区极小的目标客户端
	target := &Client{
		UID:      "bob",
		Username: "Bob",
		send:     make(chan []byte, 1), // 缓冲区大小为 1
		closed:   make(chan struct{}),
	}
	h.Register(context.Background(), target)

	sender := newTestClient(t, "alice", "Alice")

	// 预先填满发送缓冲区，使下一次 Send() 返回 ErrSendBufferFull
	target.send <- []byte(`{}`)

	// 现在发送聊天消息 —— Send 应失败（缓冲区已满），回退到离线存储
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:      proto.CmdChat,
		To:       "bob",
		ChatType: proto.ChatTypeSingle,
		MsgType:  proto.MsgTypeText,
		Content:  "Buffer test",
		NeedAck:  true,
		Seq:      1,
	})

	// 发送者仍应收到 ACK
	ack := readMessageFromChan(t, sender.send)
	if ack.Cmd != proto.CmdAck {
		t.Errorf("expected CmdAck, got cmd=%d", ack.Cmd)
	}

	// 消息应存储在离线存储中（发送失败 → 回退）
	offlineMsgs := h.DrainOffline(context.Background(),"bob")
	if len(offlineMsgs) != 1 {
		t.Fatalf("expected 1 offline message (send buffer full fallback), got %d", len(offlineMsgs))
	}
	if offlineMsgs[0].Content != "Buffer test" {
		t.Errorf("expected 'Buffer test', got '%s'", offlineMsgs[0].Content)
	}
}

// ---------- 去重 ----------

func TestRouteDuplicate(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())

	target := newTestClient(t, "bob", "Bob")
	h.Register(context.Background(), target)

	sender := newTestClient(t, "alice", "Alice")

	// 第一条消息
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:      proto.CmdChat,
		To:       "bob",
		ChatType: proto.ChatTypeSingle,
		MsgType:  proto.MsgTypeText,
		Content:  "Original",
		NeedAck:  true,
		Seq:      42,
	})

	// 排空首次投递与 ACK
	firstDelivered := readMessageFromChan(t, target.send)
	_ = readMessageFromChan(t, sender.send) // 首次 ACK
	originalMsgID := firstDelivered.MsgId

	t.Logf("First delivery: msgId=%d", originalMsgID)

	// 相同 Seq 的第二条消息 —— 应被去重
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:      proto.CmdChat,
		To:       "bob",
		ChatType: proto.ChatTypeSingle,
		MsgType:  proto.MsgTypeText,
		Content:  "Duplicate retry",
		NeedAck:  true,
		Seq:      42, // 相同的 Seq
	})

	// 发送者应收到带有原始 MsgId 的重放 ACK
	replayAck := readMessageFromChan(t, sender.send)
	if replayAck.Cmd != proto.CmdAck {
		t.Errorf("expected CmdAck replay, got cmd=%d", replayAck.Cmd)
	}
	if replayAck.MsgId != originalMsgID {
		t.Errorf("replay ACK should have original MsgId=%d, got %d", originalMsgID, replayAck.MsgId)
	}

	// 目标不应收到重复消息
	assertChanEmpty(t, target.send)

	t.Logf("ACK replay verified: msgId=%d matches original ✓", replayAck.MsgId)
}

// ---------- 未知 / 无效命令 ----------

func TestRouteCmdNone(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())

	sender := newTestClient(t, "alice", "Alice")
	// CmdNone 不应 panic 或发送任何内容
	r.Route(context.Background(), sender, &proto.Message{Cmd: proto.CmdNone})

	assertChanEmpty(t, sender.send)
}

func TestRouteUnknownCmd(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())

	sender := newTestClient(t, "alice", "Alice")
	// 未知命令不应 panic
	r.Route(context.Background(), sender, &proto.Message{Cmd: 999})

	assertChanEmpty(t, sender.send)
}

// ---------- 离线排空 ----------

func TestRouteOfflineDrain(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())

	h.StoreOffline(context.Background(),"alice", &proto.Message{
		Cmd: proto.CmdChat, MsgId: 100, Content: "stored msg",
	})

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{Cmd: proto.CmdOffline})

	// 应收到已存储的消息
	delivered := readMessageFromChan(t, sender.send)
	if delivered.Cmd != proto.CmdChat || delivered.MsgId != 100 {
		t.Errorf("expected offline message MsgID=100, got cmd=%d MsgID=%d", delivered.Cmd, delivered.MsgId)
	}

	// 队列应被排空
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

	// 应收到空的 CmdOffline 响应
	resp := readMessageFromChan(t, sender.send)
	if resp.Cmd != proto.CmdOffline {
		t.Errorf("expected CmdOffline response for empty queue, got cmd=%d", resp.Cmd)
	}
}

// ---------- 未请求 ACK ----------

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
		NeedAck:  false, // 未请求 ACK
	})

	// 目标应收到消息
	delivered := readMessageFromChan(t, target.send)
	if delivered.Content != "No ACK needed" {
		t.Errorf("expected 'No ACK needed', got '%s'", delivered.Content)
	}

	// 发送者不应收到 ACK
	assertChanEmpty(t, sender.send)
}

// ---------- 无效聊天（缺少目标）----------

func TestRouteChatInvalidTarget(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())

	sender := newTestClient(t, "alice", "Alice")
	// 缺少 To 字段 —— 应被忽略（Validate 失败）
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:      proto.CmdChat,
		To:       "", // 缺少目标
		ChatType: proto.ChatTypeSingle,
		MsgType:  proto.MsgTypeText,
		Content:  "No target",
	})

	// 无 ACK，无投递
	assertChanEmpty(t, sender.send)
}

// ---------- 历史记录 ----------

// testMsgStore 是用于测试 handleHistory 的最小内存版 MessageStore。
type testMsgStore struct {
	msgs []*proto.Message
}

func (s *testMsgStore) Save(ctx context.Context, msg *proto.Message) error {
	s.msgs = append(s.msgs, msg)
	return nil
}

func (s *testMsgStore) QueryHistory(ctx context.Context, uid1, uid2 string, before int64, limit int) ([]*proto.Message, error) {
	// 过滤 uid1 与 uid2 之间的消息，按从新到旧排序。
	var matches []*proto.Message
	for _, m := range s.msgs {
		if m.Timestamp < before &&
			((m.From == uid1 && m.To == uid2) || (m.From == uid2 && m.To == uid1)) {
			matches = append(matches, m)
		}
	}
	// 按从新到旧排序。
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].Timestamp > matches[j].Timestamp
	})
	if len(matches) > limit {
		matches = matches[:limit]
	}
	return matches, nil
}

func (s *testMsgStore) QueryGroupHistory(ctx context.Context, groupID string, before int64, limit int) ([]*proto.Message, error) {
	// 过滤发送到群组的消息，按从新到旧排序。
	var matches []*proto.Message
	for _, m := range s.msgs {
		if m.Timestamp < before && m.To == groupID && m.ChatType == 2 {
			matches = append(matches, m)
		}
	}
	// 按从新到旧排序。
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

	// 应收到 Seq=0 的 CmdHistory 完成消息。
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
		To:  "", // 缺少目标
	})

	// 无响应 —— Validate 失败。
	assertChanEmpty(t, sender.send)
}

func TestRouteHistorySuccess(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	store := &testMsgStore{}

	now := int64(1721318400000)
	// 预填充 alice 与 bob 之间的 3 条消息。
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
		Timestamp: now + 10000, // 早于此时刻
		Seq:       50,          // 限制条数
	})

	// 应收到 3 条消息（从新到旧），然后是完成消息。
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

	// 完成标记。
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
	store := &testMsgStore{} // 空存储

	r := NewRouter(h, h, sg, store, DefaultRouterConfig())

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:       proto.CmdHistory,
		To:        "bob",
		Timestamp: 9999999999999,
		Seq:       30,
	})

	// 只有完成标记。
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
	// 预填充 5 条消息。
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
	// 请求 limit=2。
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:       proto.CmdHistory,
		To:        "bob",
		Timestamp: now + 10000, // 早于此时刻
		Seq:       2,           // 限制 2 条
	})

	// 应只得到最新的 2 条消息。
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
	// Alice-Bob 消息。
	store.msgs = append(store.msgs, &proto.Message{
		MsgId: 7000, Cmd: proto.CmdChat, From: "alice", To: "bob",
		ChatType: proto.ChatTypeSingle, MsgType: proto.MsgTypeText,
		Content: "AB", Timestamp: now,
	})
	// Alice-Carol 消息（不应出现在 alice-bob 查询中）。
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
	// Seq=0 应触发默认限制 30。
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:       proto.CmdHistory,
		To:        "bob",
		Timestamp: now + 50000,
		Seq:       0, // 默认限制
	})

	// 统计消息条数。
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

	// ---------- 多网关哈希环路由 ----------

	// mockForwarder 记录所有转发的消息，供测试断言使用。
	type mockForwarder struct {
		mu        sync.Mutex
		forwarded []forwardCall
		// Forward() 的返回值。
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

	// makeHashRing 创建包含给定节点的 HashRing 并返回。
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

		// 哈希环包含 gw-1；本节点是 gw-1 → bob 归我们所有。
		hr := makeHashRing("gw-1", "gw-2")
		r.SetHashRing(hr)
		r.SetThisNodeID("gw-1")

		// 目标已本地连接。
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

		// 目标应在本地收到消息。
		delivered := readMessageFromChan(t, target.send)
		if delivered.Content != "hello via hash ring" {
			t.Errorf("expected content 'hello via hash ring', got '%s'", delivered.Content)
		}

		// 发送者应收到 ACK。
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

		// 哈希环只有 "gw-2"（不是自身）→ 所有目标转发到 gw-2。
		hr := makeHashRing("gw-2")
		r.SetHashRing(hr)
		r.SetThisNodeID("gw-1")

		fw := &mockForwarder{delivered: true}
		r.SetForwarder(fw)

		// Bob 未在本地注册。
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

		// 转发器应已被调用。
		if len(fw.forwarded) != 1 {
			t.Fatalf("expected 1 forwarded call, got %d", len(fw.forwarded))
		}
		if fw.forwarded[0].UID != "bob" {
			t.Errorf("forwarded to wrong UID: %s", fw.forwarded[0].UID)
		}
		if fw.forwarded[0].Msg.Content != "forward me" {
			t.Errorf("forwarded wrong content: %s", fw.forwarded[0].Msg.Content)
		}

		// 发送者应收到 ACK。
		ack := readMessageFromChan(t, sender.send)
		if ack.Cmd != proto.CmdAck {
			t.Errorf("expected ACK, got cmd=%d", ack.Cmd)
		}

		// 本地离线存储中无消息（对端已处理）。
		offlineMsgs := h.DrainOffline(context.Background(), "bob")
		if len(offlineMsgs) != 0 {
			t.Errorf("expected 0 offline messages stored locally, got %d", len(offlineMsgs))
		}
	}

	func TestRouteChatWithHashRingSelfOffline(t *testing.T) {
		h := NewHub(100)
		sg, _ := snowflake.New(1)
		r := NewRouter(h, h, sg, nil, DefaultRouterConfig())

		// 哈希环只有 "gw-1"（自身）→ 所有目标在本地离线存储。
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

		// 转发器不应被调用（本节点拥有所有用户）。
		if len(fw.forwarded) != 0 {
			t.Errorf("expected 0 forwarded calls, got %d", len(fw.forwarded))
		}

		// 消息应在本地离线存储中。
		offlineMsgs := h.DrainOffline(context.Background(), "bob")
		if len(offlineMsgs) != 1 {
			t.Fatalf("expected 1 offline message, got %d", len(offlineMsgs))
		}
		if offlineMsgs[0].Content != "bob is offline" {
			t.Errorf("wrong offline content: %s", offlineMsgs[0].Content)
		}

		// 发送者应收到 ACK。
		ack := readMessageFromChan(t, sender.send)
		if ack.Cmd != proto.CmdAck {
			t.Errorf("expected ACK, got cmd=%d", ack.Cmd)
		}
	}

	func TestRouteChatForwardFailsFallback(t *testing.T) {
		h := NewHub(100)
		sg, _ := snowflake.New(1)
		r := NewRouter(h, h, sg, nil, DefaultRouterConfig())

		// 哈希环只有 "gw-2"（不是自身）→ 将尝试转发。
		hr := makeHashRing("gw-2")
		r.SetHashRing(hr)
		r.SetThisNodeID("gw-1")

		// 转发器返回错误（模拟网络故障）。
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

		// 转发器已被调用。
		if len(fw.forwarded) != 1 {
			t.Fatalf("expected 1 forwarded call, got %d", len(fw.forwarded))
		}

		// 回退：消息存储在本地离线存储中。
		offlineMsgs := h.DrainOffline(context.Background(), "bob")
		if len(offlineMsgs) != 1 {
			t.Fatalf("expected 1 fallback offline message, got %d", len(offlineMsgs))
		}
		if offlineMsgs[0].Content != "forward will fail" {
			t.Errorf("wrong fallback content: %s", offlineMsgs[0].Content)
		}

		// 发送者仍收到 ACK。
		ack := readMessageFromChan(t, sender.send)
		if ack.Cmd != proto.CmdAck {
			t.Errorf("expected ACK, got cmd=%d", ack.Cmd)
		}
	}

// ---------- 群聊 ----------

func TestGroupChatFanout(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	gs := NewInMemoryGroupStore(sg)

	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	r.SetGroupStore(gs)

	// 创建群组并添加成员。
	ctx := context.Background()
	g, err := gs.Create(ctx, "Test Group", "alice", nil)
	if err != nil {
		t.Fatalf("Create group: %v", err)
	}
	gs.AddMember(ctx, g.ID, "bob")
	gs.AddMember(ctx, g.ID, "carol")

	// 将 bob 和 carol 注册为在线。
	bob := newTestClient(t, "bob", "Bob")
	carol := newTestClient(t, "carol", "Carol")
	h.Register(ctx, bob)
	h.Register(ctx, carol)

	// Alice 发送一条群组消息。
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

	// Bob 和 carol 应收到该消息。
	for _, c := range []*Client{bob, carol} {
		msg := readMessageFromChan(t, c.send)
		if msg.Content != "hello group" {
			t.Errorf("%s received wrong content: %q", c.UID, msg.Content)
		}
		if msg.ChatType != proto.ChatTypeGroup {
			t.Errorf("%s received wrong chat_type: %d", c.UID, msg.ChatType)
		}
	}

	// Alice 应先收到 ACK（她的 NeedAck=true），然后不再有其他内容。
	ack := readMessageFromChan(t, sender.send)
	if ack.Cmd != proto.CmdAck {
		t.Errorf("expected ACK, got cmd=%d", ack.Cmd)
	}
	// Alice（发送者）在 ACK 之后不应收到自己的群组消息。
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
	gs.AddMember(ctx, g.ID, "bob") // bob 未注册 → 离线

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

	// Bob 的消息应存储在离线存储中。
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

	// 群组中只有 alice。注册她。
	alice := newTestClient(t, "alice", "Alice")
	h.Register(ctx, alice)

	// Alice 发送群组消息 —— 她是唯一的成员。
	r.Route(ctx, alice, &proto.Message{
		Cmd:      proto.CmdChat,
		From:     "alice",
		To:       g.ID,
		ChatType: proto.ChatTypeGroup,
		MsgType:  proto.MsgTypeText,
		Content:  "talking to myself",
		Seq:      1,
	})

	// Alice 不应收到自己的消息（排除发送者）。
	assertChanEmpty(t, alice.send)
}

func TestGroupChatNoGroupStore(t *testing.T) {
	// 当 groupStore 为 nil 时，群组消息回退为单聊行为
	// （视为发给 msg.To 的直接消息）。
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	// 不调用 SetGroupStore —— groupStore 为 nil。

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

	// 回退为单聊：bob 收到消息。
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

	// 向不存在的群组发送消息 —— 不应 panic。
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:      proto.CmdChat,
		From:     "alice",
		To:       "g_nonexistent",
		ChatType: proto.ChatTypeGroup,
		MsgType:  proto.MsgTypeText,
		Content:  "nobody there",
		Seq:      1,
	})

	// 无消息投递，无 panic —— 只验证程序仍然存活。
	assertChanEmpty(t, sender.send)
}
