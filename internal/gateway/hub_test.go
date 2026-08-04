package gateway

import (
	"context"
	"testing"
	"time"

	"github.com/im/api/proto"
)

// newTestClient 创建用于单元测试 hub 操作的 Client。
// 返回的客户端 Transport 为 nil —— 仅对不调用 Close() 的测试安全。
// 需要 Close() 的测试（例如重复注册踢下线）由集成测试覆盖。
func newTestClient(t *testing.T, uid, username string) *Client {
	t.Helper()
	return &Client{
		UID:           uid,
		Username:      username,
		send:          make(chan []byte, 256),
		closed:        make(chan struct{}),
		lastHeartbeat: time.Now(),
	}
}

// ---------- 映射操作 ----------

func TestRegisterAndGet(t *testing.T) {
	h := NewHub(100)
	c := newTestClient(t, "alice", "Alice")
	h.Register(context.Background(),c)

	got := h.Get(context.Background(),"alice")
	if got != c {
		t.Fatal("Get returned nil or wrong client")
	}
	if got.UID != "alice" {
		t.Errorf("expected UID='alice', got '%s'", got.UID)
	}
}

func TestUnregister(t *testing.T) {
	h := NewHub(100)
	c := newTestClient(t, "alice", "Alice")
	h.Register(context.Background(),c)
	h.Unregister(context.Background(), c)

	if h.Get(context.Background(),"alice") != nil {
		t.Error("expected nil after Unregister")
	}
}

func TestIsOnline(t *testing.T) {
	h := NewHub(100)

	if h.IsOnline(context.Background(),"alice") {
		t.Error("alice should be offline initially")
	}

	h.Register(context.Background(),newTestClient(t, "alice", "Alice"))
	if !h.IsOnline(context.Background(),"alice") {
		t.Error("alice should be online after Register")
	}

	alice := h.Get(context.Background(), "alice")
	h.Unregister(context.Background(), alice)
	if h.IsOnline(context.Background(),"alice") {
		t.Error("alice should be offline after Unregister")
	}
}

func TestCount(t *testing.T) {
	h := NewHub(100)

	if h.Count(context.Background()) != 0 {
		t.Errorf("expected 0, got %d", h.Count(context.Background()))
	}

	h.Register(context.Background(),newTestClient(t, "alice", "Alice"))
	if h.Count(context.Background()) != 1 {
		t.Errorf("expected 1, got %d", h.Count(context.Background()))
	}

	h.Register(context.Background(),newTestClient(t, "bob", "Bob"))
	if h.Count(context.Background()) != 2 {
		t.Errorf("expected 2, got %d", h.Count(context.Background()))
	}

	alice := h.Get(context.Background(), "alice")
	h.Unregister(context.Background(), alice)
	if h.Count(context.Background()) != 1 {
		t.Errorf("expected 1 after unregister, got %d", h.Count(context.Background()))
	}
}

func TestOnlineUsers(t *testing.T) {
	h := NewHub(100)

	users := h.OnlineUsers(context.Background())
	if len(users) != 0 {
		t.Errorf("expected empty list, got %v", users)
	}

	h.Register(context.Background(),newTestClient(t, "alice", "Alice"))
	h.Register(context.Background(),newTestClient(t, "bob", "Bob"))

	users = h.OnlineUsers(context.Background())
	if len(users) != 2 {
		t.Errorf("expected 2 users, got %d: %v", len(users), users)
	}

	// 检查两个用户都存在
	found := make(map[string]bool)
	for _, u := range users {
		found[u] = true
	}
	if !found["alice"] || !found["bob"] {
		t.Errorf("missing expected users in list: %v", users)
	}
}

// ---------- 离线队列操作 ----------

func TestOfflineStoreAndDrain(t *testing.T) {
	h := NewHub(100)

	msgs := []*proto.Message{
		{Cmd: proto.CmdChat, MsgId: 1, Content: "msg1"},
		{Cmd: proto.CmdChat, MsgId: 2, Content: "msg2"},
		{Cmd: proto.CmdChat, MsgId: 3, Content: "msg3"},
	}

	for _, m := range msgs {
		h.StoreOffline(context.Background(),"alice", m)
	}

	drained := h.DrainOffline(context.Background(),"alice")
	if len(drained) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(drained))
	}
	for i, m := range drained {
		if m.MsgId != int64(i+1) {
			t.Errorf("msg[%d]: expected MsgId=%d, got %d", i, i+1, m.MsgId)
		}
	}

	// 再次排空应返回空
	empty := h.DrainOffline(context.Background(),"alice")
	if len(empty) != 0 {
		t.Errorf("expected empty after drain, got %d messages", len(empty))
	}
}

func TestDrainEmptyOffline(t *testing.T) {
	h := NewHub(100)

	msgs := h.DrainOffline(context.Background(),"nonexistent")
	if len(msgs) != 0 {
		t.Errorf("expected empty slice for nonexistent user, got %d messages", len(msgs))
	}
}

func TestOfflineQueueTruncation(t *testing.T) {
	maxSize := 10
	h := NewHub(maxSize)

	// 存储超过上限的消息
	for i := 0; i < maxSize+5; i++ {
		h.StoreOffline(context.Background(),"alice", &proto.Message{MsgId: int64(i)})
	}

	drained := h.DrainOffline(context.Background(),"alice")
	if len(drained) != maxSize {
		t.Fatalf("expected %d messages after truncation, got %d", maxSize, len(drained))
	}

	// 最旧的应被丢弃（前 5 条），因此第一条消息应为 #5
	if drained[0].MsgId != 5 {
		t.Errorf("expected oldest kept msg to be #5, got #%d", drained[0].MsgId)
	}
	if drained[len(drained)-1].MsgId != 14 {
		t.Errorf("expected newest msg to be #14, got #%d", drained[len(drained)-1].MsgId)
	}
}

func TestOfflinePerUserIsolation(t *testing.T) {
	h := NewHub(100)

	h.StoreOffline(context.Background(),"alice", &proto.Message{MsgId: 1, Content: "for alice"})
	h.StoreOffline(context.Background(),"bob", &proto.Message{MsgId: 2, Content: "for bob"})

	aliceMsgs := h.DrainOffline(context.Background(),"alice")
	if len(aliceMsgs) != 1 || aliceMsgs[0].Content != "for alice" {
		t.Errorf("alice got wrong messages: %+v", aliceMsgs)
	}

	bobMsgs := h.DrainOffline(context.Background(),"bob")
	if len(bobMsgs) != 1 || bobMsgs[0].Content != "for bob" {
		t.Errorf("bob got wrong messages: %+v", bobMsgs)
	}
}
