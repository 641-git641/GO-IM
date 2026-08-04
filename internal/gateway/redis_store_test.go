package gateway

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/im/api/proto"
	"github.com/redis/go-redis/v9"
)

// newRedisTestStore 创建由 miniredis 实例支撑的 RedisOfflineStore。
// 返回的清理函数会停止 miniredis 服务器。
func newRedisTestStore(t *testing.T, maxSize int) (*RedisOfflineStore, func()) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	rs := NewRedisStore(rdb, maxSize)
	return rs, func() {
		rdb.Close()
		mr.Close()
	}
}

// ---------- 映射风格（与 hub_test.go 对应）----------

func TestRedisStoreAndDrain(t *testing.T) {
	rs, cleanup := newRedisTestStore(t, 100)
	defer cleanup()

	msgs := []*proto.Message{
		{Cmd: proto.CmdChat, MsgId: 1, Content: "msg1"},
		{Cmd: proto.CmdChat, MsgId: 2, Content: "msg2"},
		{Cmd: proto.CmdChat, MsgId: 3, Content: "msg3"},
	}

	for _, m := range msgs {
		rs.StoreOffline(context.Background(), "alice", m)
	}

	drained := rs.DrainOffline(context.Background(), "alice")
	if len(drained) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(drained))
	}
	for i, m := range drained {
		if m.MsgId != int64(i+1) {
			t.Errorf("msg[%d]: expected MsgId=%d, got %d", i, i+1, m.MsgId)
		}
	}

	// 再次排空应返回空
	empty := rs.DrainOffline(context.Background(), "alice")
	if len(empty) != 0 {
		t.Errorf("expected empty after drain, got %d messages", len(empty))
	}
}

func TestRedisDrainEmpty(t *testing.T) {
	rs, cleanup := newRedisTestStore(t, 100)
	defer cleanup()

	msgs := rs.DrainOffline(context.Background(), "nonexistent")
	if len(msgs) != 0 {
		t.Errorf("expected empty slice for nonexistent user, got %d messages", len(msgs))
	}
}

func TestRedisOfflineTruncation(t *testing.T) {
	maxSize := 10
	rs, cleanup := newRedisTestStore(t, maxSize)
	defer cleanup()

	// 存储超过上限的消息
	for i := 0; i < maxSize+5; i++ {
		rs.StoreOffline(context.Background(), "alice", &proto.Message{MsgId: int64(i)})
	}

	drained := rs.DrainOffline(context.Background(), "alice")
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

func TestRedisPerUserIsolation(t *testing.T) {
	rs, cleanup := newRedisTestStore(t, 100)
	defer cleanup()

	rs.StoreOffline(context.Background(), "alice", &proto.Message{MsgId: 1, Content: "for alice"})
	rs.StoreOffline(context.Background(), "bob", &proto.Message{MsgId: 2, Content: "for bob"})

	aliceMsgs := rs.DrainOffline(context.Background(), "alice")
	if len(aliceMsgs) != 1 || aliceMsgs[0].Content != "for alice" {
		t.Errorf("alice got wrong messages: %+v", aliceMsgs)
	}

	bobMsgs := rs.DrainOffline(context.Background(), "bob")
	if len(bobMsgs) != 1 || bobMsgs[0].Content != "for bob" {
		t.Errorf("bob got wrong messages: %+v", bobMsgs)
	}
}

func TestRedisMessageRoundTrip(t *testing.T) {
	rs, cleanup := newRedisTestStore(t, 100)
	defer cleanup()

	original := &proto.Message{
		Cmd:       proto.CmdChat,
		MsgId:     123456789,
		Seq:       42,
		From:      "alice",
		To:        "bob",
		ChatType:  proto.ChatTypeSingle,
		MsgType:   proto.MsgTypeText,
		Content:   "Hello, Bob!",
		Timestamp: 1721318400000,
		NeedAck:   true,
	}

	rs.StoreOffline(context.Background(), "bob", original)
	drained := rs.DrainOffline(context.Background(), "bob")

	if len(drained) != 1 {
		t.Fatalf("expected 1 message, got %d", len(drained))
	}

	got := drained[0]
	if got.Cmd != original.Cmd {
		t.Errorf("Cmd: expected %d, got %d", original.Cmd, got.Cmd)
	}
	if got.MsgId != original.MsgId {
		t.Errorf("MsgId: expected %d, got %d", original.MsgId, got.MsgId)
	}
	if got.Seq != original.Seq {
		t.Errorf("Seq: expected %d, got %d", original.Seq, got.Seq)
	}
	if got.From != original.From {
		t.Errorf("From: expected %s, got %s", original.From, got.From)
	}
	if got.To != original.To {
		t.Errorf("To: expected %s, got %s", original.To, got.To)
	}
	if got.ChatType != original.ChatType {
		t.Errorf("ChatType: expected %d, got %d", original.ChatType, got.ChatType)
	}
	if got.MsgType != original.MsgType {
		t.Errorf("MsgType: expected %d, got %d", original.MsgType, got.MsgType)
	}
	if got.Content != original.Content {
		t.Errorf("Content: expected %s, got %s", original.Content, got.Content)
	}
	if got.Timestamp != original.Timestamp {
		t.Errorf("Timestamp: expected %d, got %d", original.Timestamp, got.Timestamp)
	}
	if got.NeedAck != original.NeedAck {
		t.Errorf("NeedAck: expected %v, got %v", original.NeedAck, got.NeedAck)
	}
}

// ---------- 回退测试 ----------

func TestRedisFallbackOnStoreError(t *testing.T) {
	rs, cleanup := newRedisTestStore(t, 100)
	defer cleanup()

	hub := NewHub(100)
	rs.WithFallback(hub)

	// 关闭 miniredis 以强制产生 Redis 错误
	rs.client.Close()

	// StoreOffline 应回退到 Hub
	msg := &proto.Message{Cmd: proto.CmdChat, MsgId: 99, Content: "fallback test"}
	rs.StoreOffline(context.Background(), "bob", msg)

	// 排空应来自 Hub（回退），而不是 Redis
	drained := hub.DrainOffline(context.Background(), "bob")
	if len(drained) != 1 {
		t.Fatalf("expected 1 message from fallback, got %d", len(drained))
	}
	if drained[0].Content != "fallback test" {
		t.Errorf("expected 'fallback test', got '%s'", drained[0].Content)
	}
}

func TestRedisFallbackOnDrainError(t *testing.T) {
	rs, cleanup := newRedisTestStore(t, 100)
	defer cleanup()

	hub := NewHub(100)
	rs.WithFallback(hub)

	// 直接在 Hub 中存储一条消息（模拟回退存储）
	hub.StoreOffline(context.Background(), "bob", &proto.Message{Cmd: proto.CmdChat, MsgId: 77, Content: "hub msg"})

	// 关闭 miniredis 以强制产生 Redis 错误
	rs.client.Close()

	// DrainOffline 应回退到 Hub
	drained := rs.DrainOffline(context.Background(), "bob")
	if len(drained) != 1 {
		t.Fatalf("expected 1 message from fallback drain, got %d", len(drained))
	}
	if drained[0].Content != "hub msg" {
		t.Errorf("expected 'hub msg', got '%s'", drained[0].Content)
	}
}
