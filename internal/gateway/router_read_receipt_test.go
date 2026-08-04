package gateway

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/im/api/proto"
	"github.com/im/internal/pkg/snowflake"
)

// --- 已读回执测试 ---

func TestReadReceiptClearsUnread(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	ut := NewInMemoryUnreadTracker()
	r.SetUnreadTracker(ut)

	// 预置未读：Bob 有 3 条来自 Alice 的未读。
	ctx := context.Background()
	ut.Increment(ctx, "bob", "alice")
	ut.Increment(ctx, "bob", "alice")
	ut.Increment(ctx, "bob", "alice")

	// 将两个用户注册为在线。
	alice := newTestClient(t, "alice", "Alice")
	bob := newTestClient(t, "bob", "Bob")
	h.Register(ctx, alice)
	h.Register(ctx, bob)

	// Bob 发送一条针对 Alice 消息的已读回执。
	r.Route(ctx, bob, &proto.Message{
		Cmd:   proto.CmdReadReceipt,
		From:  "bob",
		To:    "alice",
		MsgId: 123,
	})

	// Bob 来自 Alice 的未读计数应被清除。
	if c := ut.GetCount(ctx, "bob", "alice"); c != 0 {
		t.Errorf("expected unread count 0 after read receipt, got %d", c)
	}

	// Alice 应收到转发的已读回执。
	receipt := readMessageFromChan(t, alice.send)
	if receipt.Cmd != proto.CmdReadReceipt {
		t.Errorf("expected CmdReadReceipt, got %d", receipt.Cmd)
	}
	if receipt.From != "bob" {
		t.Errorf("expected From=bob, got %q", receipt.From)
	}
	if receipt.To != "alice" {
		t.Errorf("expected To=alice, got %q", receipt.To)
	}
}

func TestReadReceiptPeerOffline(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	ut := NewInMemoryUnreadTracker()
	r.SetUnreadTracker(ut)

	ctx := context.Background()
	ut.Increment(ctx, "bob", "alice")

	// 只有 Bob 在线；Alice 未注册到 Hub。
	bob := newTestClient(t, "bob", "Bob")
	h.Register(ctx, bob)

	// Bob 发送一条针对 Alice 消息的已读回执。
	// Alice 离线，因此回执不会被转发。
	r.Route(ctx, bob, &proto.Message{
		Cmd:   proto.CmdReadReceipt,
		From:  "bob",
		To:    "alice",
		MsgId: 123,
	})

	// Bob 的未读仍应被清除。
	if c := ut.GetCount(ctx, "bob", "alice"); c != 0 {
		t.Errorf("expected unread count 0 after read receipt, got %d", c)
	}

	// 无崩溃，无消息发送给 Bob（只转发回执，不向发送者发送任何内容）。
	assertChanEmpty(t, bob.send)
}

func TestReadReceiptInvalidPeer(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	ut := NewInMemoryUnreadTracker()
	r.SetUnreadTracker(ut)

	ctx := context.Background()
	ut.Increment(ctx, "bob", "alice")

	bob := newTestClient(t, "bob", "Bob")
	h.Register(ctx, bob)

	// To 字段为空 —— 应被拒绝。
	r.Route(ctx, bob, &proto.Message{
		Cmd:  proto.CmdReadReceipt,
		From: "bob",
		To:   "",
	})
	assertChanEmpty(t, bob.send)

	// To == From（自我回执）—— 应被拒绝。
	r.Route(ctx, bob, &proto.Message{
		Cmd:  proto.CmdReadReceipt,
		From: "bob",
		To:   "bob",
	})
	assertChanEmpty(t, bob.send)

	// 未读计数不应被清除（无效回执是空操作）。
	if c := ut.GetCount(ctx, "bob", "alice"); c != 1 {
		t.Errorf("expected unread count 1 (unchanged), got %d", c)
	}
}

func TestReadReceiptForwardedToPeer(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	ut := NewInMemoryUnreadTracker()
	r.SetUnreadTracker(ut)

	// 设置多节点哈希环：本节点 = "node1"，对端 = "node2"。
	hr := makeHashRing("node1", "node2")
	r.SetHashRing(hr)
	// 将 thisNodeID 设置为不在环中的值，使所有用户都"归属"
	// 其他节点，从而每次查找都会触发转发。
	r.SetThisNodeID("self")

	// 模拟转发器 —— alice 在 node2 上，delivered=true。
	fw := &mockForwarder{delivered: true}
	r.SetForwarder(fw)

	ctx := context.Background()
	ut.Increment(ctx, "bob", "alice")

	bob := newTestClient(t, "bob", "Bob")
	h.Register(ctx, bob)
	// Alice 未在本地注册 —— 她在 node2 上。

	r.Route(ctx, bob, &proto.Message{
		Cmd:   proto.CmdReadReceipt,
		From:  "bob",
		To:    "alice",
		MsgId: 123,
	})

	// 未读应被清除。
	if c := ut.GetCount(ctx, "bob", "alice"); c != 0 {
		t.Errorf("expected unread count 0, got %d", c)
	}

	// 转发器应已为 alice 被调用。
	if len(fw.forwarded) != 1 {
		t.Fatalf("expected 1 forwarded call, got %d", len(fw.forwarded))
	}
	if fw.forwarded[0].UID != "alice" {
		t.Errorf("expected forwarded to alice, got %q", fw.forwarded[0].UID)
	}
	if fw.forwarded[0].Msg.Cmd != proto.CmdReadReceipt {
		t.Errorf("expected forwarded CmdReadReceipt, got %d", fw.forwarded[0].Msg.Cmd)
	}
}

// --- 未读数量测试 ---

func TestUnreadCountReturnsCounts(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	ut := NewInMemoryUnreadTracker()
	r.SetUnreadTracker(ut)

	ctx := context.Background()
	ut.Increment(ctx, "alice", "bob")
	ut.Increment(ctx, "alice", "bob")
	ut.Increment(ctx, "alice", "carol")

	alice := newTestClient(t, "alice", "Alice")
	h.Register(ctx, alice)

	r.Route(ctx, alice, &proto.Message{
		Cmd:  proto.CmdUnreadCount,
		From: "alice",
	})

	resp := readMessageFromChan(t, alice.send)
	if resp.Cmd != proto.CmdUnreadCount {
		t.Fatalf("expected CmdUnreadCount, got %d", resp.Cmd)
	}

	// 从 Content 解析 JSON。
	var result struct {
		UID    string           `json:"uid"`
		Counts map[string]int64 `json:"counts"`
	}
	if err := json.Unmarshal([]byte(resp.Content), &result); err != nil {
		t.Fatalf("unmarshal Content: %v", err)
	}
	if result.UID != "alice" {
		t.Errorf("expected uid=alice, got %q", result.UID)
	}
	if result.Counts["bob"] != 2 {
		t.Errorf("expected bob count 2, got %d", result.Counts["bob"])
	}
	if result.Counts["carol"] != 1 {
		t.Errorf("expected carol count 1, got %d", result.Counts["carol"])
	}
}

func TestUnreadCountEmpty(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	ut := NewInMemoryUnreadTracker()
	r.SetUnreadTracker(ut)

	ctx := context.Background()
	alice := newTestClient(t, "alice", "Alice")
	h.Register(ctx, alice)

	r.Route(ctx, alice, &proto.Message{
		Cmd:  proto.CmdUnreadCount,
		From: "alice",
	})

	resp := readMessageFromChan(t, alice.send)
	if resp.Cmd != proto.CmdUnreadCount {
		t.Fatalf("expected CmdUnreadCount, got %d", resp.Cmd)
	}

	var result struct {
		UID    string           `json:"uid"`
		Counts map[string]int64 `json:"counts"`
	}
	if err := json.Unmarshal([]byte(resp.Content), &result); err != nil {
		t.Fatalf("unmarshal Content: %v", err)
	}
	if len(result.Counts) != 0 {
		t.Errorf("expected empty counts, got %d entries", len(result.Counts))
	}
}

// --- 聊天增加未读的测试 ---

func TestChatIncrementsUnread(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	ut := NewInMemoryUnreadTracker()
	r.SetUnreadTracker(ut)

	ctx := context.Background()
	bob := newTestClient(t, "bob", "Bob")
	h.Register(ctx, bob)

	alice := newTestClient(t, "alice", "Alice")
	r.Route(ctx, alice, &proto.Message{
		Cmd:      proto.CmdChat,
		From:     "alice",
		To:       "bob",
		ChatType: proto.ChatTypeSingle,
		MsgType:  proto.MsgTypeText,
		Content:  "Hello Bob!",
		Seq:      1,
	})

	// Bob 应有 1 条来自 Alice 的未读。
	if c := ut.GetCount(ctx, "bob", "alice"); c != 1 {
		t.Errorf("expected bob unread count 1, got %d", c)
	}

	// Alice 不应因此产生未读（她是发送者）。
	if c := ut.GetCount(ctx, "alice", "bob"); c != 0 {
		t.Errorf("expected alice unread count 0, got %d", c)
	}
}

func TestChatIncrementsUnreadOffline(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	ut := NewInMemoryUnreadTracker()
	r.SetUnreadTracker(ut)

	ctx := context.Background()
	// Bob 未注册（离线）。
	alice := newTestClient(t, "alice", "Alice")

	r.Route(ctx, alice, &proto.Message{
		Cmd:      proto.CmdChat,
		From:     "alice",
		To:       "bob",
		ChatType: proto.ChatTypeSingle,
		MsgType:  proto.MsgTypeText,
		Content:  "Hello Bob!",
		Seq:      1,
	})

	// 即使 Bob 离线，他仍应有 1 条来自 Alice 的未读。
	if c := ut.GetCount(ctx, "bob", "alice"); c != 1 {
		t.Errorf("expected bob unread count 1, got %d", c)
	}
}

func TestGroupChatIncrementsUnread(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	ut := NewInMemoryUnreadTracker()
	r.SetUnreadTracker(ut)

	ctx := context.Background()

	// 创建群组并添加成员。
	gs := NewInMemoryGroupStore(sg)
	r.SetGroupStore(gs)
	g, _ := gs.Create(ctx, "Test Group", "alice", nil)
	gs.AddMember(ctx, g.ID, "bob")
	gs.AddMember(ctx, g.ID, "carol")

	// Bob 和 Carol 在线。
	bob := newTestClient(t, "bob", "Bob")
	carol := newTestClient(t, "carol", "Carol")
	h.Register(ctx, bob)
	h.Register(ctx, carol)

	alice := newTestClient(t, "alice", "Alice")
	r.Route(ctx, alice, &proto.Message{
		Cmd:      proto.CmdChat,
		From:     "alice",
		To:       g.ID,
		ChatType: proto.ChatTypeGroup,
		MsgType:  proto.MsgTypeText,
		Content:  "Hello everyone!",
		Seq:      1,
	})

	// Bob 和 Carol 应各有 1 条来自 Alice 的未读。
	if c := ut.GetCount(ctx, "bob", "alice"); c != 1 {
		t.Errorf("expected bob unread count 1, got %d", c)
	}
	if c := ut.GetCount(ctx, "carol", "alice"); c != 1 {
		t.Errorf("expected carol unread count 1, got %d", c)
	}

	// Alice（发送者）不应有来自自己的未读。
	if c := ut.GetCount(ctx, "alice", "alice"); c != 0 {
		t.Errorf("expected alice unread count 0 (excluded as sender), got %d", c)
	}
}

func TestChatSelfDoesNotIncrementUnread(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	ut := NewInMemoryUnreadTracker()
	r.SetUnreadTracker(ut)

	ctx := context.Background()
	alice := newTestClient(t, "alice", "Alice")
	h.Register(ctx, alice)

	r.Route(ctx, alice, &proto.Message{
		Cmd:      proto.CmdChat,
		From:     "alice",
		To:       "alice",
		ChatType: proto.ChatTypeSingle,
		MsgType:  proto.MsgTypeText,
		Content:  "Note to self",
		Seq:      1,
	})

	// 自我聊天不应产生未读计数。
	if c := ut.GetCount(ctx, "alice", "alice"); c != 0 {
		t.Errorf("expected self-chat unread count 0, got %d", c)
	}
}

func TestUnreadCountUnknownUser(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	ut := NewInMemoryUnreadTracker()
	r.SetUnreadTracker(ut)

	ctx := context.Background()
	unknown := newTestClient(t, "unknown", "Unknown")
	h.Register(ctx, unknown)

	r.Route(ctx, unknown, &proto.Message{
		Cmd:  proto.CmdUnreadCount,
		From: "unknown",
	})

	resp := readMessageFromChan(t, unknown.send)
	if resp.Cmd != proto.CmdUnreadCount {
		t.Fatalf("expected CmdUnreadCount, got %d", resp.Cmd)
	}

	var result struct {
		UID    string           `json:"uid"`
		Counts map[string]int64 `json:"counts"`
	}
	if err := json.Unmarshal([]byte(resp.Content), &result); err != nil {
		t.Fatalf("unmarshal Content: %v", err)
	}
	if len(result.Counts) != 0 {
		t.Errorf("expected empty counts for unknown user, got %d entries", len(result.Counts))
	}
}
