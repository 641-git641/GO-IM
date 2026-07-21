package gateway

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/im/api/proto"
	"github.com/im/internal/pkg/snowflake"
)

// --- Read Receipt Tests ---

func TestReadReceiptClearsUnread(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	ut := NewInMemoryUnreadTracker()
	r.SetUnreadTracker(ut)

	// Pre-populate unread: Bob has 3 unread from Alice.
	ctx := context.Background()
	ut.Increment(ctx, "bob", "alice")
	ut.Increment(ctx, "bob", "alice")
	ut.Increment(ctx, "bob", "alice")

	// Register both users online.
	alice := newTestClient(t, "alice", "Alice")
	bob := newTestClient(t, "bob", "Bob")
	h.Register(ctx, alice)
	h.Register(ctx, bob)

	// Bob sends a read receipt for Alice's messages.
	r.Route(ctx, bob, &proto.Message{
		Cmd:   proto.CmdReadReceipt,
		From:  "bob",
		To:    "alice",
		MsgId: 123,
	})

	// Bob's unread count from Alice should be cleared.
	if c := ut.GetCount(ctx, "bob", "alice"); c != 0 {
		t.Errorf("expected unread count 0 after read receipt, got %d", c)
	}

	// Alice should receive the forwarded read receipt.
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

	// Only Bob is online; Alice is NOT registered in Hub.
	bob := newTestClient(t, "bob", "Bob")
	h.Register(ctx, bob)

	// Bob sends a read receipt for Alice's messages.
	// Alice is offline, so the receipt won't be forwarded.
	r.Route(ctx, bob, &proto.Message{
		Cmd:   proto.CmdReadReceipt,
		From:  "bob",
		To:    "alice",
		MsgId: 123,
	})

	// Unread should still be cleared for Bob.
	if c := ut.GetCount(ctx, "bob", "alice"); c != 0 {
		t.Errorf("expected unread count 0 after read receipt, got %d", c)
	}

	// No crash, no message sent to Bob (only receipt forwarding, nothing to sender).
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

	// Empty To field — should be rejected.
	r.Route(ctx, bob, &proto.Message{
		Cmd:  proto.CmdReadReceipt,
		From: "bob",
		To:   "",
	})
	assertChanEmpty(t, bob.send)

	// To == From (self-receipt) — should be rejected.
	r.Route(ctx, bob, &proto.Message{
		Cmd:  proto.CmdReadReceipt,
		From: "bob",
		To:   "bob",
	})
	assertChanEmpty(t, bob.send)

	// Unread count should NOT have been cleared (invalid receipts are no-ops).
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

	// Set up hash ring with multi-node: this node = "node1", peer = "node2".
	hr := makeHashRing("node1", "node2")
	r.SetHashRing(hr)
	// Set thisNodeID to something NOT in the ring, so all users are "owned"
	// by other nodes and forwarding is triggered for every lookup.
	r.SetThisNodeID("self")

	// Mock forwarder — alice is on node2, delivered=true.
	fw := &mockForwarder{delivered: true}
	r.SetForwarder(fw)

	ctx := context.Background()
	ut.Increment(ctx, "bob", "alice")

	bob := newTestClient(t, "bob", "Bob")
	h.Register(ctx, bob)
	// Alice is NOT registered locally — she's on node2.

	r.Route(ctx, bob, &proto.Message{
		Cmd:   proto.CmdReadReceipt,
		From:  "bob",
		To:    "alice",
		MsgId: 123,
	})

	// Unread should be cleared.
	if c := ut.GetCount(ctx, "bob", "alice"); c != 0 {
		t.Errorf("expected unread count 0, got %d", c)
	}

	// Forwarder should have been called for alice.
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

// --- Unread Count Tests ---

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

	// Parse JSON from Content.
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

// --- Chat Increments Unread Tests ---

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

	// Bob should have 1 unread from Alice.
	if c := ut.GetCount(ctx, "bob", "alice"); c != 1 {
		t.Errorf("expected bob unread count 1, got %d", c)
	}

	// Alice should have no unread from this (she's the sender).
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
	// Bob is NOT registered (offline).
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

	// Bob should still have 1 unread from Alice even though he's offline.
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

	// Create group and add members.
	gs := NewInMemoryGroupStore(sg)
	r.SetGroupStore(gs)
	g, _ := gs.Create(ctx, "Test Group", "alice", nil)
	gs.AddMember(ctx, g.ID, "bob")
	gs.AddMember(ctx, g.ID, "carol")

	// Bob and Carol are online.
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

	// Bob and Carol should each have 1 unread from Alice.
	if c := ut.GetCount(ctx, "bob", "alice"); c != 1 {
		t.Errorf("expected bob unread count 1, got %d", c)
	}
	if c := ut.GetCount(ctx, "carol", "alice"); c != 1 {
		t.Errorf("expected carol unread count 1, got %d", c)
	}

	// Alice (sender) should NOT have unread from herself.
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

	// Self-chat should not create unread count.
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
