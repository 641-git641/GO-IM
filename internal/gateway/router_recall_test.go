package gateway

import (
	"context"
	"fmt"
	"testing"

	"github.com/im/api/proto"
	"github.com/im/internal/pkg/snowflake"
	"github.com/im/internal/repo"
)

// ---------------------------------------------------------------------------
// CmdRecall — message recall tests
// ---------------------------------------------------------------------------

// testMsgStoreForRecall is a MessageStore that records recall calls and
// stores messages for timestamp verification.
type testMsgStoreForRecall struct {
	msgs          []*proto.Message
	recalled      []int64 // msgIDs that were recalled
	recallErr     error   // return this error from RecallMessage
	recallFromUID string  // record the fromUID passed to RecallMessage
}

func (s *testMsgStoreForRecall) Save(ctx context.Context, msg *proto.Message) error {
	s.msgs = append(s.msgs, msg)
	return nil
}

func (s *testMsgStoreForRecall) QueryHistory(ctx context.Context, uid1, uid2 string, before int64, limit int) ([]*proto.Message, error) {
	return nil, nil
}

func (s *testMsgStoreForRecall) QueryGroupHistory(ctx context.Context, groupID string, before int64, limit int) ([]*proto.Message, error) {
	return nil, nil
}

func (s *testMsgStoreForRecall) SearchMessages(ctx context.Context, params *repo.SearchParams) (*repo.SearchResult, error) {
	return nil, nil
}

func (s *testMsgStoreForRecall) RecallMessage(ctx context.Context, msgID int64, fromUID string, recallWindowMs int64) error {
	s.recalled = append(s.recalled, msgID)
	s.recallFromUID = fromUID
	return s.recallErr
}

func (s *testMsgStoreForRecall) UpdateMessageContent(ctx context.Context, msgID int64, fromUID, newContent string) error {
	return nil
}

func (s *testMsgStoreForRecall) BrowseMessages(ctx context.Context, before int64, limit int) ([]*proto.Message, error) {
	return nil, nil
}

func (s *testMsgStoreForRecall) DeleteMessage(ctx context.Context, msgID int64) error {
	return nil
}

func (s *testMsgStoreForRecall) CountMessages(ctx context.Context) (int, error) {
	return 0, nil
}

// TestRecallOnline verifies that a recall notification is delivered to an
// online target peer with the correct fields.
func TestRecallOnline(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	store := &testMsgStoreForRecall{}
	r := NewRouter(h, h, sg, store, DefaultRouterConfig())

	target := newTestClient(t, "bob", "Bob")
	h.Register(context.Background(), target)

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:  proto.CmdRecall,
		From: "alice",
		To:   "bob",
		Seq:  12345678, // original MsgID to recall
	})

	// Target should receive the recall notification.
	delivered := readMessageFromChan(t, target.send)
	if delivered.Cmd != proto.CmdRecall {
		t.Errorf("expected CmdRecall, got cmd=%d", delivered.Cmd)
	}
	if delivered.From != "alice" {
		t.Errorf("expected From=alice, got %s", delivered.From)
	}
	if delivered.To != "bob" {
		t.Errorf("expected To=bob, got %s", delivered.To)
	}
	if delivered.MsgId == 0 {
		t.Error("expected non-zero MsgId")
	}
	if delivered.Seq != 12345678 {
		t.Errorf("expected Seq=12345678 (original MsgID), got %d", delivered.Seq)
	}

	// RecallMessage was called on the store.
	if len(store.recalled) != 1 {
		t.Errorf("expected 1 recall call, got %d", len(store.recalled))
	}
	if store.recalled[0] != 12345678 {
		t.Errorf("expected recalled msgID=12345678, got %d", store.recalled[0])
	}
	if store.recallFromUID != "alice" {
		t.Errorf("expected recallFromUID=alice, got %s", store.recallFromUID)
	}

	// Sender should NOT receive an ACK.
	assertChanEmpty(t, sender.send)
}

// TestRecallOffline verifies that recall notifications for offline targets
// are stored in the offline queue.
func TestRecallOffline(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	store := &testMsgStoreForRecall{}
	r := NewRouter(h, h, sg, store, DefaultRouterConfig())

	// Bob is NOT registered (offline).
	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:  proto.CmdRecall,
		From: "alice",
		To:   "bob",
		Seq:  999888777,
	})

	// Recall notification should be stored offline.
	offlineMsgs := h.DrainOffline(context.Background(), "bob")
	if len(offlineMsgs) != 1 {
		t.Fatalf("expected 1 offline recall for bob, got %d", len(offlineMsgs))
	}
	if offlineMsgs[0].Cmd != proto.CmdRecall {
		t.Errorf("expected CmdRecall offline, got cmd=%d", offlineMsgs[0].Cmd)
	}
	if offlineMsgs[0].Seq != 999888777 {
		t.Errorf("expected Seq=999888777 offline, got %d", offlineMsgs[0].Seq)
	}
}

// TestRecallMissingTarget verifies that recall without a target is rejected.
func TestRecallMissingTarget(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	store := &testMsgStoreForRecall{}
	r := NewRouter(h, h, sg, store, DefaultRouterConfig())

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:  proto.CmdRecall,
		From: "alice",
		To:   "", // missing target
		Seq:  123,
	})

	// Nothing should be delivered, no recall called.
	assertChanEmpty(t, sender.send)
	if len(store.recalled) != 0 {
		t.Errorf("expected 0 recall calls, got %d", len(store.recalled))
	}
}

// TestRecallSelfTarget verifies that self-recall is rejected.
func TestRecallSelfTarget(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	store := &testMsgStoreForRecall{}
	r := NewRouter(h, h, sg, store, DefaultRouterConfig())

	sender := newTestClient(t, "alice", "Alice")
	h.Register(context.Background(), sender)

	r.Route(context.Background(), sender, &proto.Message{
		Cmd:  proto.CmdRecall,
		From: "alice",
		To:   "alice", // self-target
		Seq:  123,
	})

	assertChanEmpty(t, sender.send)
	if len(store.recalled) != 0 {
		t.Errorf("expected 0 recall calls, got %d", len(store.recalled))
	}
}

// TestRecallWithoutSeq verifies that recall without Seq (original MsgID) is rejected.
func TestRecallWithoutSeq(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	store := &testMsgStoreForRecall{}
	r := NewRouter(h, h, sg, store, DefaultRouterConfig())

	target := newTestClient(t, "bob", "Bob")
	h.Register(context.Background(), target)

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:  proto.CmdRecall,
		From: "alice",
		To:   "bob",
		Seq:  0, // missing original MsgID
	})

	// Sender should get error response.
	errMsg := readMessageFromChan(t, sender.send)
	if errMsg.Cmd != proto.CmdRecall {
		t.Errorf("expected CmdRecall error, got cmd=%d", errMsg.Cmd)
	}
	if errMsg.Content != `{"error":"missing original message ID"}` {
		t.Errorf("expected error content, got %s", errMsg.Content)
	}

	// Target should receive nothing.
	assertChanEmpty(t, target.send)
	if len(store.recalled) != 0 {
		t.Errorf("expected 0 recall calls, got %d", len(store.recalled))
	}
}

// TestRecallFromOverwrite verifies that the authenticated sender UID overwrites
// whatever the client sent in msg.From.
func TestRecallFromOverwrite(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	store := &testMsgStoreForRecall{}
	r := NewRouter(h, h, sg, store, DefaultRouterConfig())

	target := newTestClient(t, "bob", "Bob")
	h.Register(context.Background(), target)

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:  proto.CmdRecall,
		From: "evil_spoofer", // client tries to spoof
		To:   "bob",
		Seq:  7777,
	})

	delivered := readMessageFromChan(t, target.send)
	if delivered.From != "alice" {
		t.Errorf("expected From to be overwritten to 'alice', got '%s'", delivered.From)
	}

	// The store should see the real sender UID.
	if store.recallFromUID != "alice" {
		t.Errorf("expected recallFromUID=alice, got %s", store.recallFromUID)
	}
}

// TestRecallCrossGateway verifies that recall notifications are forwarded to
// peer gateways when the target is owned by another node.
func TestRecallCrossGateway(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	store := &testMsgStoreForRecall{}
	r := NewRouter(h, h, sg, store, DefaultRouterConfig())

	// Ring has only "gw-2" — all targets forward to gw-2.
	hr := makeHashRing("gw-2")
	r.SetHashRing(hr)
	r.SetThisNodeID("gw-1")

	fw := &mockForwarder{delivered: true}
	r.SetForwarder(fw)

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:  proto.CmdRecall,
		From: "alice",
		To:   "bob",
		Seq:  5555,
	})

	if len(fw.forwarded) != 1 {
		t.Fatalf("expected 1 forwarded call, got %d", len(fw.forwarded))
	}
	if fw.forwarded[0].UID != "bob" {
		t.Errorf("forwarded to wrong UID: %s", fw.forwarded[0].UID)
	}
	if fw.forwarded[0].Msg.Cmd != proto.CmdRecall {
		t.Errorf("expected CmdRecall forwarded, got cmd=%d", fw.forwarded[0].Msg.Cmd)
	}

	assertChanEmpty(t, sender.send)
}

// TestRecallCrossGatewayForwardFail verifies that when cross-gateway
// forwarding fails, the recall falls back to local offline storage.
func TestRecallCrossGatewayForwardFail(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	store := &testMsgStoreForRecall{}
	r := NewRouter(h, h, sg, store, DefaultRouterConfig())

	hr := makeHashRing("gw-2")
	r.SetHashRing(hr)
	r.SetThisNodeID("gw-1")

	fw := &mockForwarder{err: fmt.Errorf("connection refused")}
	r.SetForwarder(fw)

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:  proto.CmdRecall,
		From: "alice",
		To:   "bob",
		Seq:  4444,
	})

	// Forwarder was attempted.
	if len(fw.forwarded) != 1 {
		t.Fatalf("expected 1 forward attempt, got %d", len(fw.forwarded))
	}

	// Fallback: recall stored offline locally.
	offlineMsgs := h.DrainOffline(context.Background(), "bob")
	if len(offlineMsgs) != 1 {
		t.Fatalf("expected 1 offline recall after forward fail, got %d", len(offlineMsgs))
	}
	if offlineMsgs[0].Cmd != proto.CmdRecall {
		t.Errorf("expected CmdRecall in offline, got cmd=%d", offlineMsgs[0].Cmd)
	}
}

// TestRecallNoAck verifies that even when NeedAck is set, no ACK is
// generated for recall — the notification to the peer is sufficient.
func TestRecallNoAck(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	store := &testMsgStoreForRecall{}
	r := NewRouter(h, h, sg, store, DefaultRouterConfig())

	target := newTestClient(t, "bob", "Bob")
	h.Register(context.Background(), target)

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:     proto.CmdRecall,
		From:    "alice",
		To:      "bob",
		Seq:     1111,
		NeedAck: true,
	})

	// Target receives the recall notification.
	_ = readMessageFromChan(t, target.send)

	// Sender must NOT receive an ACK.
	assertChanEmpty(t, sender.send)
}

// TestRecallMessageStoreError verifies that when RecallMessage fails in the
// store, the sender gets an error response.
func TestRecallMessageStoreError(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	store := &testMsgStoreForRecall{
		recallErr: fmt.Errorf("message not found"),
	}
	r := NewRouter(h, h, sg, store, DefaultRouterConfig())

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:  proto.CmdRecall,
		From: "alice",
		To:   "bob",
		Seq:  9999,
	})

	// Sender should get error response.
	errMsg := readMessageFromChan(t, sender.send)
	if errMsg.Cmd != proto.CmdRecall {
		t.Errorf("expected CmdRecall error, got cmd=%d", errMsg.Cmd)
	}
	if errMsg.Content != `{"error":"message not found"}` {
		t.Errorf("expected error content, got %s", errMsg.Content)
	}
}

// TestRecallValidateRejectsBadCmd verifies that Validate() rejects commands
// above CmdRecall.
func TestRecallValidateRejectsBadCmd(t *testing.T) {
	// Valid: CmdRecall = 19
	msg := &proto.Message{Cmd: proto.CmdRecall, To: "bob"}
	if err := msg.Validate(); err != nil {
		t.Errorf("CmdRecall with To should be valid, got: %v", err)
	}

	// Invalid: CmdRecall without To
	msgNoTo := &proto.Message{Cmd: proto.CmdRecall, To: ""}
	if err := msgNoTo.Validate(); err == nil {
		t.Error("CmdRecall without To should fail validation")
	}

	// Invalid: value above max valid Cmd (CmdEdit=24 is now valid)
	msgBad := &proto.Message{Cmd: 25, To: "bob"}
	if err := msgBad.Validate(); err == nil {
		t.Error("cmd=25 should fail validation")
	}
}
