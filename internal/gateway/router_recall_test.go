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
// CmdRecall —— 消息撤回测试
// ---------------------------------------------------------------------------

// testMsgStoreForRecall 是一个 MessageStore，记录撤回调用并
// 存储消息用于时间戳验证。
type testMsgStoreForRecall struct {
	msgs          []*proto.Message
	recalled      []int64 // 被撤回的 msgID 列表
	recallErr     error   // 从 RecallMessage 返回该错误
	recallFromUID string  // 记录传给 RecallMessage 的 fromUID
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

// TestRecallOnline 验证撤回通知会投递给在线目标对端，
// 且字段正确。
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
		Seq:  12345678, // 要撤回的原始 MsgID
	})

	// 目标应收到撤回通知。
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

	// 存储上的 RecallMessage 已被调用。
	if len(store.recalled) != 1 {
		t.Errorf("expected 1 recall call, got %d", len(store.recalled))
	}
	if store.recalled[0] != 12345678 {
		t.Errorf("expected recalled msgID=12345678, got %d", store.recalled[0])
	}
	if store.recallFromUID != "alice" {
		t.Errorf("expected recallFromUID=alice, got %s", store.recallFromUID)
	}

	// 发送者不应收到 ACK。
	assertChanEmpty(t, sender.send)
}

// TestRecallOffline 验证针对离线目标的撤回通知
// 会存储在离线队列中。
func TestRecallOffline(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	store := &testMsgStoreForRecall{}
	r := NewRouter(h, h, sg, store, DefaultRouterConfig())

	// Bob 未注册（离线）。
	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:  proto.CmdRecall,
		From: "alice",
		To:   "bob",
		Seq:  999888777,
	})

	// 撤回通知应存储在离线队列中。
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

// TestRecallMissingTarget 验证缺少目标的撤回会被拒绝。
func TestRecallMissingTarget(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	store := &testMsgStoreForRecall{}
	r := NewRouter(h, h, sg, store, DefaultRouterConfig())

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:  proto.CmdRecall,
		From: "alice",
		To:   "", // 缺少目标
		Seq:  123,
	})

	// 不应投递任何内容，也不应调用撤回。
	assertChanEmpty(t, sender.send)
	if len(store.recalled) != 0 {
		t.Errorf("expected 0 recall calls, got %d", len(store.recalled))
	}
}

// TestRecallSelfTarget 验证自我撤回会被拒绝。
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
		To:   "alice", // 自我目标
		Seq:  123,
	})

	assertChanEmpty(t, sender.send)
	if len(store.recalled) != 0 {
		t.Errorf("expected 0 recall calls, got %d", len(store.recalled))
	}
}

// TestRecallWithoutSeq 验证缺少 Seq（原始 MsgID）的撤回会被拒绝。
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
		Seq:  0, // 缺少原始 MsgID
	})

	// 发送者应收到错误响应。
	errMsg := readMessageFromChan(t, sender.send)
	if errMsg.Cmd != proto.CmdRecall {
		t.Errorf("expected CmdRecall error, got cmd=%d", errMsg.Cmd)
	}
	if errMsg.Content != `{"error":"missing original message ID"}` {
		t.Errorf("expected error content, got %s", errMsg.Content)
	}

	// 目标不应收到任何内容。
	assertChanEmpty(t, target.send)
	if len(store.recalled) != 0 {
		t.Errorf("expected 0 recall calls, got %d", len(store.recalled))
	}
}

// TestRecallFromOverwrite 验证已认证的发送者 UID 会覆盖
// 客户端在 msg.From 中发送的任何内容。
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
		From: "evil_spoofer", // 客户端试图伪造
		To:   "bob",
		Seq:  7777,
	})

	delivered := readMessageFromChan(t, target.send)
	if delivered.From != "alice" {
		t.Errorf("expected From to be overwritten to 'alice', got '%s'", delivered.From)
	}

	// 存储应看到真实的发送者 UID。
	if store.recallFromUID != "alice" {
		t.Errorf("expected recallFromUID=alice, got %s", store.recallFromUID)
	}
}

// TestRecallCrossGateway 验证当目标归其他节点所有时，
// 撤回通知会转发给对端网关。
func TestRecallCrossGateway(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	store := &testMsgStoreForRecall{}
	r := NewRouter(h, h, sg, store, DefaultRouterConfig())

	// 哈希环只有 "gw-2" —— 所有目标转发到 gw-2。
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

// TestRecallCrossGatewayForwardFail 验证跨网关转发失败时，
// 撤回会回退到本地离线存储。
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

	// 已尝试转发。
	if len(fw.forwarded) != 1 {
		t.Fatalf("expected 1 forward attempt, got %d", len(fw.forwarded))
	}

	// 回退：撤回存储在本地离线队列中。
	offlineMsgs := h.DrainOffline(context.Background(), "bob")
	if len(offlineMsgs) != 1 {
		t.Fatalf("expected 1 offline recall after forward fail, got %d", len(offlineMsgs))
	}
	if offlineMsgs[0].Cmd != proto.CmdRecall {
		t.Errorf("expected CmdRecall in offline, got cmd=%d", offlineMsgs[0].Cmd)
	}
}

// TestRecallNoAck 验证即使设置了 NeedAck，撤回也不会生成 ACK ——
// 向对端发送通知已足够。
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

	// 目标收到撤回通知。
	_ = readMessageFromChan(t, target.send)

	// 发送者绝不应收到 ACK。
	assertChanEmpty(t, sender.send)
}

// TestRecallMessageStoreError 验证当存储中的 RecallMessage 失败时，
// 发送者会收到错误响应。
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

	// 发送者应收到错误响应。
	errMsg := readMessageFromChan(t, sender.send)
	if errMsg.Cmd != proto.CmdRecall {
		t.Errorf("expected CmdRecall error, got cmd=%d", errMsg.Cmd)
	}
	if errMsg.Content != `{"error":"message not found"}` {
		t.Errorf("expected error content, got %s", errMsg.Content)
	}
}

// TestRecallValidateRejectsBadCmd 验证 Validate() 会拒绝
// 大于 CmdRecall 的命令。
func TestRecallValidateRejectsBadCmd(t *testing.T) {
	// 有效：CmdRecall = 19
	msg := &proto.Message{Cmd: proto.CmdRecall, To: "bob"}
	if err := msg.Validate(); err != nil {
		t.Errorf("CmdRecall with To should be valid, got: %v", err)
	}

	// 无效：CmdRecall 缺少 To
	msgNoTo := &proto.Message{Cmd: proto.CmdRecall, To: ""}
	if err := msgNoTo.Validate(); err == nil {
		t.Error("CmdRecall without To should fail validation")
	}

	// 无效：超过最大有效 Cmd 的值（CmdEdit=24 现在是有效的）
	msgBad := &proto.Message{Cmd: 25, To: "bob"}
	if err := msgBad.Validate(); err == nil {
		t.Error("cmd=25 should fail validation")
	}
}
