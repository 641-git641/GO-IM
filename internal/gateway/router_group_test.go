package gateway

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/im/api/proto"
	"github.com/im/internal/pkg/snowflake"
)

// ---------- 群组管理协议处理器 ----------

func TestHandleGroupCreate(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	gs := NewInMemoryGroupStore(sg)

	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	r.SetGroupStore(gs)

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:     proto.CmdGroupCreate,
		From:    "alice",
		Content: `{"name":"My Group"}`,
	})

	// 应收到包含群组详情的响应。
	resp := readMessageFromChan(t, sender.send)
	if resp.Cmd != proto.CmdGroupCreate {
		t.Fatalf("expected CmdGroupCreate response, got cmd=%d", resp.Cmd)
	}

	// 解析响应内容。
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(resp.Content), &result); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if result["name"] != "My Group" {
		t.Errorf("expected name='My Group', got %v", result["name"])
	}
	if result["owner_uid"] != "alice" {
		t.Errorf("expected owner_uid='alice', got %v", result["owner_uid"])
	}
	if result["id"] == nil || result["id"] == "" {
		t.Error("expected non-empty group id")
	}
	members := result["members"].([]interface{})
	if len(members) != 1 || members[0] != "alice" {
		t.Errorf("expected members=[alice], got %v", members)
	}

	// 验证群组存在于存储中。
	g, err := gs.Get(context.Background(), result["id"].(string))
	if err != nil {
		t.Fatalf("group not found in store: %v", err)
	}
	if g.Name != "My Group" {
		t.Errorf("stored group name mismatch: %s", g.Name)
	}
}

func TestHandleGroupCreateNoName(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	gs := NewInMemoryGroupStore(sg)

	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	r.SetGroupStore(gs)

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:     proto.CmdGroupCreate,
		From:    "alice",
		Content: `{"name":""}`,
	})

	resp := readMessageFromChan(t, sender.send)
	if resp.Cmd != proto.CmdGroupCreate {
		t.Fatalf("expected CmdGroupCreate response, got cmd=%d", resp.Cmd)
	}
	var result map[string]string
	if err := json.Unmarshal([]byte(resp.Content), &result); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if result["error"] == "" {
		t.Error("expected error for empty name")
	}
}

func TestHandleGroupCreateNoGroupStore(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)

	// 未设置 GroupStore 的 Router。
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:     proto.CmdGroupCreate,
		From:    "alice",
		Content: `{"name":"Test"}`,
	})

	resp := readMessageFromChan(t, sender.send)
	var result map[string]string
	if err := json.Unmarshal([]byte(resp.Content), &result); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if result["error"] != "group chat not enabled" {
		t.Errorf("expected 'group chat not enabled' error, got %q", result["error"])
	}
}

func TestHandleGroupJoin(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	gs := NewInMemoryGroupStore(sg)

	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	r.SetGroupStore(gs)

	ctx := context.Background()
	g, _ := gs.Create(ctx, "Dev Team", "alice", nil)

	// 将 alice 注册为在线，以便她收到通知。
	alice := newTestClient(t, "alice", "Alice")
	h.Register(ctx, alice)

	// Bob 加入群组。
	bob := newTestClient(t, "bob", "Bob")
	r.Route(ctx, bob, &proto.Message{
		Cmd:  proto.CmdGroupJoin,
		From: "bob",
		To:   g.ID,
	})

	// Bob 应收到响应。
	resp := readMessageFromChan(t, bob.send)
	if resp.Cmd != proto.CmdGroupJoin {
		t.Fatalf("expected CmdGroupJoin response, got cmd=%d", resp.Cmd)
	}
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(resp.Content), &result); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if result["group_id"] != g.ID {
		t.Errorf("expected group_id=%s, got %v", g.ID, result["group_id"])
	}
	members := result["members"].([]interface{})
	if len(members) != 2 {
		t.Errorf("expected 2 members, got %d", len(members))
	}

	// 验证 bob 现在已是成员。
	if !gs.IsMember(ctx, g.ID, "bob") {
		t.Error("bob should be a member")
	}

	// Alice（现有成员）应收到通知。
	notif := readMessageFromChan(t, alice.send)
	if notif.ChatType != proto.ChatTypeGroup {
		t.Errorf("expected group chat notification, got chat_type=%d", notif.ChatType)
	}
}

func TestHandleGroupJoinNoGroupID(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	gs := NewInMemoryGroupStore(sg)

	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	r.SetGroupStore(gs)

	sender := newTestClient(t, "bob", "Bob")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:  proto.CmdGroupJoin,
		From: "bob",
		To:   "", // 空的 group_id
	})

	resp := readMessageFromChan(t, sender.send)
	var result map[string]string
	if err := json.Unmarshal([]byte(resp.Content), &result); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if result["error"] == "" {
		t.Error("expected error for empty group_id")
	}
}

func TestHandleGroupJoinGroupNotFound(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	gs := NewInMemoryGroupStore(sg)

	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	r.SetGroupStore(gs)

	sender := newTestClient(t, "bob", "Bob")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:  proto.CmdGroupJoin,
		From: "bob",
		To:   "g_nonexistent",
	})

	resp := readMessageFromChan(t, sender.send)
	var result map[string]string
	if err := json.Unmarshal([]byte(resp.Content), &result); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if result["error"] == "" {
		t.Error("expected error for nonexistent group")
	}
}

func TestHandleGroupJoinAlreadyMember(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	gs := NewInMemoryGroupStore(sg)

	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	r.SetGroupStore(gs)

	ctx := context.Background()
	g, _ := gs.Create(ctx, "Dev Team", "alice", nil)

	sender := newTestClient(t, "alice", "Alice")
	r.Route(ctx, sender, &proto.Message{
		Cmd:  proto.CmdGroupJoin,
		From: "alice",
		To:   g.ID,
	})

	resp := readMessageFromChan(t, sender.send)
	var result map[string]string
	if err := json.Unmarshal([]byte(resp.Content), &result); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if result["error"] == "" {
		t.Error("expected error for already a member")
	}
}

func TestHandleGroupLeave(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	gs := NewInMemoryGroupStore(sg)

	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	r.SetGroupStore(gs)

	ctx := context.Background()
	g, _ := gs.Create(ctx, "Dev Team", "alice", nil)
	gs.AddMember(ctx, g.ID, "bob")

	// 注册 alice，以便她在 bob 退出时收到通知。
	alice := newTestClient(t, "alice", "Alice")
	h.Register(ctx, alice)

	// Bob 退出群组。
	bob := newTestClient(t, "bob", "Bob")
	r.Route(ctx, bob, &proto.Message{
		Cmd:  proto.CmdGroupLeave,
		From: "bob",
		To:   g.ID,
	})

	// Bob 应收到响应。
	resp := readMessageFromChan(t, bob.send)
	if resp.Cmd != proto.CmdGroupLeave {
		t.Fatalf("expected CmdGroupLeave response, got cmd=%d", resp.Cmd)
	}
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(resp.Content), &result); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if result["group_id"] != g.ID {
		t.Errorf("expected group_id=%s, got %v", g.ID, result["group_id"])
	}

	// Bob 不应再是成员。
	if gs.IsMember(ctx, g.ID, "bob") {
		t.Error("bob should have been removed from group")
	}

	// Alice（剩余成员）应收到通知。
	notif := readMessageFromChan(t, alice.send)
	if notif.ChatType != proto.ChatTypeGroup {
		t.Errorf("expected group notification, got chat_type=%d", notif.ChatType)
	}
}

func TestHandleGroupLeaveLastMemberDeletes(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	gs := NewInMemoryGroupStore(sg)

	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	r.SetGroupStore(gs)

	ctx := context.Background()
	g, _ := gs.Create(ctx, "Solo Group", "alice", nil)

	// Alice（唯一成员）退出。
	alice := newTestClient(t, "alice", "Alice")
	r.Route(ctx, alice, &proto.Message{
		Cmd:  proto.CmdGroupLeave,
		From: "alice",
		To:   g.ID,
	})

	resp := readMessageFromChan(t, alice.send)
	if resp.Cmd != proto.CmdGroupLeave {
		t.Fatalf("expected CmdGroupLeave response, got cmd=%d", resp.Cmd)
	}
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(resp.Content), &result); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	// 群组应被删除。
	if result["deleted"] != true {
		t.Errorf("expected deleted=true for last member leave, got %v", result["deleted"])
	}

	// 验证群组不再存在。
	_, err := gs.Get(ctx, g.ID)
	if err == nil {
		t.Error("group should have been deleted")
	}
}

func TestHandleGroupLeaveNoGroupID(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	gs := NewInMemoryGroupStore(sg)

	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	r.SetGroupStore(gs)

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:  proto.CmdGroupLeave,
		From: "alice",
		To:   "",
	})

	resp := readMessageFromChan(t, sender.send)
	var result map[string]string
	if err := json.Unmarshal([]byte(resp.Content), &result); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if result["error"] == "" {
		t.Error("expected error for empty group_id")
	}
}

func TestHandleGroupLeaveNotMember(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	gs := NewInMemoryGroupStore(sg)

	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	r.SetGroupStore(gs)

	ctx := context.Background()
	g, _ := gs.Create(ctx, "Dev Team", "alice", nil)

	// Bob 试图退出一个他不在的群组。
	bob := newTestClient(t, "bob", "Bob")
	r.Route(ctx, bob, &proto.Message{
		Cmd:  proto.CmdGroupLeave,
		From: "bob",
		To:   g.ID,
	})

	resp := readMessageFromChan(t, bob.send)
	var result map[string]string
	if err := json.Unmarshal([]byte(resp.Content), &result); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if result["error"] == "" {
		t.Error("expected error for non-member leaving")
	}
}

func TestHandleGroupInfo(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	gs := NewInMemoryGroupStore(sg)

	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	r.SetGroupStore(gs)

	ctx := context.Background()
	g, _ := gs.Create(ctx, "Dev Team", "alice", nil)
	gs.AddMember(ctx, g.ID, "bob")

	sender := newTestClient(t, "alice", "Alice")
	r.Route(ctx, sender, &proto.Message{
		Cmd:  proto.CmdGroupInfo,
		From: "alice",
		To:   g.ID,
	})

	resp := readMessageFromChan(t, sender.send)
	if resp.Cmd != proto.CmdGroupInfo {
		t.Fatalf("expected CmdGroupInfo response, got cmd=%d", resp.Cmd)
	}
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(resp.Content), &result); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if result["id"] != g.ID {
		t.Errorf("expected id=%s, got %v", g.ID, result["id"])
	}
	if result["name"] != "Dev Team" {
		t.Errorf("expected name='Dev Team', got %v", result["name"])
	}
	if result["owner_uid"] != "alice" {
		t.Errorf("expected owner_uid='alice', got %v", result["owner_uid"])
	}
	members := result["members"].([]interface{})
	if len(members) != 2 {
		t.Errorf("expected 2 members, got %d", len(members))
	}
}

func TestHandleGroupInfoNoGroupID(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	gs := NewInMemoryGroupStore(sg)

	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	r.SetGroupStore(gs)

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:  proto.CmdGroupInfo,
		From: "alice",
		To:   "",
	})

	resp := readMessageFromChan(t, sender.send)
	var result map[string]string
	if err := json.Unmarshal([]byte(resp.Content), &result); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if result["error"] == "" {
		t.Error("expected error for empty group_id")
	}
}

func TestHandleGroupInfoNotFound(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	gs := NewInMemoryGroupStore(sg)

	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	r.SetGroupStore(gs)

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:  proto.CmdGroupInfo,
		From: "alice",
		To:   "g_nonexistent",
	})

	resp := readMessageFromChan(t, sender.send)
	var result map[string]string
	if err := json.Unmarshal([]byte(resp.Content), &result); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if result["error"] == "" {
		t.Error("expected error for nonexistent group")
	}
}

func TestHandleGroupList(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	gs := NewInMemoryGroupStore(sg)

	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	r.SetGroupStore(gs)

	ctx := context.Background()
	g1, _ := gs.Create(ctx, "Group A", "alice", nil)
	g2, _ := gs.Create(ctx, "Group B", "bob", nil)
	gs.AddMember(ctx, g2.ID, "alice") // alice 在两个群组中

	sender := newTestClient(t, "alice", "Alice")
	r.Route(ctx, sender, &proto.Message{
		Cmd:  proto.CmdGroupList,
		From: "alice",
	})

	resp := readMessageFromChan(t, sender.send)
	if resp.Cmd != proto.CmdGroupList {
		t.Fatalf("expected CmdGroupList response, got cmd=%d", resp.Cmd)
	}
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(resp.Content), &result); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if result["uid"] != "alice" {
		t.Errorf("expected uid='alice', got %v", result["uid"])
	}

	groups := result["groups"].([]interface{})
	if len(groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(groups))
	}

	// 检查两个群组都存在。
	ids := make(map[string]bool)
	for _, gr := range groups {
		gm := gr.(map[string]interface{})
		ids[gm["id"].(string)] = true
	}
	if !ids[g1.ID] || !ids[g2.ID] {
		t.Errorf("expected both groups in list, got ids=%v", ids)
	}
}

func TestHandleGroupListEmpty(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	gs := NewInMemoryGroupStore(sg)

	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	r.SetGroupStore(gs)

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:  proto.CmdGroupList,
		From: "alice",
	})

	resp := readMessageFromChan(t, sender.send)
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(resp.Content), &result); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	groups := result["groups"].([]interface{})
	if len(groups) != 0 {
		t.Errorf("expected 0 groups for new user, got %d", len(groups))
	}
}

func TestHandleGroupListNoGroupStore(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)

	// 未设置 GroupStore 的 Router。
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())

	sender := newTestClient(t, "alice", "Alice")
	r.Route(context.Background(), sender, &proto.Message{
		Cmd:  proto.CmdGroupList,
		From: "alice",
	})

	resp := readMessageFromChan(t, sender.send)
	var result map[string]string
	if err := json.Unmarshal([]byte(resp.Content), &result); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if result["error"] != "group chat not enabled" {
		t.Errorf("expected 'group chat not enabled' error, got %q", result["error"])
	}
}

func TestSendGroupNotification(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	gs := NewInMemoryGroupStore(sg)

	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	r.SetGroupStore(gs)

	ctx := context.Background()
	g, _ := gs.Create(ctx, "Dev Team", "alice", nil)
	gs.AddMember(ctx, g.ID, "bob")

	// 将 bob 注册为在线。
	bob := newTestClient(t, "bob", "Bob")
	h.Register(ctx, bob)

	// 通过 sendGroupNotification 发送通知（测试 member_joined 类型）。
	r.sendGroupNotification(ctx, "carol", g.ID, "member_joined")

	// Bob 应收到通知。
	notif := readMessageFromChan(t, bob.send)
	if notif.Cmd != proto.CmdChat {
		t.Errorf("expected CmdChat for notification, got cmd=%d", notif.Cmd)
	}
	if notif.ChatType != proto.ChatTypeGroup {
		t.Errorf("expected ChatTypeGroup, got %d", notif.ChatType)
	}
	if notif.To != g.ID {
		t.Errorf("expected To=%s, got %s", g.ID, notif.To)
	}

	var notifContent map[string]string
	if err := json.Unmarshal([]byte(notif.Content), &notifContent); err != nil {
		t.Fatalf("unmarshal notification: %v", err)
	}
	if notifContent["type"] != "member_joined" {
		t.Errorf("expected type='member_joined', got %q", notifContent["type"])
	}
	if notifContent["group_id"] != g.ID {
		t.Errorf("expected group_id=%s, got %q", g.ID, notifContent["group_id"])
	}
	if notifContent["uid"] != "carol" {
		t.Errorf("expected uid='carol', got %q", notifContent["uid"])
	}

	// alice（发送者）不在线 —— 给她的通知会存储在离线队列中。
	offlineMsgs := h.DrainOffline(ctx, "alice")
	if len(offlineMsgs) != 1 {
		t.Errorf("expected 1 offline notification for alice, got %d", len(offlineMsgs))
	}
}

func TestSendGroupNotificationMemberLeft(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	gs := NewInMemoryGroupStore(sg)

	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	r.SetGroupStore(gs)

	ctx := context.Background()
	g, _ := gs.Create(ctx, "Dev Team", "alice", nil)
	gs.AddMember(ctx, g.ID, "bob")

	// 将 alice 注册为在线。
	alice := newTestClient(t, "alice", "Alice")
	h.Register(ctx, alice)

	// 发送 member_left 通知。
	r.sendGroupNotification(ctx, "bob", g.ID, "member_left")

	// Alice（在线）应收到通知。
	notif := readMessageFromChan(t, alice.send)

	var notifContent map[string]string
	if err := json.Unmarshal([]byte(notif.Content), &notifContent); err != nil {
		t.Fatalf("unmarshal notification: %v", err)
	}
	if notifContent["type"] != "member_left" {
		t.Errorf("expected type='member_left', got %q", notifContent["type"])
	}
}
