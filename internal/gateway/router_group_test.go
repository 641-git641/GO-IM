package gateway

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/im/api/proto"
	"github.com/im/internal/pkg/snowflake"
)

// ---------- Group Management Protocol Handlers ----------

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

	// Should receive a response with group details.
	resp := readMessageFromChan(t, sender.send)
	if resp.Cmd != proto.CmdGroupCreate {
		t.Fatalf("expected CmdGroupCreate response, got cmd=%d", resp.Cmd)
	}

	// Parse the response content.
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

	// Verify the group exists in the store.
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

	// Router WITHOUT GroupStore set.
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

	// Register alice as online so she receives the notification.
	alice := newTestClient(t, "alice", "Alice")
	h.Register(ctx, alice)

	// Bob joins the group.
	bob := newTestClient(t, "bob", "Bob")
	r.Route(ctx, bob, &proto.Message{
		Cmd:  proto.CmdGroupJoin,
		From: "bob",
		To:   g.ID,
	})

	// Bob should receive a response.
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

	// Verify bob is now a member.
	if !gs.IsMember(ctx, g.ID, "bob") {
		t.Error("bob should be a member")
	}

	// Alice (existing member) should receive a notification.
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
		To:   "", // empty group_id
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

	// Register alice so she receives notification when bob leaves.
	alice := newTestClient(t, "alice", "Alice")
	h.Register(ctx, alice)

	// Bob leaves the group.
	bob := newTestClient(t, "bob", "Bob")
	r.Route(ctx, bob, &proto.Message{
		Cmd:  proto.CmdGroupLeave,
		From: "bob",
		To:   g.ID,
	})

	// Bob should receive a response.
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

	// Bob should no longer be a member.
	if gs.IsMember(ctx, g.ID, "bob") {
		t.Error("bob should have been removed from group")
	}

	// Alice (remaining member) should receive a notification.
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

	// Alice (the only member) leaves.
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
	// Group should be deleted.
	if result["deleted"] != true {
		t.Errorf("expected deleted=true for last member leave, got %v", result["deleted"])
	}

	// Verify group no longer exists.
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

	// Bob tries to leave a group he's not in.
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
	gs.AddMember(ctx, g2.ID, "alice") // alice is in both groups

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

	// Check both groups are present.
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

	// Router WITHOUT GroupStore set.
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

	// Register bob as online.
	bob := newTestClient(t, "bob", "Bob")
	h.Register(ctx, bob)

	// Send notification via sendGroupNotification (testing member_joined type).
	r.sendGroupNotification(ctx, "carol", g.ID, "member_joined")

	// Bob should receive the notification.
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

	// alice (sender) is not online — notification for her is stored offline.
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

	// Register alice as online.
	alice := newTestClient(t, "alice", "Alice")
	h.Register(ctx, alice)

	// Send member_left notification.
	r.sendGroupNotification(ctx, "bob", g.ID, "member_left")

	// Alice (online) should receive the notification.
	notif := readMessageFromChan(t, alice.send)

	var notifContent map[string]string
	if err := json.Unmarshal([]byte(notif.Content), &notifContent); err != nil {
		t.Fatalf("unmarshal notification: %v", err)
	}
	if notifContent["type"] != "member_left" {
		t.Errorf("expected type='member_left', got %q", notifContent["type"])
	}
}
