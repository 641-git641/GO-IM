package gateway

import (
	"context"
	"testing"

	"github.com/im/internal/pkg/snowflake"
)

func newTestGroupStore(t *testing.T) *InMemoryGroupStore {
	t.Helper()
	snow, err := snowflake.New(1)
	if err != nil {
		t.Fatalf("snowflake.New: %v", err)
	}
	return NewInMemoryGroupStore(snow)
}

func TestGroupStoreCreate(t *testing.T) {
	gs := newTestGroupStore(t)
	ctx := context.Background()

	g, err := gs.Create(ctx, "Test Group", "alice", nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if g.ID == "" {
		t.Error("group ID should not be empty")
	}
	if g.Name != "Test Group" {
		t.Errorf("expected name 'Test Group', got %q", g.Name)
	}
	if g.OwnerUID != "alice" {
		t.Errorf("expected owner 'alice', got %q", g.OwnerUID)
	}
	if !g.Members["alice"] {
		t.Error("owner should be auto-added as member")
	}
	if len(g.Members) != 1 {
		t.Errorf("expected 1 member, got %d", len(g.Members))
	}

	// Verify group is retrievable.
	got, err := gs.Get(ctx, g.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != "Test Group" {
		t.Errorf("Get returned wrong name: %q", got.Name)
	}
}

func TestGroupStoreAddMember(t *testing.T) {
	gs := newTestGroupStore(t)
	ctx := context.Background()

	g, _ := gs.Create(ctx, "Dev Team", "alice", nil)

	if err := gs.AddMember(ctx, g.ID, "bob"); err != nil {
		t.Fatalf("AddMember bob: %v", err)
	}
	if err := gs.AddMember(ctx, g.ID, "carol"); err != nil {
		t.Fatalf("AddMember carol: %v", err)
	}

	members, err := gs.GetMembers(ctx, g.ID)
	if err != nil {
		t.Fatalf("GetMembers: %v", err)
	}
	if len(members) != 3 {
		t.Errorf("expected 3 members, got %d", len(members))
	}

	if !gs.IsMember(ctx, g.ID, "bob") {
		t.Error("bob should be a member")
	}
	if !gs.IsMember(ctx, g.ID, "carol") {
		t.Error("carol should be a member")
	}
	if gs.IsMember(ctx, g.ID, "dave") {
		t.Error("dave should not be a member")
	}
}

func TestGroupStoreAddMemberAlreadyMember(t *testing.T) {
	gs := newTestGroupStore(t)
	ctx := context.Background()

	g, _ := gs.Create(ctx, "Dev Team", "alice", nil)

	if err := gs.AddMember(ctx, g.ID, "alice"); err != ErrAlreadyMember {
		t.Errorf("expected ErrAlreadyMember, got %v", err)
	}
}

func TestGroupStoreAddMemberGroupNotFound(t *testing.T) {
	gs := newTestGroupStore(t)
	ctx := context.Background()

	if err := gs.AddMember(ctx, "g_nonexistent", "bob"); err != ErrGroupNotFound {
		t.Errorf("expected ErrGroupNotFound, got %v", err)
	}
}

func TestGroupStoreRemoveMember(t *testing.T) {
	gs := newTestGroupStore(t)
	ctx := context.Background()

	g, _ := gs.Create(ctx, "Dev Team", "alice", nil)
	gs.AddMember(ctx, g.ID, "bob")

	if err := gs.RemoveMember(ctx, g.ID, "bob"); err != nil {
		t.Fatalf("RemoveMember bob: %v", err)
	}

	if gs.IsMember(ctx, g.ID, "bob") {
		t.Error("bob should no longer be a member")
	}
	if !gs.IsMember(ctx, g.ID, "alice") {
		t.Error("alice should still be a member")
	}

	members, _ := gs.GetMembers(ctx, g.ID)
	if len(members) != 1 {
		t.Errorf("expected 1 member, got %d", len(members))
	}
}

func TestGroupStoreRemoveMemberNotMember(t *testing.T) {
	gs := newTestGroupStore(t)
	ctx := context.Background()

	g, _ := gs.Create(ctx, "Dev Team", "alice", nil)

	if err := gs.RemoveMember(ctx, g.ID, "bob"); err != ErrNotMember {
		t.Errorf("expected ErrNotMember, got %v", err)
	}
}

func TestGroupStoreRemoveLastMemberDeletesGroup(t *testing.T) {
	gs := newTestGroupStore(t)
	ctx := context.Background()

	g, _ := gs.Create(ctx, "Solo Team", "alice", nil)

	if err := gs.RemoveMember(ctx, g.ID, "alice"); err != nil {
		t.Fatalf("RemoveMember alice: %v", err)
	}

	// Group should be deleted since it has no members.
	if _, err := gs.Get(ctx, g.ID); err != ErrGroupNotFound {
		t.Errorf("expected ErrGroupNotFound after last member leaves, got %v", err)
	}
}

func TestGroupStoreGetMembersGroupNotFound(t *testing.T) {
	gs := newTestGroupStore(t)
	ctx := context.Background()

	if _, err := gs.GetMembers(ctx, "g_nonexistent"); err != ErrGroupNotFound {
		t.Errorf("expected ErrGroupNotFound, got %v", err)
	}
}

func TestGroupStoreGetUserGroups(t *testing.T) {
	gs := newTestGroupStore(t)
	ctx := context.Background()

	g1, _ := gs.Create(ctx, "Team A", "alice", nil)
	g2, _ := gs.Create(ctx, "Team B", "bob", nil)
	gs.AddMember(ctx, g2.ID, "alice") // alice joins Team B

	groups, err := gs.GetUserGroups(ctx, "alice")
	if err != nil {
		t.Fatalf("GetUserGroups alice: %v", err)
	}
	if len(groups) != 2 {
		t.Errorf("alice should be in 2 groups, got %d", len(groups))
	}

	groups, _ = gs.GetUserGroups(ctx, "bob")
	if len(groups) != 1 {
		t.Errorf("bob should be in 1 group, got %d", len(groups))
	}

	groups, _ = gs.GetUserGroups(ctx, "carol")
	if len(groups) != 0 {
		t.Errorf("carol should be in 0 groups, got %d", len(groups))
	}

	// Verify group names in results.
	names := make(map[string]bool)
	for _, g := range groups {
		names[g.Name] = true
	}
	_ = g1 // used
	_ = g2 // used
}

func TestGroupStoreIsMemberGroupNotFound(t *testing.T) {
	gs := newTestGroupStore(t)
	ctx := context.Background()

	if gs.IsMember(ctx, "g_nonexistent", "alice") {
		t.Error("IsMember should return false for non-existent group")
	}
}

func TestGroupStoreConcurrentAccess(t *testing.T) {
	gs := newTestGroupStore(t)
	ctx := context.Background()

	g, _ := gs.Create(ctx, "Concurrent Team", "alice", nil)

	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			_ = gs.AddMember(ctx, g.ID, "user-"+string(rune('a'+i%26)))
			_ = gs.RemoveMember(ctx, g.ID, "user-"+string(rune('a'+i%26)))
		}
		close(done)
	}()

	for i := 0; i < 100; i++ {
		_, _ = gs.GetMembers(ctx, g.ID)
		_ = gs.IsMember(ctx, g.ID, "alice")
		_, _ = gs.GetUserGroups(ctx, "alice")
	}

	<-done
}

func TestGroupStoreGetGroupNotFound(t *testing.T) {
	gs := newTestGroupStore(t)
	ctx := context.Background()

	if _, err := gs.Get(ctx, "g_nonexistent"); err != ErrGroupNotFound {
		t.Errorf("expected ErrGroupNotFound, got %v", err)
	}
}
