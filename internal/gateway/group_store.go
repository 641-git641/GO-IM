package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/im/internal/pkg/snowflake"
)

// Group represents a chat group with its members.
type Group struct {
	ID        string          `json:"id"`         // e.g. "g_123456"
	Name      string          `json:"name"`       // human-readable group name
	OwnerUID  string          `json:"owner_uid"`  // creator/owner
	Members   map[string]bool `json:"members"`    // uid → true (includes owner)
	CreatedAt int64           `json:"created_at"` // unix millis
}

// GroupStore manages group persistence. InMemoryGroupStore is the default
// implementation; MySQLGroupStore provides a MySQL-backed alternative.
type GroupStore interface {
	// Create creates a new group and adds the owner + optional initial members.
	Create(ctx context.Context, name, ownerUID string, members []string) (*Group, error)
	AddMember(ctx context.Context, groupID, uid string) error
	RemoveMember(ctx context.Context, groupID, uid string) error
	GetMembers(ctx context.Context, groupID string) ([]string, error)
	IsMember(ctx context.Context, groupID, uid string) bool
	GetUserGroups(ctx context.Context, uid string) ([]*Group, error)
	Get(ctx context.Context, groupID string) (*Group, error)
	UpdateName(ctx context.Context, groupID, newName string) error
	TransferOwnership(ctx context.Context, groupID, fromUID, toUID string) error
}

// Sentinel errors for group operations.
var (
	ErrGroupNotFound  = errors.New("group not found")
	ErrNotOwner       = errors.New("only the group owner can perform this action")
	ErrAlreadyMember  = errors.New("user is already a member of this group")
	ErrNotMember      = errors.New("user is not a member of this group")
	ErrGroupIDExists  = errors.New("group ID already exists")
)

// InMemoryGroupStore implements GroupStore with an in-memory map.
// This is the MVP implementation; group state is lost on restart.
type InMemoryGroupStore struct {
	mu     sync.RWMutex
	groups map[string]*Group // groupID → Group
	snow   *snowflake.Generator
}

// NewInMemoryGroupStore creates an InMemoryGroupStore with a snowflake generator
// for group ID assignment.
func NewInMemoryGroupStore(snow *snowflake.Generator) *InMemoryGroupStore {
	return &InMemoryGroupStore{
		groups: make(map[string]*Group),
		snow:   snow,
	}
}

// newGroupID generates a snowflake-based group ID.
func (s *InMemoryGroupStore) newGroupID() string {
	return fmt.Sprintf("g_%d", s.snow.Next())
}

// Create creates a new group and adds the owner and initial members.
func (s *InMemoryGroupStore) Create(ctx context.Context, name, ownerUID string, members []string) (*Group, error) {
	id := s.newGroupID()
	memberMap := map[string]bool{ownerUID: true}
	for _, uid := range members {
		if uid != "" && uid != ownerUID {
			memberMap[uid] = true
		}
	}
	g := &Group{
		ID:        id,
		Name:      name,
		OwnerUID:  ownerUID,
		Members:   memberMap,
		CreatedAt: time.Now().UnixMilli(),
	}

	s.mu.Lock()
	// Guard against ID collision (should be impossible with snowflake, but be safe).
	if _, exists := s.groups[id]; exists {
		s.mu.Unlock()
		return nil, ErrGroupIDExists
	}
	s.groups[id] = g
	s.mu.Unlock()

	return g, nil
}

// AddMember adds a user to a group. Returns an error if the user is already a member.
func (s *InMemoryGroupStore) AddMember(ctx context.Context, groupID, uid string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	g, ok := s.groups[groupID]
	if !ok {
		return ErrGroupNotFound
	}
	if g.Members[uid] {
		return ErrAlreadyMember
	}
	g.Members[uid] = true
	return nil
}

// RemoveMember removes a user from a group. If the group becomes empty, it is deleted.
func (s *InMemoryGroupStore) RemoveMember(ctx context.Context, groupID, uid string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	g, ok := s.groups[groupID]
	if !ok {
		return ErrGroupNotFound
	}
	if !g.Members[uid] {
		return ErrNotMember
	}
	delete(g.Members, uid)

	// Delete empty groups automatically.
	if len(g.Members) == 0 {
		delete(s.groups, groupID)
	}
	return nil
}

// GetMembers returns the list of member UIDs for a group.
func (s *InMemoryGroupStore) GetMembers(ctx context.Context, groupID string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	g, ok := s.groups[groupID]
	if !ok {
		return nil, ErrGroupNotFound
	}
	members := make([]string, 0, len(g.Members))
	for uid := range g.Members {
		members = append(members, uid)
	}
	return members, nil
}

// IsMember returns true if the user is a member of the group.
func (s *InMemoryGroupStore) IsMember(ctx context.Context, groupID, uid string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	g, ok := s.groups[groupID]
	if !ok {
		return false
	}
	return g.Members[uid]
}

// GetUserGroups returns all groups the user is a member of.
func (s *InMemoryGroupStore) GetUserGroups(ctx context.Context, uid string) ([]*Group, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*Group
	for _, g := range s.groups {
		if g.Members[uid] {
			result = append(result, g)
		}
	}
	return result, nil
}

// Get returns a group by ID, or nil if not found.
func (s *InMemoryGroupStore) Get(ctx context.Context, groupID string) (*Group, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	g, ok := s.groups[groupID]
	if !ok {
		return nil, ErrGroupNotFound
	}
	return g, nil
}

// UpdateName changes the group's display name.
func (s *InMemoryGroupStore) UpdateName(ctx context.Context, groupID, newName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	g, ok := s.groups[groupID]
	if !ok {
		return ErrGroupNotFound
	}
	g.Name = newName
	return nil
}

// TransferOwnership transfers group ownership from one member to another.
func (s *InMemoryGroupStore) TransferOwnership(ctx context.Context, groupID, fromUID, toUID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	g, ok := s.groups[groupID]
	if !ok {
		return ErrGroupNotFound
	}
	if g.OwnerUID != fromUID {
		return ErrNotOwner
	}
	if !g.Members[toUID] {
		return ErrNotMember
	}
	g.OwnerUID = toUID
	return nil
}
