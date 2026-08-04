package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/im/internal/pkg/snowflake"
)

// Group 表示一个聊天群及其成员。
type Group struct {
	ID        string          `json:"id"`         // 例如 "g_123456"
	Name      string          `json:"name"`       // 人类可读的群名称
	OwnerUID  string          `json:"owner_uid"`  // 创建者/群主
	Members   map[string]bool `json:"members"`    // uid → true(包含群主)
	CreatedAt int64           `json:"created_at"` // unix 毫秒时间戳
}

// GroupStore 管理群的持久化。InMemoryGroupStore 是默认实现;
// MySQLGroupStore 提供基于 MySQL 的替代实现。
type GroupStore interface {
	// Create 创建新群并添加群主及可选的初始成员。
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

// 群操作的哨兵错误。
var (
	ErrGroupNotFound = errors.New("group not found")
	ErrNotOwner      = errors.New("only the group owner can perform this action")
	ErrAlreadyMember = errors.New("user is already a member of this group")
	ErrNotMember     = errors.New("user is not a member of this group")
	ErrGroupIDExists = errors.New("group ID already exists")
)

// InMemoryGroupStore 用内存映射实现 GroupStore。
// 这是 MVP 实现;重启后群状态丢失。
type InMemoryGroupStore struct {
	mu     sync.RWMutex
	groups map[string]*Group // groupID → Group
	snow   *snowflake.Generator
}

// NewInMemoryGroupStore 创建一个 InMemoryGroupStore,使用 snowflake 生成器
// 分配群 ID。
func NewInMemoryGroupStore(snow *snowflake.Generator) *InMemoryGroupStore {
	return &InMemoryGroupStore{
		groups: make(map[string]*Group),
		snow:   snow,
	}
}

// newGroupID 生成基于 snowflake 的群 ID。
func (s *InMemoryGroupStore) newGroupID() string {
	return fmt.Sprintf("g_%d", s.snow.Next())
}

// Create 创建新群并添加群主和初始成员。
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
	// 防止 ID 冲突(使用 snowflake 几乎不可能,但以防万一)。
	if _, exists := s.groups[id]; exists {
		s.mu.Unlock()
		return nil, ErrGroupIDExists
	}
	s.groups[id] = g
	s.mu.Unlock()

	return g, nil
}

// AddMember 将用户加入群。如果用户已是成员则返回错误。
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

// RemoveMember 将用户移出群。如果群变为空,则删除该群。
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

	// 自动删除空群。
	if len(g.Members) == 0 {
		delete(s.groups, groupID)
	}
	return nil
}

// GetMembers 返回群的成员 UID 列表。
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

// IsMember 如果用户是该群成员则返回 true。
func (s *InMemoryGroupStore) IsMember(ctx context.Context, groupID, uid string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	g, ok := s.groups[groupID]
	if !ok {
		return false
	}
	return g.Members[uid]
}

// GetUserGroups 返回用户所属的所有群。
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

// Get 按 ID 返回群,未找到时返回 nil。
func (s *InMemoryGroupStore) Get(ctx context.Context, groupID string) (*Group, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	g, ok := s.groups[groupID]
	if !ok {
		return nil, ErrGroupNotFound
	}
	return g, nil
}

// UpdateName 修改群的显示名称。
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

// TransferOwnership 将群所有权从一个成员转给另一个成员。
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
