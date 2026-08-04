package gateway

import (
	"context"
	"database/sql"
	"errors"

	"github.com/im/internal/pkg/snowflake"
	"github.com/im/internal/repo"
)

// 编译期检查:MySQLGroupStore 实现了 GroupStore。
var _ GroupStore = (*MySQLGroupStore)(nil)

// MySQLGroupStore 实现基于 MySQL 的 GroupStore。
// 它将所有 SQL 操作委托给 repo.MySQLGroupStore,充当薄适配器,
// 在 repo 层的 GroupRow 类型与 gateway 层的 Group 类型
// (包含已填充的 Members 映射)之间做映射。
type MySQLGroupStore struct {
	db   *sql.DB               // 保留以备直接访问之需
	snow *snowflake.Generator  // 保留用于生成 ID
	repo *repo.MySQLGroupStore // SQL 操作的唯一来源
}

// NewMySQLGroupStore 使用现有的数据库连接创建一个 MySQLGroupStore。
// 调用方必须确保 groups 和 group_members 表已存在
// (MySQLStore.migrate() 会创建它们)。
func NewMySQLGroupStore(db *sql.DB, snow *snowflake.Generator) *MySQLGroupStore {
	return &MySQLGroupStore{
		db:   db,
		snow: snow,
		repo: repo.NewMySQLGroupStore(db, snow.Next),
	}
}

// translateRepoError 将 repo 层的哨兵错误映射为 gateway 层对应的错误。
func translateRepoError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, repo.ErrGroupNotFound):
		return ErrGroupNotFound
	case errors.Is(err, repo.ErrNotOwner):
		return ErrNotOwner
	case errors.Is(err, repo.ErrAlreadyMember):
		return ErrAlreadyMember
	case errors.Is(err, repo.ErrNotMember):
		return ErrNotMember
	default:
		return err
	}
}

// ---------- GroupStore 实现 ----------

// Create 创建新群并添加群主和初始成员。
func (s *MySQLGroupStore) Create(ctx context.Context, name, ownerUID string, members []string) (*Group, error) {
	row, err := s.repo.CreateGroup(ctx, name, ownerUID, members)
	if err != nil {
		return nil, translateRepoError(err)
	}
	memberMap := map[string]bool{ownerUID: true}
	for _, uid := range members {
		if uid != "" && uid != ownerUID {
			memberMap[uid] = true
		}
	}
	return &Group{
		ID:        row.ID,
		Name:      row.Name,
		OwnerUID:  row.OwnerUID,
		Members:   memberMap,
		CreatedAt: row.CreatedAt,
	}, nil
}

// AddMember 将用户加入群。
func (s *MySQLGroupStore) AddMember(ctx context.Context, groupID, uid string) error {
	return translateRepoError(s.repo.AddMember(ctx, groupID, uid))
}

// RemoveMember 将用户移出群。如果群变为空,则删除该群。
func (s *MySQLGroupStore) RemoveMember(ctx context.Context, groupID, uid string) error {
	_, err := s.repo.RemoveMember(ctx, groupID, uid)
	return translateRepoError(err)
}

// GetMembers 返回群的成员 UID 列表。
func (s *MySQLGroupStore) GetMembers(ctx context.Context, groupID string) ([]string, error) {
	members, err := s.repo.GetMembers(ctx, groupID)
	return members, translateRepoError(err)
}

// IsMember 如果用户是该群成员则返回 true。
func (s *MySQLGroupStore) IsMember(ctx context.Context, groupID, uid string) bool {
	return s.repo.IsMember(ctx, groupID, uid)
}

// GetUserGroups 返回用户所属的所有群,并填充成员列表。
func (s *MySQLGroupStore) GetUserGroups(ctx context.Context, uid string) ([]*Group, error) {
	rows, err := s.repo.ListGroups(ctx, uid)
	if err != nil {
		return nil, translateRepoError(err)
	}

	groups := make([]*Group, 0, len(rows))
	for _, row := range rows {
		g := &Group{
			ID:        row.ID,
			Name:      row.Name,
			OwnerUID:  row.OwnerUID,
			Members:   make(map[string]bool),
			CreatedAt: row.CreatedAt,
		}
		// 为每个群填充成员列表。
		members, err := s.repo.GetMembers(ctx, g.ID)
		if err != nil {
			return nil, translateRepoError(err)
		}
		for _, mUID := range members {
			g.Members[mUID] = true
		}
		groups = append(groups, g)
	}

	return groups, nil
}

// Get 按 ID 返回群并填充成员列表,未找到时返回 ErrGroupNotFound。
func (s *MySQLGroupStore) Get(ctx context.Context, groupID string) (*Group, error) {
	row, members, err := s.repo.GetMembersForGroup(ctx, groupID)
	if err != nil {
		return nil, translateRepoError(err)
	}

	g := &Group{
		ID:        row.ID,
		Name:      row.Name,
		OwnerUID:  row.OwnerUID,
		Members:   make(map[string]bool, len(members)),
		CreatedAt: row.CreatedAt,
	}
	for _, uid := range members {
		g.Members[uid] = true
	}
	return g, nil
}

// UpdateName 修改群的显示名称。
func (s *MySQLGroupStore) UpdateName(ctx context.Context, groupID, newName string) error {
	return translateRepoError(s.repo.UpdateName(ctx, groupID, newName))
}

// TransferOwnership 将群所有权从一个成员转给另一个成员。
func (s *MySQLGroupStore) TransferOwnership(ctx context.Context, groupID, fromUID, toUID string) error {
	return translateRepoError(s.repo.TransferOwnership(ctx, groupID, fromUID, toUID))
}
