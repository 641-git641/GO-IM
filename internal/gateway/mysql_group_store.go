package gateway

import (
	"context"
	"database/sql"
	"errors"

	"github.com/im/internal/pkg/snowflake"
	"github.com/im/internal/repo"
)

// Compile-time check: MySQLGroupStore implements GroupStore.
var _ GroupStore = (*MySQLGroupStore)(nil)

// MySQLGroupStore implements GroupStore backed by MySQL.
// It delegates all SQL operations to repo.MySQLGroupStore, acting as a thin
// adapter that maps between the repo layer's GroupRow types and the gateway
// layer's Group type (which includes a populated Members map).
type MySQLGroupStore struct {
	db   *sql.DB                 // retained for direct access if needed
	snow *snowflake.Generator    // retained for ID generation
	repo *repo.MySQLGroupStore   // the single source of SQL operations
}

// NewMySQLGroupStore creates a MySQLGroupStore using an existing database connection.
// The caller must ensure that the groups and group_members tables exist
// (MySQLStore.migrate() creates them).
func NewMySQLGroupStore(db *sql.DB, snow *snowflake.Generator) *MySQLGroupStore {
	return &MySQLGroupStore{
		db:   db,
		snow: snow,
		repo: repo.NewMySQLGroupStore(db, snow.Next),
	}
}

// translateRepoError maps repo-layer sentinel errors to their gateway equivalents.
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

// ---------- GroupStore implementation ----------

// Create creates a new group and adds the owner and initial members.
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

// AddMember adds a user to a group.
func (s *MySQLGroupStore) AddMember(ctx context.Context, groupID, uid string) error {
	return translateRepoError(s.repo.AddMember(ctx, groupID, uid))
}

// RemoveMember removes a user from a group. If the group becomes empty, it is deleted.
func (s *MySQLGroupStore) RemoveMember(ctx context.Context, groupID, uid string) error {
	_, err := s.repo.RemoveMember(ctx, groupID, uid)
	return translateRepoError(err)
}

// GetMembers returns the list of member UIDs for a group.
func (s *MySQLGroupStore) GetMembers(ctx context.Context, groupID string) ([]string, error) {
	members, err := s.repo.GetMembers(ctx, groupID)
	return members, translateRepoError(err)
}

// IsMember returns true if the user is a member of the group.
func (s *MySQLGroupStore) IsMember(ctx context.Context, groupID, uid string) bool {
	return s.repo.IsMember(ctx, groupID, uid)
}

// GetUserGroups returns all groups the user is a member of, with members populated.
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
		// Populate member list for each group.
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

// Get returns a group by ID with members populated, or ErrGroupNotFound.
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

// UpdateName changes the group's display name.
func (s *MySQLGroupStore) UpdateName(ctx context.Context, groupID, newName string) error {
	return translateRepoError(s.repo.UpdateName(ctx, groupID, newName))
}

// TransferOwnership transfers group ownership from one member to another.
func (s *MySQLGroupStore) TransferOwnership(ctx context.Context, groupID, fromUID, toUID string) error {
	return translateRepoError(s.repo.TransferOwnership(ctx, groupID, fromUID, toUID))
}
