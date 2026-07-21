package repo

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"
)

// GroupRow represents a group row from the database.
type GroupRow struct {
	ID        string
	Name      string
	OwnerUID  string
	CreatedAt int64
}

// MySQLGroupStore implements group persistence backed by MySQL.
// It operates on the groups and group_members tables created by MySQLStore.migrate().
type MySQLGroupStore struct {
	db   *sql.DB
	dsID func() int64 // id generator (snowflake, called by the caller)
}

// NewMySQLGroupStore creates a MySQLGroupStore using an existing database connection.
// The idFn should return a unique int64 ID (e.g., from snowflake generator).
func NewMySQLGroupStore(db *sql.DB, idFn func() int64) *MySQLGroupStore {
	return &MySQLGroupStore{db: db, dsID: idFn}
}

// newGroupID generates a snowflake-based group ID.
func (s *MySQLGroupStore) newGroupID() string {
	if s.dsID != nil {
		return fmt.Sprintf("g_%d", s.dsID())
	}
	return fmt.Sprintf("g_%d", time.Now().UnixNano())
}

// CreateGroup creates a new group and adds the owner as the first member.
// Returns the created group row.
func (s *MySQLGroupStore) CreateGroup(ctx context.Context, name, ownerUID string) (*GroupRow, error) {
	id := s.newGroupID()
	now := time.Now().UnixMilli()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx,
		"INSERT INTO `groups` (id, name, owner_uid, created_at) VALUES (?, ?, ?, ?)",
		id, name, ownerUID, now,
	)
	if err != nil {
		return nil, fmt.Errorf("insert group: %w", err)
	}

	_, err = tx.ExecContext(ctx,
		"INSERT INTO group_members (group_id, uid, joined_at) VALUES (?, ?, ?)",
		id, ownerUID, now,
	)
	if err != nil {
		return nil, fmt.Errorf("insert owner member: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	log.Printf("[mysql_group] created group %s for owner %s", id, ownerUID)
	return &GroupRow{ID: id, Name: name, OwnerUID: ownerUID, CreatedAt: now}, nil
}

// AddMember adds a user to a group. Returns a sentinel error on failure.
func (s *MySQLGroupStore) AddMember(ctx context.Context, groupID, uid string) error {
	var exists int
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(1) FROM `groups` WHERE id = ?", groupID).Scan(&exists)
	if err != nil {
		return fmt.Errorf("check group: %w", err)
	}
	if exists == 0 {
		return ErrGroupNotFound
	}

	now := time.Now().UnixMilli()
	_, err = s.db.ExecContext(ctx,
		"INSERT INTO group_members (group_id, uid, joined_at) VALUES (?, ?, ?)",
		groupID, uid, now,
	)
	if err != nil {
		if isMySQLDuplicate(err) {
			return ErrAlreadyMember
		}
		return fmt.Errorf("insert member: %w", err)
	}

	return nil
}

// RemoveMember removes a user from a group. If the group becomes empty, it is deleted.
// Returns whether the group was deleted, and any error.
func (s *MySQLGroupStore) RemoveMember(ctx context.Context, groupID, uid string) (deleted bool, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	var memberCount int
	err = tx.QueryRowContext(ctx,
		"SELECT COUNT(1) FROM group_members WHERE group_id = ?", groupID,
	).Scan(&memberCount)
	if err != nil {
		return false, fmt.Errorf("count members: %w", err)
	}

	var groupExists int
	err = tx.QueryRowContext(ctx, "SELECT COUNT(1) FROM `groups` WHERE id = ?", groupID).Scan(&groupExists)
	if err != nil {
		return false, fmt.Errorf("check group: %w", err)
	}
	if groupExists == 0 {
		return false, ErrGroupNotFound
	}

	result, err := tx.ExecContext(ctx,
		"DELETE FROM group_members WHERE group_id = ? AND uid = ?", groupID, uid,
	)
	if err != nil {
		return false, fmt.Errorf("delete member: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return false, ErrNotMember
	}

	if memberCount <= 1 {
		_, err = tx.ExecContext(ctx, "DELETE FROM `groups` WHERE id = ?", groupID)
		if err != nil {
			return false, fmt.Errorf("delete empty group: %w", err)
		}
		deleted = true
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit: %w", err)
	}

	return deleted, nil
}

// GetMembers returns the list of member UIDs for a group.
func (s *MySQLGroupStore) GetMembers(ctx context.Context, groupID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT uid FROM group_members WHERE group_id = ? ORDER BY joined_at ASC", groupID,
	)
	if err != nil {
		return nil, fmt.Errorf("query members: %w", err)
	}
	defer rows.Close()

	var members []string
	for rows.Next() {
		var uid string
		if err := rows.Scan(&uid); err != nil {
			return nil, fmt.Errorf("scan member: %w", err)
		}
		members = append(members, uid)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate members: %w", err)
	}

	if members == nil {
		var exists int
		if err := s.db.QueryRowContext(ctx, "SELECT COUNT(1) FROM `groups` WHERE id = ?", groupID).Scan(&exists); err != nil {
			return nil, fmt.Errorf("check group: %w", err)
		}
		if exists == 0 {
			return nil, ErrGroupNotFound
		}
		return []string{}, nil
	}

	return members, nil
}

// GetGroup returns a group row by ID.
func (s *MySQLGroupStore) GetGroup(ctx context.Context, groupID string) (*GroupRow, error) {
	g := &GroupRow{}
	err := s.db.QueryRowContext(ctx,
		"SELECT id, name, owner_uid, created_at FROM `groups` WHERE id = ?", groupID,
	).Scan(&g.ID, &g.Name, &g.OwnerUID, &g.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, ErrGroupNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("select group: %w", err)
	}
	return g, nil
}

// ListGroups returns all groups the user is a member of.
func (s *MySQLGroupStore) ListGroups(ctx context.Context, uid string) ([]*GroupRow, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT g.id, g.name, g.owner_uid, g.created_at "+
			"FROM `groups` g "+
			"INNER JOIN group_members gm ON g.id = gm.group_id "+
			"WHERE gm.uid = ? "+
			"ORDER BY g.created_at DESC", uid,
	)
	if err != nil {
		return nil, fmt.Errorf("query user groups: %w", err)
	}
	defer rows.Close()

	var groups []*GroupRow
	for rows.Next() {
		g := &GroupRow{}
		if err := rows.Scan(&g.ID, &g.Name, &g.OwnerUID, &g.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan group: %w", err)
		}
		groups = append(groups, g)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate groups: %w", err)
	}

	if groups == nil {
		return []*GroupRow{}, nil
	}
	return groups, nil
}

// IsMember returns true if the user is a member of the group.
func (s *MySQLGroupStore) IsMember(ctx context.Context, groupID, uid string) bool {
	var count int
	err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(1) FROM group_members WHERE group_id = ? AND uid = ?",
		groupID, uid,
	).Scan(&count)
	return err == nil && count > 0
}

// UpdateName changes a group's display name. Returns a sentinel error on failure.
func (s *MySQLGroupStore) UpdateName(ctx context.Context, groupID, newName string) error {
	result, err := s.db.ExecContext(ctx,
		"UPDATE `groups` SET name = ? WHERE id = ?", newName, groupID,
	)
	if err != nil {
		return fmt.Errorf("update group name: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return ErrGroupNotFound
	}
	return nil
}

// TransferOwnership transfers group ownership from one member to another.
func (s *MySQLGroupStore) TransferOwnership(ctx context.Context, groupID, fromUID, toUID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	var ownerUID string
	err = tx.QueryRowContext(ctx,
		"SELECT owner_uid FROM `groups` WHERE id = ?", groupID,
	).Scan(&ownerUID)
	if err == sql.ErrNoRows {
		return ErrGroupNotFound
	}
	if err != nil {
		return fmt.Errorf("select group: %w", err)
	}
	if ownerUID != fromUID {
		return ErrNotOwner
	}

	var exists int
	err = tx.QueryRowContext(ctx,
		"SELECT COUNT(1) FROM group_members WHERE group_id = ? AND uid = ?", groupID, toUID,
	).Scan(&exists)
	if err != nil {
		return fmt.Errorf("check member: %w", err)
	}
	if exists == 0 {
		return ErrNotMember
	}

	_, err = tx.ExecContext(ctx,
		"UPDATE `groups` SET owner_uid = ? WHERE id = ?", toUID, groupID,
	)
	if err != nil {
		return fmt.Errorf("update owner: %w", err)
	}

	return tx.Commit()
}

// GetMembersForGroup returns member UIDs with their details (same as GetMembers but
// returns the group row too, used by callers that need both).
func (s *MySQLGroupStore) GetMembersForGroup(ctx context.Context, groupID string) (group *GroupRow, members []string, err error) {
	group, err = s.GetGroup(ctx, groupID)
	if err != nil {
		return nil, nil, err
	}
	members, err = s.GetMembers(ctx, groupID)
	if err != nil {
		return nil, nil, err
	}
	return group, members, nil
}

// Sentinel errors for group operations. Shared by MySQL and in-memory implementations.
var (
	ErrGroupNotFound = fmt.Errorf("group not found")
	ErrNotOwner      = fmt.Errorf("only the group owner can perform this action")
	ErrAlreadyMember = fmt.Errorf("user is already a member of this group")
	ErrNotMember     = fmt.Errorf("user is not a member of this group")
)

// isMySQLDuplicate checks whether the error is MySQL error 1062 (duplicate entry).
func isMySQLDuplicate(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Error 1062") || strings.Contains(msg, "Duplicate entry")
}
