package repo

import (
	"context"
	"database/sql"
	"fmt"
	"log"
)

// UnreadRow represents an unread count row.
type UnreadRow struct {
	UID   string
	Peer  string
	Count int64
}

// MySQLUnreadStore persists unread counts to MySQL.
// This is the authoritative store; the Gateway maintains an in-memory cache
// for hot-path increment operations.
type MySQLUnreadStore struct {
	db *sql.DB
}

// NewMySQLUnreadStore creates a MySQLUnreadStore using an existing database connection.
// The caller must ensure that the unread table exists (MySQLStore.migrate() creates it).
func NewMySQLUnreadStore(db *sql.DB) *MySQLUnreadStore {
	return &MySQLUnreadStore{db: db}
}

// Increment increments the unread count for (uid, peer) and returns the new count.
// Uses INSERT ... ON DUPLICATE KEY UPDATE for atomic upsert.
func (s *MySQLUnreadStore) Increment(ctx context.Context, uid, peer string) (int64, error) {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO unread (uid, peer, count) VALUES (?, ?, 1)
		 ON DUPLICATE KEY UPDATE count = count + 1`,
		uid, peer,
	)
	if err != nil {
		return 0, fmt.Errorf("increment unread: %w", err)
	}

	// Read back the new count.
	return s.GetCount(ctx, uid, peer)
}

// MarkRead clears the unread count for (reader, peer).
func (s *MySQLUnreadStore) MarkRead(ctx context.Context, reader, peer string) error {
	_, err := s.db.ExecContext(ctx,
		"DELETE FROM unread WHERE uid = ? AND peer = ?",
		reader, peer,
	)
	if err != nil {
		return fmt.Errorf("mark read: %w", err)
	}
	log.Printf("[mysql_unread] marked read: %s <-> %s", reader, peer)
	return nil
}

// GetCounts returns all unread counts for a user.
func (s *MySQLUnreadStore) GetCounts(ctx context.Context, uid string) (map[string]int64, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT peer, count FROM unread WHERE uid = ?", uid,
	)
	if err != nil {
		return nil, fmt.Errorf("query unread counts: %w", err)
	}
	defer rows.Close()

	counts := make(map[string]int64)
	for rows.Next() {
		var peer string
		var count int64
		if err := rows.Scan(&peer, &count); err != nil {
			return nil, fmt.Errorf("scan unread: %w", err)
		}
		counts[peer] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate unread: %w", err)
	}
	return counts, nil
}

// GetCount returns the unread count for a specific (uid, peer).
func (s *MySQLUnreadStore) GetCount(ctx context.Context, uid, peer string) (int64, error) {
	var count int64
	err := s.db.QueryRowContext(ctx,
		"SELECT count FROM unread WHERE uid = ? AND peer = ?",
		uid, peer,
	).Scan(&count)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("get unread count: %w", err)
	}
	return count, nil
}
