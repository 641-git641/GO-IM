package repo

import (
	"context"
	"database/sql"
	"fmt"
	"log"
)

// UnreadRow 表示一条未读计数的数据行。
type UnreadRow struct {
	UID   string
	Peer  string
	Count int64
}

// MySQLUnreadStore 将未读计数持久化到 MySQL。
// 这是权威存储；Gateway 维护一个内存缓存用于热路径的递增操作。
type MySQLUnreadStore struct {
	db *sql.DB
}

// NewMySQLUnreadStore 使用现有的数据库连接创建 MySQLUnreadStore。
// 调用方必须确保 unread 表已存在（MySQLStore.migrate() 会创建它）。
func NewMySQLUnreadStore(db *sql.DB) *MySQLUnreadStore {
	return &MySQLUnreadStore{db: db}
}

// Increment 增加 (uid, peer) 的未读计数并返回新计数。
// 使用 INSERT ... ON DUPLICATE KEY UPDATE 实现原子 upsert。
func (s *MySQLUnreadStore) Increment(ctx context.Context, uid, peer string) (int64, error) {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO unread (uid, peer, count) VALUES (?, ?, 1)
		 ON DUPLICATE KEY UPDATE count = count + 1`,
		uid, peer,
	)
	if err != nil {
		return 0, fmt.Errorf("increment unread: %w", err)
	}

	// 读回新计数。
	return s.GetCount(ctx, uid, peer)
}

// MarkRead 清除 (reader, peer) 的未读计数。
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

// GetCounts 返回一个用户的全部未读计数。
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

// GetCount 返回特定 (uid, peer) 的未读计数。
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
