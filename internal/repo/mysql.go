package repo

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/im/api/proto"

	_ "github.com/go-sql-driver/mysql" // MySQL driver registration
)

// Compile-time interface compliance checks.
var (
	_ UserStore    = (*MySQLStore)(nil)
	_ MessageStore = (*MySQLStore)(nil)
	_ FriendStore  = (*MySQLStore)(nil)
)

// MySQLStore implements UserStore and MessageStore backed by MySQL.
type MySQLStore struct {
	db *sql.DB
}

// NewMySQLStore opens a MySQL connection, verifies it, and creates tables.
// The caller is responsible for closing the store via Close().
func NewMySQLStore(dsn string) (*MySQLStore, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("mysql open: %w", err)
	}

	// Connection pool settings.
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("mysql ping: %w", err)
	}

	s := &MySQLStore{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("mysql migrate: %w", err)
	}

	log.Printf("[mysql] connected and migrated")
	return s, nil
}

// DB returns the underlying *sql.DB for use by other MySQL-backed stores
// (e.g., MySQLGroupStore) that share the same connection pool.
func (s *MySQLStore) DB() *sql.DB {
	return s.db
}

// Ping checks the database connection health.
func (s *MySQLStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// Close releases the database connection pool.
func (s *MySQLStore) Close() error {
	return s.db.Close()
}

// migrate creates tables if they do not exist.
func (s *MySQLStore) migrate() error {
	userTable := `
	CREATE TABLE IF NOT EXISTS users (
		uid           VARCHAR(64)  PRIMARY KEY,
		username      VARCHAR(128) NOT NULL,
		password_hash VARCHAR(256) NOT NULL,
		created_at    BIGINT       NOT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`

	msgTable := `
	CREATE TABLE IF NOT EXISTS messages (
		msg_id    BIGINT       PRIMARY KEY,
		seq       BIGINT       NOT NULL DEFAULT 0,
		cmd       INT          NOT NULL DEFAULT 1,
		from_uid  VARCHAR(64)  NOT NULL,
		to_uid    VARCHAR(64)  NOT NULL,
		chat_type INT          NOT NULL DEFAULT 1,
		msg_type  INT          NOT NULL DEFAULT 1,
		content   TEXT         NOT NULL,
		timestamp BIGINT       NOT NULL,
		need_ack  TINYINT      NOT NULL DEFAULT 0,
		recalled  TINYINT      NOT NULL DEFAULT 0,
		INDEX idx_from_to_ts (from_uid, to_uid, timestamp),
		INDEX idx_to_ts (to_uid, timestamp)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`

	groupTable := `
		CREATE TABLE IF NOT EXISTS ` + "`groups`" + ` (
			id          VARCHAR(64)  PRIMARY KEY,
			name        VARCHAR(256) NOT NULL,
			owner_uid   VARCHAR(64)  NOT NULL,
			created_at  BIGINT       NOT NULL,
			INDEX idx_owner (owner_uid)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`

	groupMembersTable := `
		CREATE TABLE IF NOT EXISTS group_members (
			group_id    VARCHAR(64)  NOT NULL,
			uid         VARCHAR(64)  NOT NULL,
			joined_at   BIGINT       NOT NULL,
			PRIMARY KEY (group_id, uid),
			INDEX idx_member_uid (uid)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`

	unreadTable := `
		CREATE TABLE IF NOT EXISTS unread (
			uid   VARCHAR(64) NOT NULL,
			peer  VARCHAR(64) NOT NULL,
			count BIGINT      NOT NULL DEFAULT 0,
			PRIMARY KEY (uid, peer)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`

	friendsTable := `
		CREATE TABLE IF NOT EXISTS friends (
			id         BIGINT AUTO_INCREMENT PRIMARY KEY,
			uid        VARCHAR(64) NOT NULL,
			friend_uid VARCHAR(64) NOT NULL,
			status     TINYINT     NOT NULL DEFAULT 0,
			created_at BIGINT      NOT NULL,
			updated_at BIGINT      NOT NULL,
			UNIQUE KEY uk_friendship (uid, friend_uid),
			INDEX idx_uid_status (uid, status),
			INDEX idx_friend_status (friend_uid, status)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`

	for _, ddl := range []string{userTable, msgTable, groupTable, groupMembersTable, unreadTable, friendsTable} {
		if _, err := s.db.Exec(ddl); err != nil {
			return fmt.Errorf("exec ddl: %w", err)
		}
	}

	// Add FULLTEXT index for message search (best-effort).
	ftsDDL := `ALTER TABLE messages ADD FULLTEXT INDEX ft_content (content) WITH PARSER ngram`
	if _, err := s.db.Exec(ftsDDL); err != nil {
		// MySQL error 1061 = duplicate index name — safe to ignore.
		// Error 1128/1214 = ngram parser not supported or other schema issue — log as warning.
		if strings.Contains(err.Error(), "Error 1061") {
			log.Printf("[mysql] FULLTEXT index already exists (this is OK)")
		} else {
			log.Printf("[mysql] WARNING: FULLTEXT index creation failed (search may not work): %v", err)
		}
	}

		// Add recalled column for existing databases (best-effort migration).
		recallDDL := `ALTER TABLE messages ADD COLUMN recalled TINYINT NOT NULL DEFAULT 0`
		if _, err := s.db.Exec(recallDDL); err != nil {
			if strings.Contains(err.Error(), "Error 1060") {
				log.Printf("[mysql] recalled column already exists (this is OK)")
			} else {
				log.Printf("[mysql] WARNING: recall column migration failed (recall may not work): %v", err)
			}
		}

		// Add role column for admin users (best-effort migration).
		roleDDL := `ALTER TABLE users ADD COLUMN role VARCHAR(16) NOT NULL DEFAULT 'user'`
		if _, err := s.db.Exec(roleDDL); err != nil {
			if strings.Contains(err.Error(), "Error 1060") {
				log.Printf("[mysql] role column already exists (this is OK)")
			} else {
				log.Printf("[mysql] WARNING: role column migration failed: %v", err)
			}
		}

		// Add is_disabled column for disabling users (best-effort migration).
		disabledDDL := `ALTER TABLE users ADD COLUMN is_disabled TINYINT NOT NULL DEFAULT 0`
		if _, err := s.db.Exec(disabledDDL); err != nil {
			if strings.Contains(err.Error(), "Error 1060") {
				log.Printf("[mysql] is_disabled column already exists (this is OK)")
			} else {
				log.Printf("[mysql] WARNING: is_disabled column migration failed: %v", err)
			}
		}
	return nil
}

// ---------- UserStore implementation ----------

// Create inserts a new user. Returns an error if the UID already exists.
func (s *MySQLStore) Create(ctx context.Context, u *User) error {
	_, err := s.db.ExecContext(ctx,
		"INSERT INTO users (uid, username, password_hash, created_at) VALUES (?, ?, ?, ?)",
		u.UID, u.Username, u.PasswordHash, u.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert user: %w", err)
	}
	return nil
}

// UpdatePassword updates a user's password hash.
// Returns an error if the UID does not exist.
func (s *MySQLStore) UpdatePassword(ctx context.Context, uid, newPasswordHash string) error {
	result, err := s.db.ExecContext(ctx,
		"UPDATE users SET password_hash = ? WHERE uid = ?",
		newPasswordHash, uid,
	)
	if err != nil {
		return fmt.Errorf("update password: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("user %s not found", uid)
	}
	return nil
}

// GetByUID retrieves a user by UID. Returns nil if not found.
func (s *MySQLStore) GetByUID(ctx context.Context, uid string) (*User, error) {
	u := &User{}
	err := s.db.QueryRowContext(ctx,
		"SELECT uid, username, password_hash, role, is_disabled, created_at FROM users WHERE uid = ?", uid,
	).Scan(&u.UID, &u.Username, &u.PasswordHash, &u.Role, &u.IsDisabled, &u.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("select user: %w", err)
	}
	return u, nil
}

// ---------- MessageStore implementation ----------

// Save persists a chat message to the message history.
func (s *MySQLStore) Save(ctx context.Context, msg *proto.Message) error {
	needAck := 0
	if msg.NeedAck {
		needAck = 1
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT IGNORE INTO messages (msg_id, seq, cmd, from_uid, to_uid, chat_type, msg_type, content, timestamp, need_ack, recalled)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)`,
		msg.MsgId, msg.Seq, msg.Cmd, msg.From, msg.To,
		msg.ChatType, msg.MsgType, msg.Content, msg.Timestamp, needAck,
	)
	if err != nil {
		return fmt.Errorf("insert message: %w", err)
	}
	return nil
}

// RecallMessage marks a message as recalled by setting its content and recalled flag.
// Only the original sender can recall their own message, and only within the recall window
// (recallWindowMs milliseconds since the message was sent). Returns an error if no row was matched.
func (s *MySQLStore) RecallMessage(ctx context.Context, msgID int64, fromUID string, recallWindowMs int64) error {
	cutoff := time.Now().UnixMilli() - recallWindowMs
	result, err := s.db.ExecContext(ctx,
		`UPDATE messages SET content = '{"recalled":true}', recalled = 1 WHERE msg_id = ? AND from_uid = ? AND timestamp >= ?`,
		msgID, fromUID, cutoff,
	)
	if err != nil {
		return fmt.Errorf("recall message: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("message %d not found or not owned by %s", msgID, fromUID)
	}
	return nil
}

// UpdateMessageContent updates the content of a previously stored message.
// Only the author (fromUID) can edit their own messages.
func (s *MySQLStore) UpdateMessageContent(ctx context.Context, msgID int64, fromUID, newContent string) error {
	result, err := s.db.ExecContext(ctx,
		`UPDATE messages SET content = ? WHERE msg_id = ? AND from_uid = ?`,
		newContent, msgID, fromUID,
	)
	if err != nil {
		return fmt.Errorf("update message content: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("message %d not found", msgID)
	}
	return nil
}

// QueryHistory returns messages between two users, ordered newest-first.
// before is a timestamp in unix millis; only messages older than this are returned.
// limit caps the result count (max 200).
func (s *MySQLStore) QueryHistory(ctx context.Context, uid1, uid2 string, before int64, limit int) ([]*proto.Message, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT msg_id, seq, cmd, from_uid, to_uid, chat_type, msg_type, content, timestamp, need_ack, recalled
		 FROM messages
		 WHERE ((from_uid = ? AND to_uid = ?) OR (from_uid = ? AND to_uid = ?))
		   AND timestamp < ?
		 ORDER BY timestamp DESC
		 LIMIT ?`,
		uid1, uid2, uid2, uid1, before, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query history: %w", err)
	}
	defer rows.Close()

	msgs := make([]*proto.Message, 0)
	for rows.Next() {
		m := &proto.Message{}
		var needAck, recalled int
		if err := rows.Scan(
			&m.MsgId, &m.Seq, &m.Cmd, &m.From, &m.To,
			&m.ChatType, &m.MsgType, &m.Content, &m.Timestamp, &needAck, &recalled,
		); err != nil {
			return nil, fmt.Errorf("scan message: %w", err)
		}
		m.NeedAck = needAck == 1
		if recalled == 1 {
			m.Content = `{"recalled":true}`
		}
		msgs = append(msgs, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iterate: %w", err)
	}
	return msgs, nil
}

// QueryGroupHistory returns messages sent to a group, ordered newest-first.
// before is a timestamp in unix millis; only messages older than this are returned.
// limit caps the result count (max 200).
func (s *MySQLStore) QueryGroupHistory(ctx context.Context, groupID string, before int64, limit int) ([]*proto.Message, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT msg_id, seq, cmd, from_uid, to_uid, chat_type, msg_type, content, timestamp, need_ack, recalled
		 FROM messages
		 WHERE to_uid = ? AND chat_type = 2 AND timestamp < ?
		 ORDER BY timestamp DESC
		 LIMIT ?`,
		groupID, before, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query group history: %w", err)
	}
	defer rows.Close()

	msgs := make([]*proto.Message, 0)
	for rows.Next() {
		m := &proto.Message{}
		var needAck, recalled int
		if err := rows.Scan(
			&m.MsgId, &m.Seq, &m.Cmd, &m.From, &m.To,
			&m.ChatType, &m.MsgType, &m.Content, &m.Timestamp, &needAck, &recalled,
		); err != nil {
			return nil, fmt.Errorf("scan message: %w", err)
		}
		m.NeedAck = needAck == 1
		if recalled == 1 {
			m.Content = `{"recalled":true}`
		}
		msgs = append(msgs, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iterate: %w", err)
	}
	return msgs, nil
}

// SearchMessages performs a fulltext search on message content.
// Access control: only returns messages where uid is a participant (from or to).
func (s *MySQLStore) SearchMessages(ctx context.Context, params *SearchParams) (*SearchResult, error) {
	if params == nil {
		return &SearchResult{}, nil
	}

	limit := params.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 50 {
		limit = 50
	}

	// Validate and sanitize query — empty or whitespace-only query returns empty results.
	queryText := strings.TrimSpace(params.Query)
	if queryText == "" {
		return &SearchResult{}, nil
	}
	// Escape BOOLEAN MODE special characters to prevent query injection.
	queryText = escapeBooleanMode(queryText)

	// Build WHERE clause dynamically based on optional filters.
	query := `SELECT msg_id, seq, cmd, from_uid, to_uid, chat_type, msg_type, content, timestamp, need_ack, recalled
		FROM messages
		WHERE MATCH(content) AGAINST(? IN BOOLEAN MODE)`

	args := []interface{}{queryText}

	// Access control: user must be from_uid or to_uid.
	if params.Peer != "" {
		// Scoped to a specific conversation.
		query += ` AND ((from_uid = ? AND to_uid = ?) OR (from_uid = ? AND to_uid = ?))`
		args = append(args, params.UID, params.Peer, params.Peer, params.UID)
	} else {
		// All conversations the user participates in.
		query += ` AND (from_uid = ? OR to_uid = ?)`
		args = append(args, params.UID, params.UID)
	}

	if params.ChatType > 0 {
		query += ` AND chat_type = ?`
		args = append(args, params.ChatType)
	}
	if params.MsgType > 0 {
		query += ` AND msg_type = ?`
		args = append(args, params.MsgType)
	}
	if params.Before > 0 {
		query += ` AND timestamp < ?`
		args = append(args, params.Before)
	}
	if params.After > 0 {
		query += ` AND timestamp > ?`
		args = append(args, params.After)
	}
	if params.Cursor > 0 {
		query += ` AND msg_id < ?`
		args = append(args, params.Cursor)
	}

	query += ` ORDER BY timestamp DESC LIMIT ?`
	args = append(args, limit+1) // fetch one extra to detect more pages

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("search: %w", err)
	}
	defer rows.Close()

	msgs := make([]*proto.Message, 0)
	for rows.Next() {
		m := &proto.Message{}
		var needAck, recalled int
		if err := rows.Scan(
			&m.MsgId, &m.Seq, &m.Cmd, &m.From, &m.To,
			&m.ChatType, &m.MsgType, &m.Content, &m.Timestamp, &needAck, &recalled,
		); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		m.NeedAck = needAck == 1
		if recalled == 1 {
			m.Content = `{"recalled":true}`
		}
		msgs = append(msgs, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iterate: %w", err)
	}

	hasMore := len(msgs) > limit
	if hasMore {
		msgs = msgs[:limit]
	}

	nextCursor := int64(0)
	if hasMore && len(msgs) > 0 {
		nextCursor = msgs[len(msgs)-1].MsgId
	}

	return &SearchResult{
		Messages:   msgs,
		Count:      len(msgs),
		NextCursor: nextCursor,
	}, nil
}

// escapeBooleanMode escapes MySQL BOOLEAN MODE special characters
// to prevent query injection. Characters: + - > < ( ) ~ * " @
func escapeBooleanMode(s string) string {
	replacer := strings.NewReplacer(
		"+", `\+`,
		"-", `\-`,
		">", `\>`,
		"<", `\<`,
		"(", `\(`,
		")", `\)`,
		"~", `\~`,
		"*", `\*`,
		`"`, `\"`,
		"@", `\@`,
	)
	return replacer.Replace(s)
}

// ---------- FriendStore implementation ----------

// SendRequest sends a friend request from fromUID to toUID.
func (s *MySQLStore) SendRequest(ctx context.Context, fromUID, toUID string) error {
	now := timeNow()
	// Insert with status=pending. The UNIQUE KEY prevents duplicate requests.
	// If a request already exists in the opposite direction it's also caught.
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO friends (uid, friend_uid, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`,
		fromUID, toUID, FriendStatusPending, now, now,
	)
	if err != nil {
		if strings.Contains(err.Error(), "Error 1062") || strings.Contains(err.Error(), "Duplicate entry") {
			return fmt.Errorf("friend request already exists between %s and %s", fromUID, toUID)
		}
		return fmt.Errorf("insert friend request: %w", err)
	}
	return nil
}

// AcceptRequest accepts a pending friend request.
func (s *MySQLStore) AcceptRequest(ctx context.Context, uid, fromUID string) error {
	result, err := s.db.ExecContext(ctx,
		`UPDATE friends SET status = ?, updated_at = ? WHERE uid = ? AND friend_uid = ? AND status = ?`,
		FriendStatusAccepted, timeNow(), fromUID, uid, FriendStatusPending,
	)
	if err != nil {
		return fmt.Errorf("accept friend request: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("no pending friend request from %s to %s", fromUID, uid)
	}
	return nil
}

// RejectRequest rejects a pending friend request.
func (s *MySQLStore) RejectRequest(ctx context.Context, uid, fromUID string) error {
	result, err := s.db.ExecContext(ctx,
		`UPDATE friends SET status = ?, updated_at = ? WHERE uid = ? AND friend_uid = ? AND status = ?`,
		FriendStatusRejected, timeNow(), fromUID, uid, FriendStatusPending,
	)
	if err != nil {
		return fmt.Errorf("reject friend request: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("no pending friend request from %s to %s", fromUID, uid)
	}
	return nil
}

// RemoveFriend removes a friend relationship (either direction).
func (s *MySQLStore) RemoveFriend(ctx context.Context, uid, friendUID string) error {
	result, err := s.db.ExecContext(ctx,
		`DELETE FROM friends WHERE ((uid = ? AND friend_uid = ?) OR (uid = ? AND friend_uid = ?)) AND status = ?`,
		uid, friendUID, friendUID, uid, FriendStatusAccepted,
	)
	if err != nil {
		return fmt.Errorf("remove friend: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("friendship not found between %s and %s", uid, friendUID)
	}
	return nil
}

// GetFriends returns all accepted friends for a user.
func (s *MySQLStore) GetFriends(ctx context.Context, uid string) ([]*Friend, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT uid, friend_uid, status, created_at FROM friends
		 WHERE (uid = ? OR friend_uid = ?) AND status = ?
		 ORDER BY created_at DESC`,
		uid, uid, FriendStatusAccepted,
	)
	if err != nil {
		return nil, fmt.Errorf("get friends: %w", err)
	}
	defer rows.Close()

	friends := make([]*Friend, 0)
	for rows.Next() {
		f := &Friend{}
		var status int32
		if err := rows.Scan(&f.UID, &f.FriendUID, &status, &f.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan friend: %w", err)
		}
		f.Status = FriendStatus(status)
		// Normalize: ensure the friend is always the "other" user.
		if f.UID == uid {
			f.UID, f.FriendUID = f.FriendUID, f.UID
		}
		friends = append(friends, f)
	}
	return friends, rows.Err()
}

// GetPendingRequests returns all incoming pending friend requests for a user.
func (s *MySQLStore) GetPendingRequests(ctx context.Context, uid string) ([]*FriendRequest, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT f.uid, f.created_at, COALESCE(u.username, f.uid) as username
		 FROM friends f
		 LEFT JOIN users u ON f.uid = u.uid
		 WHERE f.friend_uid = ? AND f.status = ?
		 ORDER BY f.created_at DESC`,
		uid, FriendStatusPending,
	)
	if err != nil {
		return nil, fmt.Errorf("get pending requests: %w", err)
	}
	defer rows.Close()

	requests := make([]*FriendRequest, 0)
	for rows.Next() {
		req := &FriendRequest{}
		if err := rows.Scan(&req.FromUID, &req.CreatedAt, &req.Username); err != nil {
			return nil, fmt.Errorf("scan request: %w", err)
		}
		requests = append(requests, req)
	}
	return requests, rows.Err()
}

// ---------- Admin UserStore methods ----------

// ListUsers returns a paginated list of all users ordered by creation time.
func (s *MySQLStore) ListUsers(ctx context.Context, offset, limit int) ([]*User, int, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}

	var total int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count users: %w", err)
	}

	rows, err := s.db.QueryContext(ctx,
		"SELECT uid, username, password_hash, role, is_disabled, created_at FROM users ORDER BY created_at DESC LIMIT ? OFFSET ?",
		limit, offset,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("list users: %w", err)
	}
	defer rows.Close()

	users := make([]*User, 0)
	for rows.Next() {
		u := &User{}
		if err := rows.Scan(&u.UID, &u.Username, &u.PasswordHash, &u.Role, &u.IsDisabled, &u.CreatedAt); err != nil {
			return nil, 0, fmt.Errorf("scan user: %w", err)
		}
		users = append(users, u)
	}
	return users, total, rows.Err()
}

// DeleteUser deletes a user by UID. Returns an error if no rows were affected.
func (s *MySQLStore) DeleteUser(ctx context.Context, uid string) error {
	result, err := s.db.ExecContext(ctx, "DELETE FROM users WHERE uid = ?", uid)
	if err != nil {
		return fmt.Errorf("delete user: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("user %s not found", uid)
	}
	return nil
}

// UpdateUserRole updates a user's role. Returns an error if no rows were affected.
func (s *MySQLStore) UpdateUserRole(ctx context.Context, uid, role string) error {
	result, err := s.db.ExecContext(ctx, "UPDATE users SET role = ? WHERE uid = ?", role, uid)
	if err != nil {
		return fmt.Errorf("update user role: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("user %s not found", uid)
	}
	return nil
}

// UpdateUserDisabled updates a user's disabled status. Returns an error if no rows were affected.
func (s *MySQLStore) UpdateUserDisabled(ctx context.Context, uid string, disabled bool) error {
	val := 0
	if disabled {
		val = 1
	}
	result, err := s.db.ExecContext(ctx, "UPDATE users SET is_disabled = ? WHERE uid = ?", val, uid)
	if err != nil {
		return fmt.Errorf("update user disabled: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("user %s not found", uid)
	}
	return nil
}

// CountUsers returns the total number of registered users.
func (s *MySQLStore) CountUsers(ctx context.Context) (int, error) {
	var count int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&count); err != nil {
		return 0, fmt.Errorf("count users: %w", err)
	}
	return count, nil
}

// ---------- Admin MessageStore methods ----------

// BrowseMessages returns recent messages globally (admin-only, no access control).
func (s *MySQLStore) BrowseMessages(ctx context.Context, before int64, limit int) ([]*proto.Message, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	if before <= 0 {
		before = time.Now().UnixMilli()
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT msg_id, seq, cmd, from_uid, to_uid, chat_type, msg_type, content, timestamp, need_ack, recalled
		 FROM messages WHERE timestamp < ? ORDER BY timestamp DESC LIMIT ?`,
		before, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("browse messages: %w", err)
	}
	defer rows.Close()

	msgs := make([]*proto.Message, 0)
	for rows.Next() {
		m := &proto.Message{}
		var needAck, recalled int
		if err := rows.Scan(
			&m.MsgId, &m.Seq, &m.Cmd, &m.From, &m.To,
			&m.ChatType, &m.MsgType, &m.Content, &m.Timestamp, &needAck, &recalled,
		); err != nil {
			return nil, fmt.Errorf("scan message: %w", err)
		}
		m.NeedAck = needAck == 1
		if recalled == 1 {
			m.Content = `{"recalled":true}`
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}

// DeleteMessage deletes a message by ID (admin only).
func (s *MySQLStore) DeleteMessage(ctx context.Context, msgID int64) error {
	result, err := s.db.ExecContext(ctx, "DELETE FROM messages WHERE msg_id = ?", msgID)
	if err != nil {
		return fmt.Errorf("delete message: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("message %d not found", msgID)
	}
	return nil
}

// CountMessages returns the total number of stored messages.
func (s *MySQLStore) CountMessages(ctx context.Context) (int, error) {
	var count int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM messages").Scan(&count); err != nil {
		return 0, fmt.Errorf("count messages: %w", err)
	}
	return count, nil
}

// timeNow returns the current unix millis. Extracted for testability.
func timeNow() int64 {
	return time.Now().UnixMilli()
}
