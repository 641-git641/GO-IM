package repo

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/im/api/proto"

	_ "github.com/go-sql-driver/mysql" // MySQL 驱动注册
)

// 编译期接口合规性检查。
var (
	_ UserStore    = (*MySQLStore)(nil)
	_ MessageStore = (*MySQLStore)(nil)
	_ FriendStore  = (*MySQLStore)(nil)
)

// MySQLStore 基于 MySQL 实现 UserStore 与 MessageStore。
type MySQLStore struct {
	db *sql.DB
}

// NewMySQLStore 打开 MySQL 连接，验证连接并创建表。
// 调用方负责通过 Close() 关闭存储。
func NewMySQLStore(dsn string) (*MySQLStore, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("mysql open: %w", err)
	}

	// 连接池设置。
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

// DB 返回底层的 *sql.DB，供共享同一连接池的其他 MySQL 存储
// （例如 MySQLGroupStore）使用。
func (s *MySQLStore) DB() *sql.DB {
	return s.db
}

// Ping 检查数据库连接的健康状况。
func (s *MySQLStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// Close 释放数据库连接池。
func (s *MySQLStore) Close() error {
	return s.db.Close()
}

// migrate 在表不存在时创建表。
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

	// 为消息搜索添加 FULLTEXT 索引（尽力而为）。
	ftsDDL := `ALTER TABLE messages ADD FULLTEXT INDEX ft_content (content) WITH PARSER ngram`
	if _, err := s.db.Exec(ftsDDL); err != nil {
		// MySQL 错误 1061 = 索引名重复 —— 可以安全忽略。
		// 错误 1128/1214 = 不支持 ngram 解析器或其他 schema 问题 —— 记录为警告。
		if strings.Contains(err.Error(), "Error 1061") {
			log.Printf("[mysql] FULLTEXT index already exists (this is OK)")
		} else {
			log.Printf("[mysql] WARNING: FULLTEXT index creation failed (search may not work): %v", err)
		}
	}

	// 为现有数据库添加 recalled 列（尽力而为的迁移）。
	recallDDL := `ALTER TABLE messages ADD COLUMN recalled TINYINT NOT NULL DEFAULT 0`
	if _, err := s.db.Exec(recallDDL); err != nil {
		if strings.Contains(err.Error(), "Error 1060") {
			log.Printf("[mysql] recalled column already exists (this is OK)")
		} else {
			log.Printf("[mysql] WARNING: recall column migration failed (recall may not work): %v", err)
		}
	}

	// 为管理员用户添加 role 列（尽力而为的迁移）。
	roleDDL := `ALTER TABLE users ADD COLUMN role VARCHAR(16) NOT NULL DEFAULT 'user'`
	if _, err := s.db.Exec(roleDDL); err != nil {
		if strings.Contains(err.Error(), "Error 1060") {
			log.Printf("[mysql] role column already exists (this is OK)")
		} else {
			log.Printf("[mysql] WARNING: role column migration failed: %v", err)
		}
	}

	// 添加 is_disabled 列用于禁用用户（尽力而为的迁移）。
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

// ---------- UserStore 实现 ----------

// Create 插入一个新用户。如果 UID 已存在则返回错误。
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

// UpdatePassword 更新用户的密码哈希。
// 如果 UID 不存在则返回错误。
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

// GetByUID 按 UID 获取用户。未找到时返回 nil。
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

// ---------- MessageStore 实现 ----------

// Save 将一条聊天消息持久化到消息历史记录。
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

// RecallMessage 通过设置内容与 recalled 标记将消息标记为已撤回。
// 只有原始发送者可以撤回自己的消息，且只能在撤回时间窗口内
// （自消息发送起 recallWindowMs 毫秒）。如果没有匹配的行则返回错误。
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

// UpdateMessageContent 更新已存储消息的内容。
// 只有作者（fromUID）可以编辑自己的消息。
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

// QueryHistory 返回两个用户之间的消息，按时间从新到旧排序。
// before 是毫秒级 Unix 时间戳；只返回早于该时间戳的消息。
// limit 限制结果数量（最大 200）。
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

// QueryGroupHistory 返回发送到群组的消息，按时间从新到旧排序。
// before 是毫秒级 Unix 时间戳；只返回早于该时间戳的消息。
// limit 限制结果数量（最大 200）。
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

// SearchMessages 对消息内容执行全文搜索。
// 访问控制：只返回 uid 作为参与者（from 或 to）的消息。
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

	// 校验并清理查询 —— 空查询或纯空白查询返回空结果。
	queryText := strings.TrimSpace(params.Query)
	if queryText == "" {
		return &SearchResult{}, nil
	}
	// 转义 BOOLEAN MODE 特殊字符以防止查询注入。
	queryText = escapeBooleanMode(queryText)

	// 根据可选过滤条件动态构建 WHERE 子句。
	query := `SELECT msg_id, seq, cmd, from_uid, to_uid, chat_type, msg_type, content, timestamp, need_ack, recalled
		FROM messages
		WHERE MATCH(content) AGAINST(? IN BOOLEAN MODE)`

	args := []interface{}{queryText}

	// 访问控制：用户必须是 from_uid 或 to_uid。
	if params.Peer != "" {
		// 限定到特定会话。
		query += ` AND ((from_uid = ? AND to_uid = ?) OR (from_uid = ? AND to_uid = ?))`
		args = append(args, params.UID, params.Peer, params.Peer, params.UID)
	} else {
		// 用户参与的所有会话。
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
	args = append(args, limit+1) // 多取一条以检测是否还有更多页

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

// escapeBooleanMode 转义 MySQL BOOLEAN MODE 特殊字符
// 以防止查询注入。字符：+ - > < ( ) ~ * " @
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

// ---------- FriendStore 实现 ----------

// SendRequest 发送一条从 fromUID 到 toUID 的好友请求。
func (s *MySQLStore) SendRequest(ctx context.Context, fromUID, toUID string) error {
	now := timeNow()
	// 以 status=pending 插入。UNIQUE KEY 防止重复请求。
	// 反向已存在的请求也会被捕获。
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

// AcceptRequest 接受一条待处理的好友请求。
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

// RejectRequest 拒绝一条待处理的好友请求。
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

// RemoveFriend 移除一条好友关系（任意方向）。
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

// GetFriends 返回一个用户的所有已接受好友。
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
		// 规范化：确保好友始终是"对方"用户。
		if f.UID == uid {
			f.UID, f.FriendUID = f.FriendUID, f.UID
		}
		friends = append(friends, f)
	}
	return friends, rows.Err()
}

// GetPendingRequests 返回一个用户收到的所有待处理好友请求。
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

// ---------- 管理员 UserStore 方法 ----------

// ListUsers 返回按创建时间排序的用户分页列表。
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

// DeleteUser 按 UID 删除用户。如果没有行受影响则返回错误。
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

// userExists 检查用户是否存在。
// 供 UpdateUserRole / UpdateUserDisabled 使用:MySQL 默认语义下
// UPDATE 值不变时 RowsAffected=0,需借此区分"用户不存在"与"幂等更新成功"。
func (s *MySQLStore) userExists(ctx context.Context, uid string) (bool, error) {
	var n int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users WHERE uid = ?", uid).Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}

// UpdateUserRole 更新用户的角色。如果用户不存在则返回错误。
// WHERE 附加 AND role <> ?:排除"已是目标角色"的幂等更新,
// 避免 MySQL 相同值更新时 RowsAffected=0 误报用户不存在。
func (s *MySQLStore) UpdateUserRole(ctx context.Context, uid, role string) error {
	result, err := s.db.ExecContext(ctx, "UPDATE users SET role = ? WHERE uid = ? AND role <> ?", role, uid, role)
	if err != nil {
		return fmt.Errorf("update user role: %w", err)
	}
	if n, _ := result.RowsAffected(); n == 0 {
		ok, err := s.userExists(ctx, uid)
		if err != nil {
			return fmt.Errorf("check user %s: %w", uid, err)
		}
		if !ok {
			return fmt.Errorf("user %s not found", uid)
		}
	}
	return nil
}

// UpdateUserDisabled 更新用户的禁用状态。如果用户不存在则返回错误。
// 与 UpdateUserRole 相同,附加 AND is_disabled <> ? 排除幂等更新误报。
func (s *MySQLStore) UpdateUserDisabled(ctx context.Context, uid string, disabled bool) error {
	val := 0
	if disabled {
		val = 1
	}
	result, err := s.db.ExecContext(ctx, "UPDATE users SET is_disabled = ? WHERE uid = ? AND is_disabled <> ?", val, uid, val)
	if err != nil {
		return fmt.Errorf("update user disabled: %w", err)
	}
	if n, _ := result.RowsAffected(); n == 0 {
		ok, err := s.userExists(ctx, uid)
		if err != nil {
			return fmt.Errorf("check user %s: %w", uid, err)
		}
		if !ok {
			return fmt.Errorf("user %s not found", uid)
		}
	}
	return nil
}

// CountUsers 返回已注册用户的总数。
func (s *MySQLStore) CountUsers(ctx context.Context) (int, error) {
	var count int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&count); err != nil {
		return 0, fmt.Errorf("count users: %w", err)
	}
	return count, nil
}

// ---------- 管理员 MessageStore 方法 ----------

// BrowseMessages 全局返回最近的消息（仅管理员，无访问控制）。
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

// DeleteMessage 按 ID 删除一条消息（仅管理员）。
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

// CountMessages 返回已存储消息的总数。
func (s *MySQLStore) CountMessages(ctx context.Context) (int, error) {
	var count int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM messages").Scan(&count); err != nil {
		return 0, fmt.Errorf("count messages: %w", err)
	}
	return count, nil
}

// timeNow 返回当前毫秒级 Unix 时间戳。为可测试性而提取。
func timeNow() int64 {
	return time.Now().UnixMilli()
}
