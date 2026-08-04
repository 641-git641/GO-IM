package repo

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/im/api/proto"

	_ "github.com/go-sql-driver/mysql"
)

// mysqlDSN 返回测试用 MySQL DSN，如果 MySQL 不可用则返回空字符串。
func mysqlDSN() string {
	dsn := os.Getenv("MYSQL_TEST_DSN")
	if dsn == "" {
		dsn = "im:im-dev@tcp(127.0.0.1:3306)/im?parseTime=true&charset=utf8mb4"
	}
	return dsn
}

// newTestMySQLStore 创建用于测试的 MySQLStore，不可用时跳过测试。
func newTestMySQLStore(t *testing.T) *MySQLStore {
	t.Helper()

	dsn := mysqlDSN()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Skipf("cannot open MySQL: %v", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		t.Skipf("MySQL not available at %s: %v (start with: docker-compose up -d mysql)", dsn, err)
	}
	db.Close()

	s, err := NewMySQLStore(dsn)
	if err != nil {
		t.Fatalf("NewMySQLStore: %v", err)
	}
	return s
}

// truncateAll 从两张表中删除测试数据。
func truncateAll(t *testing.T, s *MySQLStore) {
	t.Helper()
	for _, table := range []string{"messages", "users"} {
		if _, err := s.db.Exec("DELETE FROM " + table); err != nil {
			t.Logf("truncate %s: %v", table, err)
		}
	}
}

// ---------- 用户测试 ----------

func TestMySQLUserCreateAndGet(t *testing.T) {
	s := newTestMySQLStore(t)
	defer s.Close()
	truncateAll(t, s)

	ctx := context.Background()
	u := &User{
		UID:          "testuser1",
		Username:     "Test User",
		PasswordHash: "$2a$10$dummyhash",
		CreatedAt:    1721318400000,
	}

	if err := s.Create(ctx, u); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := s.GetByUID(ctx, "testuser1")
	if err != nil {
		t.Fatalf("GetByUID: %v", err)
	}
	if got == nil {
		t.Fatal("expected user, got nil")
	}
	if got.UID != u.UID {
		t.Errorf("UID: expected %s, got %s", u.UID, got.UID)
	}
	if got.Username != u.Username {
		t.Errorf("Username: expected %s, got %s", u.Username, got.Username)
	}
	if got.PasswordHash != u.PasswordHash {
		t.Errorf("PasswordHash: expected %s, got %s", u.PasswordHash, got.PasswordHash)
	}
}

func TestMySQLUserCreateDuplicate(t *testing.T) {
	s := newTestMySQLStore(t)
	defer s.Close()
	truncateAll(t, s)

	ctx := context.Background()
	u := &User{UID: "dupuser", Username: "Dup", PasswordHash: "hash", CreatedAt: 1}

	if err := s.Create(ctx, u); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	if err := s.Create(ctx, u); err == nil {
		t.Error("expected duplicate key error, got nil")
	}
}

func TestMySQLUserNotFound(t *testing.T) {
	s := newTestMySQLStore(t)
	defer s.Close()

	got, err := s.GetByUID(context.Background(), "nonexistent")
	if err != nil {
		t.Fatalf("GetByUID: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for nonexistent user, got %+v", got)
	}
}

// ---------- 消息测试 ----------

func TestMySQLMessageSaveAndQuery(t *testing.T) {
	s := newTestMySQLStore(t)
	defer s.Close()
	truncateAll(t, s)

	ctx := context.Background()
	now := int64(1721318400000)

	// 保存 alice 与 bob 之间的 3 条消息。
	msgs := []*proto.Message{
		{MsgId: 1001, Seq: 1, Cmd: proto.CmdChat, From: "alice", To: "bob", ChatType: proto.ChatTypeSingle, MsgType: proto.MsgTypeText, Content: "Hello Bob!", Timestamp: now, NeedAck: true},
		{MsgId: 1002, Seq: 1, Cmd: proto.CmdChat, From: "bob", To: "alice", ChatType: proto.ChatTypeSingle, MsgType: proto.MsgTypeText, Content: "Hi Alice!", Timestamp: now + 1000, NeedAck: true},
		{MsgId: 1003, Seq: 2, Cmd: proto.CmdChat, From: "alice", To: "bob", ChatType: proto.ChatTypeSingle, MsgType: proto.MsgTypeText, Content: "How are you?", Timestamp: now + 2000, NeedAck: false},
	}

	for _, m := range msgs {
		if err := s.Save(ctx, m); err != nil {
			t.Fatalf("Save msg %d: %v", m.MsgId, err)
		}
	}

	// 查询 alice 与 bob 之间的历史记录（全部消息，按从新到旧排序）。
	history, err := s.QueryHistory(ctx, "alice", "bob", now+10000, 50)
	if err != nil {
		t.Fatalf("QueryHistory: %v", err)
	}
	if len(history) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(history))
	}

	// 从新到旧：msg 1003, 1002, 1001。
	if history[0].MsgId != 1003 {
		t.Errorf("history[0]: expected msgId=1003, got %d", history[0].MsgId)
	}
	if history[1].MsgId != 1002 {
		t.Errorf("history[1]: expected msgId=1002, got %d", history[1].MsgId)
	}
	if history[2].MsgId != 1001 {
		t.Errorf("history[2]: expected msgId=1001, got %d", history[2].MsgId)
	}

	// 验证内容可以正确往返。
	if history[0].Content != "How are you?" {
		t.Errorf("content: expected 'How are you?', got '%s'", history[0].Content)
	}
	if history[0].NeedAck != false {
		t.Error("msg 1003 should have NeedAck=false")
	}
	if history[2].NeedAck != true {
		t.Error("msg 1001 should have NeedAck=true")
	}
}

func TestMySQLQueryHistoryPagination(t *testing.T) {
	s := newTestMySQLStore(t)
	defer s.Close()
	truncateAll(t, s)

	ctx := context.Background()
	now := int64(1721318400000)

	// 保存 5 条消息。
	for i := 0; i < 5; i++ {
		from := "alice"
		to := "bob"
		if i%2 == 1 {
			from, to = to, from
		}
		if err := s.Save(ctx, &proto.Message{
			MsgId: int64(2000 + i), Seq: int64(i + 1),
			Cmd: proto.CmdChat, From: from, To: to,
			ChatType: proto.ChatTypeSingle, MsgType: proto.MsgTypeText,
			Content:   fmt.Sprintf("msg %d", i),
			Timestamp: now + int64(i*1000),
		}); err != nil {
			t.Fatalf("Save msg %d: %v", i, err)
		}
	}

	// 以 limit 3 查询早于最新时间戳的消息。
	history, err := s.QueryHistory(ctx, "alice", "bob", now+10000, 3)
	if err != nil {
		t.Fatalf("QueryHistory: %v", err)
	}
	if len(history) != 3 {
		t.Fatalf("expected 3 messages (limit), got %d", len(history))
	}

	// 分页：应返回最新的 3 条消息。
	// 消息为：2004（最新）、2003、2002
	if history[0].MsgId != 2004 {
		t.Errorf("history[0]: expected msgId=2004, got %d", history[0].MsgId)
	}
	if history[2].MsgId != 2002 {
		t.Errorf("history[2]: expected msgId=2002, got %d", history[2].MsgId)
	}

	// 现在查询早于 msg 2002 时间戳（now+1000）的消息。
	history2, err := s.QueryHistory(ctx, "alice", "bob", now+1000, 50)
	if err != nil {
		t.Fatalf("QueryHistory (before): %v", err)
	}
	if len(history2) != 1 {
		t.Fatalf("expected 1 message before now+1000, got %d", len(history2))
	}
	if history2[0].MsgId != 2000 {
		t.Errorf("expected msgId=2000, got %d", history2[0].MsgId)
	}
}

func TestMySQLQueryHistoryEmpty(t *testing.T) {
	s := newTestMySQLStore(t)
	defer s.Close()
	truncateAll(t, s)

	history, err := s.QueryHistory(context.Background(), "alice", "bob", 9999999999999, 50)
	if err != nil {
		t.Fatalf("QueryHistory: %v", err)
	}
	if len(history) != 0 {
		t.Errorf("expected empty history, got %d messages", len(history))
	}
}

func TestMySQLQueryHistoryOtherUserNotIncluded(t *testing.T) {
	s := newTestMySQLStore(t)
	defer s.Close()
	truncateAll(t, s)

	ctx := context.Background()
	now := int64(1721318400000)

	// Alice-Bob 会话。
	s.Save(ctx, &proto.Message{
		MsgId: 3001, Seq: 1, Cmd: proto.CmdChat,
		From: "alice", To: "bob", ChatType: proto.ChatTypeSingle, MsgType: proto.MsgTypeText,
		Content: "AB", Timestamp: now,
	})
	// Alice-Carol 会话（不应出现在 alice-bob 查询中）。
	s.Save(ctx, &proto.Message{
		MsgId: 3002, Seq: 1, Cmd: proto.CmdChat,
		From: "alice", To: "carol", ChatType: proto.ChatTypeSingle, MsgType: proto.MsgTypeText,
		Content: "AC", Timestamp: now + 1000,
	})

	history, err := s.QueryHistory(ctx, "alice", "bob", now+10000, 50)
	if err != nil {
		t.Fatalf("QueryHistory: %v", err)
	}
	if len(history) != 1 {
		t.Fatalf("expected 1 message (alice-bob only), got %d", len(history))
	}
	if history[0].Content != "AB" {
		t.Errorf("expected 'AB', got '%s'", history[0].Content)
	}
}
