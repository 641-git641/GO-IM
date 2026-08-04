package logic

import (
	"context"
	"testing"

	"github.com/im/api/proto"
)

// TestNewServer 构造服务器而不触发 panic。
func TestNewServer(t *testing.T) {
	srv := NewServer(nil, 0)
	if srv == nil {
		t.Fatal("NewServer returned nil")
	}
}

// TestServerGetUserNilMySQL 在 mysql 为 nil 时优雅地返回 Found=false。
func TestServerGetUserNilMySQL(t *testing.T) {
	srv := &Server{} // 没有 mysql，没有 userRepo

	req := &proto.UserRequest{Uid: "alice"}
	resp, err := srv.GetUser(context.Background(), req)
	if err != nil {
		t.Fatalf("GetUser error: %v", err)
	}
	if resp.Found {
		t.Error("expected Found=false when mysql is nil")
	}
}

// TestServerGetUserNilRepo 检查 userRepo 为 nil 时的安全性。
func TestServerGetUserNilRepo(t *testing.T) {
	srv := &Server{userRepo: nil}

	req := &proto.UserRequest{Uid: "nonexistent"}
	resp, err := srv.GetUser(context.Background(), req)
	if err != nil {
		t.Fatalf("GetUser error: %v", err)
	}
	if resp.Found {
		t.Error("expected Found=false for nonexistent user with nil repo")
	}
}

// TestServerQueryHistoryNilMySQL 会 panic（预期行为 —— 这需要 MySQL）。
func TestServerQueryHistoryRequiresMySQL(t *testing.T) {
	t.Skip("QueryHistory requires a real *repo.MySQLStore — tested via integration tests")
}
