package logic

import (
	"context"
	"testing"

	"github.com/im/api/proto"
)

// TestNewServer constructs a server without panicking.
func TestNewServer(t *testing.T) {
	srv := NewServer(nil, 0)
	if srv == nil {
		t.Fatal("NewServer returned nil")
	}
}

// TestServerGetUserNilMySQL returns Found=false gracefully when mysql is nil.
func TestServerGetUserNilMySQL(t *testing.T) {
	srv := &Server{} // no mysql, no userRepo

	req := &proto.UserRequest{Uid: "alice"}
	resp, err := srv.GetUser(context.Background(), req)
	if err != nil {
		t.Fatalf("GetUser error: %v", err)
	}
	if resp.Found {
		t.Error("expected Found=false when mysql is nil")
	}
}

// TestServerGetUserNilRepo checks safety when userRepo is nil.
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

// TestServerQueryHistoryNilMySQL panics (expected — this requires MySQL).
func TestServerQueryHistoryRequiresMySQL(t *testing.T) {
	t.Skip("QueryHistory requires a real *repo.MySQLStore — tested via integration tests")
}
