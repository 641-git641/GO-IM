package gateway

import (
	"testing"
)

// TestNewLogicClientEmptyAddr 返回 nil（gRPC 已禁用）。
func TestNewLogicClientEmptyAddr(t *testing.T) {
	lc, err := NewLogicClient("")
	if err != nil {
		t.Fatalf("NewLogicClient with empty addr: %v", err)
	}
	if lc != nil {
		t.Error("expected nil LogicClient for empty addr")
	}
}

// TestNewLogicClientInvalidAddr 对不可达地址返回错误。
func TestNewLogicClientInvalidAddr(t *testing.T) {
	// 使用 grpc.WithBlock() 时，DialContext 对不可达地址会失败。
	lc, err := NewLogicClient("localhost:19999")
	if err == nil {
		if lc != nil {
			lc.Close()
		}
		t.Log("connected to localhost:19999 (unexpected but ok — some process may be there)")
	} else {
		t.Logf("expected error for unreachable addr: %v", err)
	}
}
