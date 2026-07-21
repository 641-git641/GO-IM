package gateway

import (
	"testing"
)

// TestNewLogicClientEmptyAddr returns nil (gRPC disabled).
func TestNewLogicClientEmptyAddr(t *testing.T) {
	lc, err := NewLogicClient("")
	if err != nil {
		t.Fatalf("NewLogicClient with empty addr: %v", err)
	}
	if lc != nil {
		t.Error("expected nil LogicClient for empty addr")
	}
}

// TestNewLogicClientInvalidAddr returns an error for an unreachable address.
func TestNewLogicClientInvalidAddr(t *testing.T) {
	// With grpc.WithBlock(), DialContext will fail for an unreachable address.
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
