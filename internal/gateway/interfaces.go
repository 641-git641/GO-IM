package gateway

import (
	"context"

	"github.com/im/api/proto"
)

// ClientRegistry manages connection registration and lookup.
// Hub is the primary in-memory implementation. A Redis-backed
// implementation is available for OfflineStore (redis_store.go);
// ClientRegistry itself remains in-memory since connections are
// bound to the local Gateway process.
type ClientRegistry interface {
	Register(ctx context.Context, c *Client)
	Unregister(ctx context.Context, c *Client)
	Get(ctx context.Context, uid string) *Client
	IsOnline(ctx context.Context, uid string) bool
	OnlineUsers(ctx context.Context) []string
	Count(ctx context.Context) int
}

// OfflineStore stores messages for offline users and drains them on reconnect.
// Hub is the current in-memory implementation. See StoreOffline/DrainOffline
// for the implicit contract (FIFO, size-bounded, drain clears queue).
type OfflineStore interface {
	StoreOffline(ctx context.Context, uid string, msg *proto.Message)
	DrainOffline(ctx context.Context, uid string) []*proto.Message
}
