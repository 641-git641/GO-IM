package gateway

import (
	"context"

	"github.com/im/api/proto"
)

// ClientRegistry 管理连接的注册与查询。
// Hub 是主要的内存实现。OfflineStore 有基于 Redis 的
// 实现(redis_store.go);ClientRegistry 本身仍为内存实现,
// 因为连接绑定于本地 Gateway 进程。
type ClientRegistry interface {
	Register(ctx context.Context, c *Client)
	Unregister(ctx context.Context, c *Client)
	Get(ctx context.Context, uid string) *Client
	IsOnline(ctx context.Context, uid string) bool
	OnlineUsers(ctx context.Context) []string
	Count(ctx context.Context) int
}

// OfflineStore 为离线用户存储消息,并在重连时取出。
// Hub 是当前的内存实现。隐式约定见 StoreOffline/DrainOffline
// (FIFO、大小受限、取出即清空队列)。
type OfflineStore interface {
	StoreOffline(ctx context.Context, uid string, msg *proto.Message)
	DrainOffline(ctx context.Context, uid string) []*proto.Message
}
