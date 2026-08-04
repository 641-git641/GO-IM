package gateway

import (
	"context"
	"log"
	"sync"

	"github.com/im/api/proto"
)

// Hub 管理所有已连接客户端并在它们之间路由消息。
type Hub struct {
	mu      sync.RWMutex
	clients map[string]*Client // UID -> Client

	// offline 为离线用户存储消息:UID -> 消息队列
	offline        map[string][]*proto.Message
	offlineMu      sync.Mutex
	offlineMaxSize int // 每个用户的最大排队消息数

	maxConnections int // 0 = 无限制
}

// NewHub 创建一个新的 Hub。
func NewHub(offlineMaxSize int) *Hub {
	return &Hub{
		clients:        make(map[string]*Client),
		offline:        make(map[string][]*proto.Message),
		offlineMaxSize: offlineMaxSize,
	}
}

// SetMaxConnections 设置全局连接数上限。0 表示无限制。
func (h *Hub) SetMaxConnections(max int) {
	h.mu.Lock()
	h.maxConnections = max
	h.mu.Unlock()
}

// 服务器容量已满时,ErrServerFull 会以踢出消息的形式被返回。
const errServerFull = "server at capacity, please try later"

// Register 将客户端添加到 hub。
// 如果该 UID 已存在连接,旧连接在关闭前会收到踢出通知。
// 当服务器容量已满且这是新 UID(非替换现有连接)时,返回错误。
func (h *Hub) Register(ctx context.Context, c *Client) {
	h.mu.Lock()
	old, exists := h.clients[c.UID]

	// 强制连接数上限 —— 仅针对新 UID。
	if !exists && h.maxConnections > 0 && len(h.clients) >= h.maxConnections {
		h.mu.Unlock()
		// 拒绝:发送踢出消息并关闭连接(尽力而为)。
		c.Send(&proto.Message{
			Cmd:     proto.CmdKick,
			Content: errServerFull,
		})
		c.Close()
		log.Printf("[hub] connection rejected for %s: server at capacity (%d)", c.UID, h.maxConnections)
		return
	}

	// 在释放锁之前替换映射条目,以防竞态:
	// 否则另一个 goroutine 可能在 old.Close() 与写入映射之间注册不同客户端。
	h.clients[c.UID] = c
	h.mu.Unlock()

	// 现在可以在锁外安全地通知并关闭旧连接。
	// 这能防止网络 I/O 阻塞其他 Register/Unregister 调用。
	if exists {
		old.Send(&proto.Message{
			Cmd:     proto.CmdKick,
			Content: "logged in from another device",
		})
		old.Close()
	}
}

// Unregister 将客户端从 hub 中移除,但仅当映射条目是同一个 *Client 时。
// 这能防止过期连接的清理误删同一 UID 的新连接。
func (h *Hub) Unregister(ctx context.Context, c *Client) {
	h.mu.Lock()
	if existing, ok := h.clients[c.UID]; ok && existing == c {
		delete(h.clients, c.UID)
	}
	h.mu.Unlock()
}

// Get 返回指定 UID 的客户端,不存在时返回 nil。
func (h *Hub) Get(ctx context.Context, uid string) *Client {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.clients[uid]
}

// IsOnline 如果用户在线则返回 true。
func (h *Hub) IsOnline(ctx context.Context, uid string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, ok := h.clients[uid]
	return ok
}

// StoreOffline 为离线用户排队存储一条消息。
// 当队列超过配置上限时,丢弃最旧的消息并记录日志。
func (h *Hub) StoreOffline(ctx context.Context, uid string, msg *proto.Message) {
	h.offlineMu.Lock()
	h.offline[uid] = append(h.offline[uid], msg)
	if len(h.offline[uid]) > h.offlineMaxSize {
		dropped := len(h.offline[uid]) - h.offlineMaxSize
		h.offline[uid] = h.offline[uid][dropped:]
		log.Printf("[hub] offline queue truncated for %s: dropped %d oldest messages (limit=%d)", uid, dropped, h.offlineMaxSize)
	}
	h.offlineMu.Unlock()
}

// DrainOffline 返回并清除用户的离线消息。
func (h *Hub) DrainOffline(ctx context.Context, uid string) []*proto.Message {
	h.offlineMu.Lock()
	msgs := h.offline[uid]
	delete(h.offline, uid)
	h.offlineMu.Unlock()
	return msgs
}

// OnlineUsers 返回当前在线 UID 的列表。
func (h *Hub) OnlineUsers(ctx context.Context) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	uids := make([]string, 0, len(h.clients))
	for uid := range h.clients {
		uids = append(uids, uid)
	}
	return uids
}

// Count 返回已连接客户端的数量。
func (h *Hub) Count(ctx context.Context) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}
