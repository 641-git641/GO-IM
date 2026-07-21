package gateway

import (
	"context"
	"log"
	"sync"

	"github.com/im/api/proto"
)

// Hub manages all connected clients and routes messages between them.
type Hub struct {
	mu      sync.RWMutex
	clients map[string]*Client // UID -> Client

	// offline stores messages for offline users: UID -> message queue
	offline        map[string][]*proto.Message
	offlineMu      sync.Mutex
	offlineMaxSize int // max queued messages per user

	maxConnections int // 0 = unlimited
}

// NewHub creates a new Hub.
func NewHub(offlineMaxSize int) *Hub {
	return &Hub{
		clients:        make(map[string]*Client),
		offline:        make(map[string][]*proto.Message),
		offlineMaxSize: offlineMaxSize,
	}
}

// SetMaxConnections sets the global connection limit. 0 means unlimited.
func (h *Hub) SetMaxConnections(max int) {
	h.mu.Lock()
	h.maxConnections = max
	h.mu.Unlock()
}

// ErrServerFull is returned (as a kick message) when the server is at capacity.
const errServerFull = "server at capacity, please try later"

// Register adds a client to the hub.
// If a connection already exists for this UID, it is sent a kick notification
// before being closed. Returns an error if the server is at capacity and this
// is a new UID (not replacing an existing connection).
func (h *Hub) Register(ctx context.Context, c *Client) {
	h.mu.Lock()
	old, exists := h.clients[c.UID]

	// Enforce max connections — only for new UIDs.
	if !exists && h.maxConnections > 0 && len(h.clients) >= h.maxConnections {
		h.mu.Unlock()
		// Reject: send kick and close (best-effort).
		c.Send(&proto.Message{
			Cmd:     proto.CmdKick,
			Content: errServerFull,
		})
		c.Close()
		log.Printf("[hub] connection rejected for %s: server at capacity (%d)", c.UID, h.maxConnections)
		return
	}

	// Replace the map entry BEFORE releasing the lock to prevent a race
	// where another goroutine could register a different client between
	// old.Close() and the map write.
	h.clients[c.UID] = c
	h.mu.Unlock()

	// Now safe to notify and close the old connection outside the lock.
	// This prevents network I/O from blocking other Register/Unregister calls.
	if exists {
		old.Send(&proto.Message{
			Cmd:     proto.CmdKick,
			Content: "logged in from another device",
		})
		old.Close()
	}
}

// Unregister removes a client from the hub, but only if the map entry
// is the same *Client. This prevents a stale connection's cleanup from
// accidentally removing a newer connection for the same UID.
func (h *Hub) Unregister(ctx context.Context, c *Client) {
	h.mu.Lock()
	if existing, ok := h.clients[c.UID]; ok && existing == c {
		delete(h.clients, c.UID)
	}
	h.mu.Unlock()
}

// Get returns the client for the given UID, or nil.
func (h *Hub) Get(ctx context.Context, uid string) *Client {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.clients[uid]
}

// IsOnline returns true if the user is online.
func (h *Hub) IsOnline(ctx context.Context, uid string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, ok := h.clients[uid]
	return ok
}

// StoreOffline queues a message for an offline user.
// When the queue exceeds the configured max, oldest messages are dropped and logged.
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

// DrainOffline returns and clears offline messages for a user.
func (h *Hub) DrainOffline(ctx context.Context, uid string) []*proto.Message {
	h.offlineMu.Lock()
	msgs := h.offline[uid]
	delete(h.offline, uid)
	h.offlineMu.Unlock()
	return msgs
}

// OnlineUsers returns a list of currently online UIDs.
func (h *Hub) OnlineUsers(ctx context.Context) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	uids := make([]string, 0, len(h.clients))
	for uid := range h.clients {
		uids = append(uids, uid)
	}
	return uids
}

// Count returns the number of connected clients.
func (h *Hub) Count(ctx context.Context) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}
