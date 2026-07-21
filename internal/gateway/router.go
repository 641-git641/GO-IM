package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/im/api/proto"
	"github.com/im/internal/mq"
	"github.com/im/internal/pkg/snowflake"
	"github.com/im/internal/repo"
	pbproto "google.golang.org/protobuf/proto"
)

// Router handles message routing logic.
type Router struct {
	clients     ClientRegistry
	offline     OfflineStore
	snow        *snowflake.Generator
	dedup       *DedupCache
	rateLimit   *RateLimiter
	rateLimitMu sync.Mutex   // guards rateLimit field during SetRateLimit reconfiguration
	msgStore    repo.MessageStore // nil when MySQL disabled
	kafka       *mq.Producer    // nil when Kafka disabled
	logicClient *LogicClient      // nil when gRPC Logic service disabled

	// Multi-gateway horizontal scaling (nil/empty = single-node mode).
	hashRing   *HashRing  // nil when multi-gateway disabled
	forwarder  Forwarder  // nil when multi-gateway disabled
	thisNodeID string     // "" when multi-gateway disabled

	// Group chat support (nil = group chat disabled).
	groupStore GroupStore // nil when group chat not initialized

	// Read/unread receipt tracking (nil = tracking disabled).
	unreadTracker UnreadTracker // nil when unread tracking not initialized

	// Friend relationship management (nil = friend system disabled).
	friendStore repo.FriendStore // nil when MySQL disabled

	// persistSem bounds concurrent async persistence goroutines to prevent
	// unbounded goroutine growth under high message throughput.
	persistSem chan struct{}

	// Configurable operational parameters (previously hardcoded constants).
	recallWindow int64         // message recall window in milliseconds
	historyLimit int           // default history page size
	searchLimit  int           // default search result limit
	rlCleanup    time.Duration // rate limiter stale bucket cleanup interval
}

// SetKafkaProducer injects a Kafka producer for async message persistence.
// When nil (the default), Kafka is not used.
func (r *Router) SetKafkaProducer(p *mq.Producer) {
	r.kafka = p
}

// SetLogicClient injects a gRPC client for the Logic service (history, user queries).
// When nil (the default), the local MessageStore is used.
func (r *Router) SetLogicClient(c *LogicClient) {
	r.logicClient = c
}

// SetHashRing injects the consistent hash ring for multi-gateway routing.
// When nil (the default), all messages are delivered or stored locally.
func (r *Router) SetHashRing(hr *HashRing) {
	r.hashRing = hr
}

// SetForwarder injects the cross-gateway message forwarder.
// When nil (the default), the Router does not attempt to forward to peers.
func (r *Router) SetForwarder(f Forwarder) {
	r.forwarder = f
}

// SetThisNodeID sets the local Gateway's node ID for hash ring comparisons.
func (r *Router) SetThisNodeID(id string) {
	r.thisNodeID = id
}

// SetGroupStore injects a GroupStore for group chat message fan-out.
// When nil (the default), group chat messages are treated as single-chat.
func (r *Router) SetGroupStore(gs GroupStore) {
	r.groupStore = gs
}

// SetUnreadTracker injects an UnreadTracker for read receipt and unread count support.
// When nil (the default), unread tracking is disabled.
func (r *Router) SetUnreadTracker(ut UnreadTracker) {
	r.unreadTracker = ut
}

// SetDedupRedis enables Redis-backed durability for the dedup cache.
// When nil (the default), dedup is memory-only. Call during startup before
// the server starts accepting connections.
func (r *Router) SetDedupRedis(rdb *redis.Client) {
	r.dedup.SetRedis(rdb)
}

// SetFriendStore injects a FriendStore for friend request/response handling.
// When nil (the default), friend management is unavailable.
func (r *Router) SetFriendStore(fs repo.FriendStore) {
	r.friendStore = fs
}

// RouterConfig holds tunable operational parameters for the Router.
// These were previously hardcoded constants; they are now configurable.
type RouterConfig struct {
	DedupTTL            time.Duration // dedup cache entry TTL, default 5m
	PersistConcurrency  int           // max concurrent async persist goroutines, default 64
	RecallWindowMs      int64         // message recall window in milliseconds, default 120000
	HistoryDefaultLimit int           // default history page size, default 30
	SearchDefaultLimit  int           // default search result limit, default 20
	RateLimitCleanup    time.Duration // rate limiter stale bucket cleanup interval, default 5m
}

// DefaultRouterConfig returns sensible defaults for RouterConfig.
func DefaultRouterConfig() RouterConfig {
	return RouterConfig{
		DedupTTL:            5 * time.Minute,
		PersistConcurrency:  64,
		RecallWindowMs:      120_000,
		HistoryDefaultLimit: 30,
		SearchDefaultLimit:  20,
		RateLimitCleanup:    5 * time.Minute,
	}
}

// NewRouter creates a Router. Rate limiting is set separately via SetRateLimitConfig.
func NewRouter(clients ClientRegistry, offline OfflineStore, snow *snowflake.Generator, msgStore repo.MessageStore, cfg RouterConfig) *Router {
	if cfg.PersistConcurrency <= 0 {
		cfg.PersistConcurrency = 64
	}
	if cfg.DedupTTL <= 0 {
		cfg.DedupTTL = 5 * time.Minute
	}
	return &Router{
		clients:      clients,
		offline:      offline,
		snow:         snow,
		dedup:        NewDedupCache(cfg.DedupTTL),
		msgStore:     msgStore,
		persistSem:   make(chan struct{}, cfg.PersistConcurrency),
		recallWindow: cfg.RecallWindowMs,
		historyLimit: cfg.HistoryDefaultLimit,
		searchLimit:  cfg.SearchDefaultLimit,
		rlCleanup:    cfg.RateLimitCleanup,
	}
}

// SetRateLimit configures rate limiting. When rate <= 0, rate limiting is disabled.
// Stops any previously running RateLimiter to prevent goroutine leaks.
func (r *Router) SetRateLimit(rate, burst int) {
	r.rateLimitMu.Lock()
	defer r.rateLimitMu.Unlock()

	// Stop old limiter to prevent goroutine leak.
	if r.rateLimit != nil {
		r.rateLimit.Stop()
	}

	if rate > 0 {
		if burst <= 0 {
			burst = rate * 2
		}
		r.rateLimit = NewRateLimiter(rate, burst, r.rlCleanup)
	} else {
		r.rateLimit = nil
	}
}

// Search performs a fulltext search via the local MessageStore.
// Returns nil, nil if no MessageStore is configured.
func (r *Router) Search(ctx context.Context, params *repo.SearchParams) (*repo.SearchResult, error) {
	if r.msgStore == nil {
		return nil, nil
	}
	return r.msgStore.SearchMessages(ctx, params)
}

// Stop gracefully stops background goroutines owned by the router
// (dedup cache cleanup, rate limiter cleanup).
func (r *Router) Stop() {
	if r.dedup != nil {
		r.dedup.Stop()
	}
	r.rateLimitMu.Lock()
	if r.rateLimit != nil {
		r.rateLimit.Stop()
	}
	r.rateLimitMu.Unlock()
}

// checkRateLimit returns true if the user exceeded the rate limit.
// Thread-safe: acquires rateLimitMu to read the rate limiter pointer.
func (r *Router) checkRateLimit(uid string) bool {
	r.rateLimitMu.Lock()
	rl := r.rateLimit
	r.rateLimitMu.Unlock()
	if rl != nil {
		return !rl.Allow(uid)
	}
	return false
}

// persistAsync asynchronously persists a message to Kafka or MySQL.
// It is a non-blocking, best-effort operation — failures are logged but never
// propagated to the caller. A semaphore bounds concurrent persist goroutines.
func (r *Router) persistAsync(ctx context.Context, msg *proto.Message) {
	// Kafka async persistence (fire-and-forget).
	if r.kafka != nil {
		msgCopy := pbproto.Clone(msg).(*proto.Message)
		log.Printf("[router] persistAsync: publishing msgId=%d cmd=%d via Kafka", msg.MsgId, msg.Cmd)
		go func() {
			r.persistSem <- struct{}{}
			defer func() { <-r.persistSem }()
			r.kafka.Publish(context.WithoutCancel(ctx), msgCopy)
		}()
	}
	// Direct MySQL persistence as safety net (always attempted when available).
	// This ensures messages survive even when Kafka has issues.
	if r.msgStore != nil {
		msgCopy := pbproto.Clone(msg).(*proto.Message)
		log.Printf("[router] persistAsync: dispatching msgId=%d to MySQL (from=%s to=%s cmd=%d)", msg.MsgId, msg.From, msg.To, msg.Cmd)
		go func() {
			r.persistSem <- struct{}{}
			defer func() { <-r.persistSem }()
			if err := r.msgStore.Save(context.WithoutCancel(ctx), msgCopy); err != nil {
				log.Printf("[router] mysql save FAIL for msgId=%d: %v", msgCopy.MsgId, err)
			} else {
				log.Printf("[router] mysql save OK for msgId=%d", msgCopy.MsgId)
			}
		}()
	}
	if r.kafka == nil && r.msgStore == nil {
		log.Printf("[router] persistAsync: no persistence backend configured (kafka=%v msgStore=%v)", r.kafka != nil, r.msgStore != nil)
	}
}

// Route dispatches an incoming message from a client.
func (r *Router) Route(ctx context.Context, sender *Client, msg *proto.Message) {
	// Reject uninitialized or out-of-range commands early. This catches the
	// protobuf zero value (CmdNone=0) which would otherwise be silently dropped.
	if err := msg.Validate(); err != nil {
		log.Printf("[router] invalid message from %s: %v (cmd=%d)", sender.UID, err, msg.Cmd)
		return
	}

	switch msg.Cmd {
	case proto.CmdNone:
		// Should be unreachable — Validate() rejects CmdNone. Keep as a defensive
		// fallback for messages that bypass validation (e.g. internal construction).
		log.Printf("[router] received CmdNone (uninitialized message) from %s — this should not happen", sender.UID)
	case proto.CmdHeartbeat:
		r.handleHeartbeat(ctx, sender, msg)
	case proto.CmdChat:
		r.handleChat(ctx, sender, msg)
	case proto.CmdFile:
		r.handleChat(ctx, sender, msg) // file messages go through the full chat pipeline
	case proto.CmdOffline:
		r.handleOffline(ctx, sender)
	case proto.CmdHistory:
		r.handleHistory(ctx, sender, msg)
	case proto.CmdReadReceipt:
		r.handleReadReceipt(ctx, sender, msg)
	case proto.CmdUnreadCount:
		r.handleUnreadCount(ctx, sender, msg)
	case proto.CmdSearch:
		r.handleSearch(ctx, sender, msg)
	case proto.CmdGroupCreate:
		r.handleGroupCreate(ctx, sender, msg)
	case proto.CmdGroupJoin:
		r.handleGroupJoin(ctx, sender, msg)
	case proto.CmdGroupInviteMember:
		r.handleGroupInviteMember(ctx, sender, msg)
	case proto.CmdGroupLeave:
		r.handleGroupLeave(ctx, sender, msg)
	case proto.CmdGroupInfo:
		r.handleGroupInfo(ctx, sender, msg)
	case proto.CmdGroupList:
		r.handleGroupList(ctx, sender, msg)
	case proto.CmdRecall:
		r.handleRecall(ctx, sender, msg)
	case proto.CmdFriendRequest:
		r.handleFriendRequest(ctx, sender, msg)
	case proto.CmdFriendResponse:
		r.handleFriendResponse(ctx, sender, msg)
	case proto.CmdTyping:
		r.handleTyping(ctx, sender, msg)
	case proto.CmdForward:
		r.handleForward(ctx, sender, msg)
	case proto.CmdEdit:
		r.handleEdit(ctx, sender, msg)
	case proto.CmdKick:
		// Server-initiated only; clients should not send this
		log.Printf("[router] unexpected CmdKick from %s — ignored", sender.UID)
	default:
		log.Printf("[router] unknown cmd=%d from=%s", msg.Cmd, sender.UID)
	}
}

func (r *Router) handleHeartbeat(ctx context.Context, sender *Client, msg *proto.Message) {
	resp := &proto.Message{
		Cmd:       proto.CmdHeartbeat,
		MsgId:     r.snow.Next(),
		Timestamp: time.Now().UnixMilli(),
	}
	sender.Send(resp)
}

// handleRecall processes a message recall request. The sender asks to delete a
// previously sent message. Seq carries the original message's MsgID.
// Only single-chat recall is supported; the sender must be the original author
// and the message must be within the 2-minute recall window.
func (r *Router) handleRecall(ctx context.Context, sender *Client, msg *proto.Message) {
	// Validate basic fields (Cmd range, target required).
	if err := msg.Validate(); err != nil {
		log.Printf("[router] recall from %s invalid: %v", sender.UID, err)
		return
	}
	// Self-recall is not allowed.
	if msg.To == sender.UID {
		log.Printf("[router] recall from %s dropped: self-target", sender.UID)
		return
	}
	// Seq must carry the original message's MsgID.
	if msg.Seq == 0 {
		log.Printf("[router] recall from %s dropped: missing original MsgId (seq=0)", sender.UID)
		r.sendRecallError(sender, "missing original message ID")
		return
	}

	// Security: overwrite From with the authenticated sender UID.
	msg.From = sender.UID


	// Mark the original message as recalled in the persistent store.
	// The recall window is enforced by the store layer via the message timestamp.
	if r.msgStore != nil {
		if err := r.msgStore.RecallMessage(ctx, msg.Seq, sender.UID, r.recallWindow); err != nil {
			log.Printf("[router] recall from %s for msg=%d failed: %v", sender.UID, msg.Seq, err)
			r.sendRecallError(sender, err.Error())
			return
		}
	}

	// Assign a message ID and timestamp for the recall notification.
	msg.MsgId = r.snow.Next()
	msg.Timestamp = time.Now().UnixMilli()

	// Build the recall notification for the target peer.
	// The Seq field carries the original message's MsgID so the client knows which
	// message to remove. Content is intentionally empty — the CmdRecall itself is
	// the signal.
	msg.Content = fmt.Sprintf(`{"recalled":true,"msg_id":%d}`, msg.Seq)

	// Try local delivery first.
	target := r.clients.Get(ctx, msg.To)
	if target != nil {
		if err := target.Send(msg); err != nil {
			log.Printf("[router] recall send to %s failed, storing offline: %v", msg.To, err)
			r.offline.StoreOffline(ctx, msg.To, msg)
		}
		return
	}

	// Target not local — forward to peer Gateway or store offline.
	r.routeOrStoreOffline(ctx, msg.To, msg)
}

// sendRecallError sends an error response for a failed recall request.
func (r *Router) sendRecallError(sender *Client, reason string) {
	sender.Send(&proto.Message{
		Cmd:       proto.CmdRecall,
		MsgId:     r.snow.Next(),
		To:        sender.UID,
		Content:   fmt.Sprintf(`{"error":"%s"}`, reason),
		Timestamp: time.Now().UnixMilli(),
	})
}

func (r *Router) handleChat(ctx context.Context, sender *Client, msg *proto.Message) {
	// --- Deduplication check ---
	if msg.Seq > 0 {
		if isDup, existingMsgID := r.dedup.IsDuplicate(sender.UID, msg.Seq); isDup {
			// Resend ACK with the previously assigned MsgID
			if msg.NeedAck {
				ack := &proto.Message{
					Cmd:       proto.CmdAck,
					MsgId:     existingMsgID,
					Seq:       msg.Seq,
					To:        sender.UID,
					Timestamp: time.Now().UnixMilli(),
				}
				sender.Send(ack)
			}
			log.Printf("[router] duplicate message seq=%d from=%s — ACK replayed", msg.Seq, sender.UID)
			return
		}
	}

	// --- Validation ---
	if err := msg.Validate(); err != nil {
		log.Printf("[router] invalid message from %s: %v", sender.UID, err)
		return
	}

	// --- Rate limiting ---
	if r.checkRateLimit(sender.UID) {
		log.Printf("[router] rate limited uid=%s", sender.UID)
		return
	}

	// Assign a globally unique message ID
	msg.MsgId = r.snow.Next()
	msg.Timestamp = time.Now().UnixMilli()

	// Deliver to target(s).
	// NOTE: Dedup Mark is called AFTER delivery, not before.
	// If the send buffer is full and we marked first, the client's retry
	// would be silently dropped (message loss). Marking after ensures
	// at-least-once semantics for online targets.
	if msg.ChatType == proto.ChatTypeGroup && r.groupStore != nil {
		r.fanoutGroup(ctx, sender, msg)
		// Group fanout: mark after all members processed.
		if msg.Seq > 0 {
			r.dedup.Mark(sender.UID, msg.Seq, msg.MsgId)
		}
	} else {
		target := r.clients.Get(ctx, msg.To)
		if target != nil {
			if err := target.Send(msg); err != nil {
				// Send failed (buffer full) — store offline, but do NOT mark yet
				// so the client can retry for online delivery.
				log.Printf("[router] send failed for %s, storing offline: %v", msg.To, err)
				r.offline.StoreOffline(ctx, msg.To, msg)
			} else if msg.Seq > 0 {
				// Online delivery succeeded — safe to mark.
				r.dedup.Mark(sender.UID, msg.Seq, msg.MsgId)
			}
		} else {
			// Target not locally online — route or store offline.
			r.routeOrStoreOffline(ctx, msg.To, msg)
			if msg.Seq > 0 {
				r.dedup.Mark(sender.UID, msg.Seq, msg.MsgId)
			}
		}
	}

	// Increment unread count for the target(s).
	// For single chat: the target gets +1 unread from the sender.
	// For group chat: ALL members except sender get +1 unread from the sender.
	if r.unreadTracker != nil {
		if msg.ChatType == proto.ChatTypeGroup && r.groupStore != nil {
			members, err := r.groupStore.GetMembers(ctx, msg.To)
			if err == nil {
				for _, memberUID := range members {
					if memberUID != sender.UID {
						r.unreadTracker.Increment(ctx, memberUID, msg.From)
					}
				}
			}
		} else {
			if msg.To != msg.From {
				r.unreadTracker.Increment(ctx, msg.To, msg.From)
			}
		}
	}

	// Send ACK to sender
	if msg.NeedAck {
		ack := &proto.Message{
			Cmd:       proto.CmdAck,
			MsgId:     msg.MsgId,
			Seq:       msg.Seq,
			To:        sender.UID,
			Timestamp: time.Now().UnixMilli(),
		}
		sender.Send(ack)
	}

	r.persistAsync(ctx, msg)
}

// routeOrStoreOffline decides where to deliver or store a message when the target
// is not connected locally. In single-node mode (no hash ring configured), the
// message is stored offline locally. In multi-node mode, the hash ring determines
// the owner node: if it's this node, store offline; otherwise forward to the peer.
// If forwarding fails, the message is stored offline locally as a fallback.
func (r *Router) routeOrStoreOffline(ctx context.Context, targetUID string, msg *proto.Message) {
	// No multi-gateway configured — store offline locally (backward compatible).
	if r.hashRing == nil || r.thisNodeID == "" {
		r.offline.StoreOffline(ctx, targetUID, msg)
		log.Printf("[router] stored offline message for %s from %s", targetUID, msg.From)
		return
	}

	ownerNode := r.hashRing.Get(targetUID)
	if ownerNode == "" || ownerNode == r.thisNodeID {
		// Empty ring or this node owns the user — user is genuinely offline.
		r.offline.StoreOffline(ctx, targetUID, msg)
		log.Printf("[router] stored offline message for %s from %s (this node owns %s)",
			targetUID, msg.From, targetUID)
		return
	}

	// Forward to the peer Gateway that owns the target user.
	if r.forwarder == nil {
		log.Printf("[router] forwarder not configured, storing offline locally for %s", targetUID)
		r.offline.StoreOffline(ctx, targetUID, msg)
		return
	}

	delivered, err := r.forwarder.Forward(ctx, targetUID, msg)
	if err != nil {
		// Forward failed (network error, peer down) — fallback to local offline storage.
		log.Printf("[router] forward to %s for %s failed: %v — storing offline locally",
			ownerNode, targetUID, err)
		r.offline.StoreOffline(ctx, targetUID, msg)
		return
	}

	if delivered {
		log.Printf("[router] forwarded message for %s to peer %s (delivered online)", targetUID, ownerNode)
	} else {
		log.Printf("[router] forwarded message for %s to peer %s (peer stored offline)", targetUID, ownerNode)
	}
}

// fanoutGroup delivers a group chat message to all members except the sender.
// Each member is delivered independently: online members get the message pushed;
// offline members get it stored; failures on one member don't block others.
func (r *Router) fanoutGroup(ctx context.Context, sender *Client, msg *proto.Message) {
	members, err := r.groupStore.GetMembers(ctx, msg.To)
	if err != nil {
		log.Printf("[router] group chat: get members for group %s failed: %v", msg.To, err)
		return
	}

	delivered := 0
	for _, memberUID := range members {
		if memberUID == sender.UID {
			continue // don't deliver to self
		}

		target := r.clients.Get(ctx, memberUID)
		if target != nil {
			if err := target.Send(msg); err != nil {
				log.Printf("[router] group chat: send to %s failed, storing offline: %v", memberUID, err)
				r.offline.StoreOffline(ctx, memberUID, msg)
			} else {
				delivered++
			}
		} else {
			// Member not connected locally — forward or store offline.
			r.routeOrStoreOffline(ctx, memberUID, msg)
		}
	}

	log.Printf("[router] group chat: fanout for group %s — %d/%d members delivered online",
		msg.To, delivered, len(members)-1)
}

// fanoutGroupWithMembers delivers a message to an explicit list of group members.
// Unlike fanoutGroup, it does not query the local GroupStore — the caller provides
// the member UIDs directly. This is used when group membership is managed
// externally (e.g. via gRPC Logic service) to avoid double-writes.
func (r *Router) fanoutGroupWithMembers(ctx context.Context, sender *Client, msg *proto.Message, members []string) {
	delivered := 0
	for _, memberUID := range members {
		if memberUID == sender.UID {
			continue // don't deliver to self
		}

		target := r.clients.Get(ctx, memberUID)
		if target != nil {
			if err := target.Send(msg); err != nil {
				log.Printf("[router] group chat: send to %s failed, storing offline: %v", memberUID, err)
				r.offline.StoreOffline(ctx, memberUID, msg)
			} else {
				delivered++
			}
		} else {
			// Member not connected locally — forward or store offline.
			r.routeOrStoreOffline(ctx, memberUID, msg)
		}
	}

	log.Printf("[router] group chat: fanout (explicit members) for group %s — %d/%d members delivered online",
		msg.To, delivered, len(members)-1)
}

func (r *Router) handleOffline(ctx context.Context, sender *Client) {
	msgs := r.offline.DrainOffline(ctx, sender.UID)
	if len(msgs) == 0 {
		sender.Send(&proto.Message{
			Cmd:       proto.CmdOffline,
			MsgId:     r.snow.Next(),
			Timestamp: time.Now().UnixMilli(),
		})
		return
	}

	delivered := 0
	for _, msg := range msgs {
		if err := sender.Send(msg); err != nil {
			// Re-enqueue undelivered messages back to offline storage.
			for _, m := range msgs[delivered:] {
				r.offline.StoreOffline(ctx, sender.UID, m)
			}
			log.Printf("[router] send buffer full for %s, re-enqueued %d offline messages",
				sender.UID, len(msgs)-delivered)
			return
		}
		delivered++
	}

	log.Printf("[router] delivered %d offline messages to %s", delivered, sender.UID)
}

func (r *Router) handleHistory(ctx context.Context, sender *Client, msg *proto.Message) {
	// Validate — ensures To field is present.
	if err := msg.Validate(); err != nil {
		log.Printf("[router] invalid history request from %s: %v", sender.UID, err)
		return
	}

	// Parse pagination parameters from the message.
	limit := int(msg.Seq)
	if limit <= 0 {
		limit = r.historyLimit // default page size
	}
	if limit > 100 {
		limit = 100 // cap
	}

	before := msg.Timestamp
	if before <= 0 {
		before = time.Now().UnixMilli()
	}

	// Query conversation history.
	// Prefer gRPC Logic service, fall back to local MessageStore, then empty.
	// Group history goes directly through local MessageStore (gRPC path is Step 3).
	var msgs []*proto.Message
	var err error

	if msg.ChatType == proto.ChatTypeGroup {
		if r.msgStore != nil {
			msgs, err = r.msgStore.QueryGroupHistory(ctx, msg.To, before, limit)
		}
	} else {
		if r.logicClient != nil {
			msgs, err = r.logicClient.QueryHistory(ctx, sender.UID, msg.To, before, limit)
		} else if r.msgStore != nil {
			msgs, err = r.msgStore.QueryHistory(ctx, sender.UID, msg.To, before, limit)
		}
	}

	if err != nil {
		log.Printf("[router] history query error for %s<->%s: %v", sender.UID, msg.To, err)
		sender.Send(&proto.Message{
			Cmd:       proto.CmdHistory,
			MsgId:     r.snow.Next(),
			Timestamp: time.Now().UnixMilli(),
		})
		return
	}

	if msgs == nil && r.msgStore == nil && r.logicClient == nil {
		// No persistence layer at all — return empty completion.
		sender.Send(&proto.Message{
			Cmd:       proto.CmdHistory,
			MsgId:     r.snow.Next(),
			Timestamp: time.Now().UnixMilli(),
		})
		return
	}

	// Send each history message preserving original MsgId, From, Timestamp, etc.
	delivered := 0
	for _, m := range msgs {
		if err := sender.Send(m); err != nil {
			log.Printf("[router] send buffer full for %s during history, sent %d/%d",
				sender.UID, delivered, len(msgs))
			break
		}
		delivered++
	}

	// Signal completion with the count of delivered messages in Seq.
	sender.Send(&proto.Message{
		Cmd:       proto.CmdHistory,
		Seq:       int64(delivered),
		MsgId:     r.snow.Next(),
		Timestamp: time.Now().UnixMilli(),
	})

	log.Printf("[router] delivered %d history messages to %s (with=%s)", delivered, sender.UID, msg.To)
}

// handleReadReceipt processes a read receipt from a client.
// It clears the sender's unread count from the peer, then forwards the receipt
// to the original sender (peerUID) so their client knows the messages were read.
// Read receipts are ephemeral: if the peer is offline, the receipt is dropped.
func (r *Router) handleReadReceipt(ctx context.Context, sender *Client, msg *proto.Message) {
	peerUID := msg.To // the user whose messages were read by sender

	// Validate: peerUID is required and must differ from sender.
	if peerUID == "" || peerUID == sender.UID {
		log.Printf("[router] invalid read receipt from %s: peer=%q", sender.UID, peerUID)
		return
	}

	// 1. Clear the unread count for the reader (sender) from the peer.
	// Try gRPC Logic service first, fall back to local tracker.
	if r.logicClient != nil {
		if err := r.logicClient.MarkReadClient(ctx, sender.UID, peerUID); err != nil {
			log.Printf("[router] gRPC MarkRead error: %v", err)
			// Fall back to local tracker on gRPC failure.
			if r.unreadTracker != nil {
				r.unreadTracker.MarkRead(ctx, sender.UID, peerUID)
			}
		}
	} else if r.unreadTracker != nil {
		r.unreadTracker.MarkRead(ctx, sender.UID, peerUID)
	}

	// 2. Build a read receipt notification for the original sender (peerUID).
	receipt := &proto.Message{
		Cmd:       proto.CmdReadReceipt,
		MsgId:     r.snow.Next(),
		From:      sender.UID, // who read the messages
		To:        peerUID,    // should be notified
		Seq:       msg.MsgId,  // carry the last read message ID
		Timestamp: time.Now().UnixMilli(),
	}

	// 3. Try local delivery first.
	target := r.clients.Get(ctx, peerUID)
	if target != nil {
		if err := target.Send(receipt); err != nil {
			log.Printf("[router] read receipt send to %s failed: %v", peerUID, err)
		}
		return
	}

	// 4. Try cross-gateway forwarding via hash ring.
	if r.hashRing != nil && r.thisNodeID != "" {
		ownerNode := r.hashRing.Get(peerUID)
		if ownerNode != "" && ownerNode != r.thisNodeID && r.forwarder != nil {
			if _, err := r.forwarder.Forward(ctx, peerUID, receipt); err != nil {
				log.Printf("[router] read receipt forward to %s failed: %v", peerUID, err)
			}
			return
		}
	}

	// 5. Peer is offline or unreachable — drop the receipt.
	log.Printf("[router] read receipt from %s for %s dropped (peer offline)",
		sender.UID, peerUID)
}

// handleUnreadCount returns per-peer unread counts for the requesting user.
func (r *Router) handleUnreadCount(ctx context.Context, sender *Client, msg *proto.Message) {
	// Try gRPC Logic service first, fall back to local tracker.
	counts := map[string]int64{}
	if r.logicClient != nil {
		if remoteCounts, err := r.logicClient.GetUnreadCountsClient(ctx, sender.UID); err == nil && remoteCounts != nil {
			counts = remoteCounts
		}
	}
	if len(counts) == 0 && r.unreadTracker != nil {
		counts = r.unreadTracker.GetCounts(ctx, sender.UID)
	}

	result := map[string]interface{}{
		"uid":    sender.UID,
		"counts": counts,
	}
	data, err := json.Marshal(result)
	if err != nil {
		log.Printf("[router] unread count marshal error for %s: %v", sender.UID, err)
		return
	}

	resp := &proto.Message{
		Cmd:       proto.CmdUnreadCount,
		MsgId:     r.snow.Next(),
		To:        sender.UID,
		Content:   string(data),
		Timestamp: time.Now().UnixMilli(),
	}
	sender.Send(resp)
}

// handleSearch performs a fulltext search on message content.
// The search query and filters are JSON-encoded in msg.Content.
// Results are sent as individual messages followed by a completion signal.
func (r *Router) handleSearch(ctx context.Context, sender *Client, msg *proto.Message) {
	// Parse search params from Content (JSON).
	var params repo.SearchParams
	if msg.Content != "" {
		if err := json.Unmarshal([]byte(msg.Content), &params); err != nil {
			log.Printf("[router] search parse error from %s: %v", sender.UID, err)
			sender.Send(&proto.Message{
				Cmd:       proto.CmdSearch,
				MsgId:     r.snow.Next(),
				Content:   `{"error":"invalid search params"}`,
				Timestamp: time.Now().UnixMilli(),
			})
			return
		}
	}
	params.UID = sender.UID

	// Defaults.
	if params.Limit <= 0 {
		params.Limit = r.searchLimit
	}
	if params.Limit > 50 {
		params.Limit = 50
	}

	// Try gRPC Logic service first, fall back to local MessageStore.
	var result *repo.SearchResult
	var searchErr error
	if r.logicClient != nil {
		result, searchErr = r.logicClient.SearchMessages(ctx, &params)
		if searchErr != nil {
			log.Printf("[router] search via gRPC error for %s (q=%q): %v", sender.UID, params.Query, searchErr)
			sender.Send(&proto.Message{
				Cmd:       proto.CmdSearch,
				MsgId:     r.snow.Next(),
				Content:   `{"error":"search failed"}`,
				Timestamp: time.Now().UnixMilli(),
			})
			return
		}
	}
	if result == nil && r.msgStore != nil {
		result, searchErr = r.msgStore.SearchMessages(ctx, &params)
		if searchErr != nil {
			log.Printf("[router] search via local error for %s (q=%q): %v", sender.UID, params.Query, searchErr)
			sender.Send(&proto.Message{
				Cmd:       proto.CmdSearch,
				MsgId:     r.snow.Next(),
				Content:   `{"error":"search failed"}`,
				Timestamp: time.Now().UnixMilli(),
			})
			return
		}
	}

	if result == nil || len(result.Messages) == 0 {
		// No results — send empty completion.
		sender.Send(&proto.Message{
			Cmd:       proto.CmdSearch,
			MsgId:     r.snow.Next(),
			Seq:       0,
			Timestamp: time.Now().UnixMilli(),
		})
		return
	}

	// Send each matching message.
	delivered := 0
	for _, m := range result.Messages {
		if err := sender.Send(m); err != nil {
			break
		}
		delivered++
	}

	// Send completion signal with count and next cursor.
	completion, _ := json.Marshal(map[string]interface{}{
		"delivered":   delivered,
		"next_cursor": result.NextCursor,
	})
	sender.Send(&proto.Message{
		Cmd:       proto.CmdSearch,
		Seq:       int64(delivered),
		MsgId:     r.snow.Next(),
		Content:   string(completion),
		Timestamp: time.Now().UnixMilli(),
	})

	log.Printf("[router] search for %s (q=%q): delivered %d results", sender.UID, params.Query, delivered)
}

// sendGroupNotification constructs a system notification for group events
// (member join/leave) and delivers it to all group members via fanout + persist.
// It fetches the member list from the local GroupStore.
func (r *Router) sendGroupNotification(ctx context.Context, fromUID, groupID, notifType string) {
	r.sendGroupNotificationWithMembers(ctx, fromUID, groupID, notifType, nil)
}

// sendGroupNotificationWithMembers is like sendGroupNotification but uses an
// explicit member list when provided (members != nil). This avoids a round-trip
// to the local GroupStore and is the preferred path when group membership is
// managed externally (e.g. via gRPC Logic service). When members is nil, the
// member list is fetched from the local GroupStore as a fallback.
func (r *Router) sendGroupNotificationWithMembers(ctx context.Context, fromUID, groupID, notifType string, members []string) {
	content, _ := json.Marshal(map[string]string{
		"type":     notifType,
		"group_id": groupID,
		"uid":      fromUID,
	})
	notif := &proto.Message{
		Cmd:       proto.CmdChat,
		MsgId:     r.snow.Next(),
		From:      fromUID,
		To:        groupID,
		ChatType:  proto.ChatTypeGroup,
		MsgType:   proto.MsgTypeText,
		Content:   string(content),
		Timestamp: time.Now().UnixMilli(),
	}
	sender := r.clients.Get(ctx, fromUID)
	if sender == nil {
		sender = &Client{UID: fromUID} // minimal client for self-skip in fanout
	}
	if members != nil {
		r.fanoutGroupWithMembers(ctx, sender, notif, members)
	} else {
		r.fanoutGroup(ctx, sender, notif)
	}
	r.persistAsync(ctx, notif)
}

// --- Group management handlers (implemented Phase 5) ---

// handleGroupCreate creates a new group. The sender becomes the owner and first member.
// Request: Content = {"name": "My Group"}
// Response: Content = {"id":"g_123","name":"My Group","owner_uid":"alice","members":["alice"],"created_at":123}
func (r *Router) handleGroupCreate(ctx context.Context, sender *Client, msg *proto.Message) {
	var req struct {
		Name    string   `json:"name"`
		Members []string `json:"members"`
	}
	if msg.Content != "" {
		if err := json.Unmarshal([]byte(msg.Content), &req); err != nil {
			r.sendGroupError(sender, proto.CmdGroupCreate, "invalid request: "+err.Error())
			return
		}
	}
	if req.Name == "" {
		r.sendGroupError(sender, proto.CmdGroupCreate, "group name is required")
		return
	}

	// Try gRPC Logic service first.
	if r.logicClient != nil {
		groupInfo, err := r.logicClient.CreateGroupClient(ctx, req.Name, sender.UID)
		if err != nil {
			r.sendGroupError(sender, proto.CmdGroupCreate, err.Error())
			return
		}
		if groupInfo != nil {
			// Add initial members via gRPC (owner is already a member).
			for _, memberUID := range req.Members {
				if memberUID != "" && memberUID != sender.UID {
					if joinErr := r.logicClient.JoinGroupClient(ctx, groupInfo.Id, memberUID); joinErr != nil {
						log.Printf("[router] group create: failed to add member %s to %s: %v", memberUID, groupInfo.Id, joinErr)
					} else {
						groupInfo.Members = append(groupInfo.Members, memberUID)
					}
				}
			}
			data, _ := json.Marshal(map[string]interface{}{
				"id":         groupInfo.Id,
				"name":       groupInfo.Name,
				"owner_uid":  groupInfo.OwnerUid,
				"members":    groupInfo.Members,
				"created_at": groupInfo.CreatedAt,
			})
			sender.Send(&proto.Message{
				Cmd:       proto.CmdGroupCreate,
				MsgId:     r.snow.Next(),
				Content:   string(data),
				Timestamp: time.Now().UnixMilli(),
			})
			// Notify invited members (skip self).
			joinedMembers := make([]string, 0)
			for _, m := range req.Members {
				if m != "" && m != sender.UID {
					joinedMembers = append(joinedMembers, m)
				}
			}
			if len(joinedMembers) > 0 {
				r.sendGroupNotificationWithMembers(ctx, sender.UID, groupInfo.Id, "member_joined", groupInfo.Members)
			}
			log.Printf("[router] group created via gRPC: id=%s name=%q owner=%s members=%d", groupInfo.Id, req.Name, sender.UID, len(groupInfo.Members))
			return
		}
	}

	// Fall back to local GroupStore.
	if r.groupStore == nil {
		r.sendGroupError(sender, proto.CmdGroupCreate, "group chat not enabled")
		return
	}

	group, err := r.groupStore.Create(ctx, req.Name, sender.UID, req.Members)
	if err != nil {
		r.sendGroupError(sender, proto.CmdGroupCreate, err.Error())
		return
	}

	members, _ := r.groupStore.GetMembers(ctx, group.ID)
	data, _ := json.Marshal(map[string]interface{}{
		"id":         group.ID,
		"name":       group.Name,
		"owner_uid":  group.OwnerUID,
		"members":    members,
		"created_at": group.CreatedAt,
	})

	sender.Send(&proto.Message{
		Cmd:       proto.CmdGroupCreate,
		MsgId:     r.snow.Next(),
		Content:   string(data),
		Timestamp: time.Now().UnixMilli(),
	})

	// Notify invited members (skip self).
	if len(req.Members) > 0 {
		r.sendGroupNotificationWithMembers(ctx, sender.UID, group.ID, "member_joined", members)
	}

	log.Printf("[router] group created: id=%s name=%q owner=%s members=%d", group.ID, req.Name, sender.UID, len(members))
}

// handleGroupJoin adds the sender to a group.
// Request: To = group_id, Content = optional (unused)
// Response: Content = {"group_id":"g_123","uid":"bob","members":["alice","bob"]}
func (r *Router) handleGroupJoin(ctx context.Context, sender *Client, msg *proto.Message) {
	groupID := msg.To
	if groupID == "" {
		r.sendGroupError(sender, proto.CmdGroupJoin, "group_id is required (set 'to' field)")
		return
	}

	// Try gRPC Logic service first.
	if r.logicClient != nil {
		if err := r.logicClient.JoinGroupClient(ctx, groupID, sender.UID); err != nil {
			r.sendGroupError(sender, proto.CmdGroupJoin, err.Error())
			return
		}
		// Fetch member list for the response.
		groupInfo, _ := r.logicClient.GetGroupClient(ctx, groupID)
		members := []string{}
		if groupInfo != nil {
			members = groupInfo.Members
		}
		data, _ := json.Marshal(map[string]interface{}{
			"group_id": groupID,
			"uid":      sender.UID,
			"members":  members,
		})
		sender.Send(&proto.Message{
			Cmd:       proto.CmdGroupJoin,
			MsgId:     r.snow.Next(),
			Content:   string(data),
			Timestamp: time.Now().UnixMilli(),
		})
		// Use the member list from gRPC for notification fanout instead of writing
		// to the local GroupStore — avoids double-write when groupStore is MySQL-backed.
		r.sendGroupNotificationWithMembers(ctx, sender.UID, groupID, "member_joined", members)
		log.Printf("[router] %s joined group %s via gRPC", sender.UID, groupID)
		return
	}

	// Fall back to local GroupStore.
	if r.groupStore == nil {
		r.sendGroupError(sender, proto.CmdGroupJoin, "group chat not enabled")
		return
	}

	if err := r.groupStore.AddMember(ctx, groupID, sender.UID); err != nil {
		r.sendGroupError(sender, proto.CmdGroupJoin, err.Error())
		return
	}

	members, _ := r.groupStore.GetMembers(ctx, groupID)
	data, _ := json.Marshal(map[string]interface{}{
		"group_id": groupID,
		"uid":      sender.UID,
		"members":  members,
	})

	sender.Send(&proto.Message{
		Cmd:       proto.CmdGroupJoin,
		MsgId:     r.snow.Next(),
		Content:   string(data),
		Timestamp: time.Now().UnixMilli(),
	})

	// Notify all group members about the new member.
	r.sendGroupNotification(ctx, sender.UID, groupID, "member_joined")

	log.Printf("[router] %s joined group %s", sender.UID, groupID)
}

// handleGroupInviteMember invites a third-party user to a group. Only the group owner can invite.
// Request: To = group_id, Content = {"target_uid":"bob"}
// Response: Content = {"group_id":"g_123","target_uid":"bob","inviter_uid":"alice","members":[...]}
func (r *Router) handleGroupInviteMember(ctx context.Context, sender *Client, msg *proto.Message) {
	groupID := msg.To
	if groupID == "" {
		r.sendGroupError(sender, proto.CmdGroupInviteMember, "group_id is required (set 'to' field)")
		return
	}

	// Parse target_uid from content.
	var req struct {
		TargetUID string `json:"target_uid"`
	}
	if err := json.Unmarshal([]byte(msg.Content), &req); err != nil || req.TargetUID == "" {
		r.sendGroupError(sender, proto.CmdGroupInviteMember, "target_uid is required in content")
		return
	}
	targetUID := req.TargetUID

	if targetUID == sender.UID {
		r.sendGroupError(sender, proto.CmdGroupInviteMember, "cannot invite yourself — use CmdGroupJoin instead")
		return
	}

	// Try gRPC Logic service first.
	if r.logicClient != nil {
		// Validate sender is the group owner.
		groupInfo, err := r.logicClient.GetGroupClient(ctx, groupID)
		if err != nil {
			r.sendGroupError(sender, proto.CmdGroupInviteMember, err.Error())
			return
		}
		if groupInfo == nil {
			r.sendGroupError(sender, proto.CmdGroupInviteMember, "group not found")
			return
		}
		if groupInfo.OwnerUid != sender.UID {
			r.sendGroupError(sender, proto.CmdGroupInviteMember, "only the group owner can invite members")
			return
		}

		// Add the target user.
		if err := r.logicClient.JoinGroupClient(ctx, groupID, targetUID); err != nil {
			r.sendGroupError(sender, proto.CmdGroupInviteMember, err.Error())
			return
		}

		// Fetch updated member list.
		updatedGroup, _ := r.logicClient.GetGroupClient(ctx, groupID)
		members := []string{}
		if updatedGroup != nil {
			members = updatedGroup.Members
		}
		data, _ := json.Marshal(map[string]interface{}{
			"group_id":    groupID,
			"target_uid":  targetUID,
			"inviter_uid": sender.UID,
			"members":     members,
		})
		sender.Send(&proto.Message{
			Cmd:       proto.CmdGroupInviteMember,
			MsgId:     r.snow.Next(),
			Content:   string(data),
			Timestamp: time.Now().UnixMilli(),
		})
		// Notify all members about the new member.
		r.sendGroupNotificationWithMembers(ctx, targetUID, groupID, "member_joined", members)
		log.Printf("[router] %s invited %s to group %s via gRPC", sender.UID, targetUID, groupID)
		return
	}

	// Fall back to local GroupStore.
	if r.groupStore == nil {
		r.sendGroupError(sender, proto.CmdGroupInviteMember, "group chat not enabled")
		return
	}

	// Validate sender is the group owner.
	g, err := r.groupStore.Get(ctx, groupID)
	if err != nil {
		r.sendGroupError(sender, proto.CmdGroupInviteMember, err.Error())
		return
	}
	if g == nil {
		r.sendGroupError(sender, proto.CmdGroupInviteMember, "group not found")
		return
	}
	if g.OwnerUID != sender.UID {
		r.sendGroupError(sender, proto.CmdGroupInviteMember, "only the group owner can invite members")
		return
	}

	// Add the target user.
	if err := r.groupStore.AddMember(ctx, groupID, targetUID); err != nil {
		r.sendGroupError(sender, proto.CmdGroupInviteMember, err.Error())
		return
	}

	members, _ := r.groupStore.GetMembers(ctx, groupID)
	data, _ := json.Marshal(map[string]interface{}{
		"group_id":    groupID,
		"target_uid":  targetUID,
		"inviter_uid": sender.UID,
		"members":     members,
	})

	sender.Send(&proto.Message{
		Cmd:       proto.CmdGroupInviteMember,
		MsgId:     r.snow.Next(),
		Content:   string(data),
		Timestamp: time.Now().UnixMilli(),
	})

	// Notify all group members about the new member.
	r.sendGroupNotification(ctx, targetUID, groupID, "member_joined")

	log.Printf("[router] %s invited %s to group %s", sender.UID, targetUID, groupID)
}

// handleGroupLeave removes the sender from a group. If the group becomes empty, it is deleted.
// Request: To = group_id
// Response: Content = {"group_id":"g_123","uid":"bob","deleted":false}
func (r *Router) handleGroupLeave(ctx context.Context, sender *Client, msg *proto.Message) {
	groupID := msg.To
	if groupID == "" {
		r.sendGroupError(sender, proto.CmdGroupLeave, "group_id is required (set 'to' field)")
		return
	}

	// Try gRPC Logic service first.
	if r.logicClient != nil {
		deleted, err := r.logicClient.LeaveGroupClient(ctx, groupID, sender.UID)
		if err != nil {
			r.sendGroupError(sender, proto.CmdGroupLeave, err.Error())
			return
		}
		data, _ := json.Marshal(map[string]interface{}{
			"group_id": groupID,
			"uid":      sender.UID,
			"deleted":  deleted,
		})
		sender.Send(&proto.Message{
			Cmd:       proto.CmdGroupLeave,
			MsgId:     r.snow.Next(),
			Content:   string(data),
			Timestamp: time.Now().UnixMilli(),
		})
		// Fetch remaining members from gRPC for notification fanout instead of
		// writing to the local GroupStore — avoids double-write with MySQL-backed store.
		if !deleted {
			groupInfo, _ := r.logicClient.GetGroupClient(ctx, groupID)
			members := []string{}
			if groupInfo != nil {
				members = groupInfo.Members
			}
			r.sendGroupNotificationWithMembers(ctx, sender.UID, groupID, "member_left", members)
		}
		log.Printf("[router] %s left group %s via gRPC", sender.UID, groupID)
		return
	}

	// Fall back to local GroupStore.
	if r.groupStore == nil {
		r.sendGroupError(sender, proto.CmdGroupLeave, "group chat not enabled")
		return
	}

	if err := r.groupStore.RemoveMember(ctx, groupID, sender.UID); err != nil {
		r.sendGroupError(sender, proto.CmdGroupLeave, err.Error())
		return
	}

	// Check if the group still exists (it's deleted when the last member leaves).
	_, getErr := r.groupStore.Get(ctx, groupID)
	wasDeleted := getErr != nil
	data, _ := json.Marshal(map[string]interface{}{
		"group_id": groupID,
		"uid":      sender.UID,
		"deleted":  wasDeleted, // true if group was deleted (last member left)
	})

	sender.Send(&proto.Message{
		Cmd:       proto.CmdGroupLeave,
		MsgId:     r.snow.Next(),
		Content:   string(data),
		Timestamp: time.Now().UnixMilli(),
	})

	// Notify remaining group members about the departure.
	// Only send when the group still exists (has remaining members).
	if !wasDeleted {
		r.sendGroupNotification(ctx, sender.UID, groupID, "member_left")
	}

	log.Printf("[router] %s left group %s", sender.UID, groupID)
}

// handleGroupInfo returns full group information including member list.
// Request: To = group_id
// Response: Content = {"id":"g_123","name":"My Group","owner_uid":"alice","members":["alice","bob"],"created_at":123}
func (r *Router) handleGroupInfo(ctx context.Context, sender *Client, msg *proto.Message) {
	groupID := msg.To
	if groupID == "" {
		r.sendGroupError(sender, proto.CmdGroupInfo, "group_id is required (set 'to' field)")
		return
	}

	// Try gRPC Logic service first.
	if r.logicClient != nil {
		groupInfo, err := r.logicClient.GetGroupClient(ctx, groupID)
		if err != nil {
			r.sendGroupError(sender, proto.CmdGroupInfo, err.Error())
			return
		}
		if groupInfo == nil {
			r.sendGroupError(sender, proto.CmdGroupInfo, "group not found")
			return
		}
		data, _ := json.Marshal(map[string]interface{}{
			"id":         groupInfo.Id,
			"name":       groupInfo.Name,
			"owner_uid":  groupInfo.OwnerUid,
			"members":    groupInfo.Members,
			"created_at": groupInfo.CreatedAt,
		})
		sender.Send(&proto.Message{
			Cmd:       proto.CmdGroupInfo,
			MsgId:     r.snow.Next(),
			Content:   string(data),
			Timestamp: time.Now().UnixMilli(),
		})
		return
	}

	// Fall back to local GroupStore.
	if r.groupStore == nil {
		r.sendGroupError(sender, proto.CmdGroupInfo, "group chat not enabled")
		return
	}

	group, err := r.groupStore.Get(ctx, groupID)
	if err != nil {
		r.sendGroupError(sender, proto.CmdGroupInfo, err.Error())
		return
	}

	members, _ := r.groupStore.GetMembers(ctx, groupID)
	data, _ := json.Marshal(map[string]interface{}{
		"id":         group.ID,
		"name":       group.Name,
		"owner_uid":  group.OwnerUID,
		"members":    members,
		"created_at": group.CreatedAt,
	})

	sender.Send(&proto.Message{
		Cmd:       proto.CmdGroupInfo,
		MsgId:     r.snow.Next(),
		Content:   string(data),
		Timestamp: time.Now().UnixMilli(),
	})
}

// handleGroupList returns all groups the sender belongs to.
// Request: no special fields
// Response: Content = {"uid":"alice","groups":[{"id":"g_1","name":"...","owner_uid":"...","member_count":2,"created_at":123},...]}
func (r *Router) handleGroupList(ctx context.Context, sender *Client, msg *proto.Message) {
	// Try gRPC Logic service first.
	if r.logicClient != nil {
		groupInfos, err := r.logicClient.ListGroupsClient(ctx, sender.UID)
		if err != nil {
			r.sendGroupError(sender, proto.CmdGroupList, err.Error())
			return
		}
		type groupSummary struct {
			ID          string `json:"id"`
			Name        string `json:"name"`
			OwnerUID    string `json:"owner_uid"`
			MemberCount int    `json:"member_count"`
			CreatedAt   int64  `json:"created_at"`
		}
		summaries := make([]groupSummary, 0, len(groupInfos))
		for _, g := range groupInfos {
			summaries = append(summaries, groupSummary{
				ID:          g.Id,
				Name:        g.Name,
				OwnerUID:    g.OwnerUid,
				MemberCount: len(g.Members),
				CreatedAt:   g.CreatedAt,
			})
		}
		data, _ := json.Marshal(map[string]interface{}{
			"uid":    sender.UID,
			"groups": summaries,
		})
		sender.Send(&proto.Message{
			Cmd:       proto.CmdGroupList,
			MsgId:     r.snow.Next(),
			Content:   string(data),
			Timestamp: time.Now().UnixMilli(),
		})
		log.Printf("[router] group list for %s: %d groups (via gRPC)", sender.UID, len(summaries))
		return
	}

	// Fall back to local GroupStore.
	if r.groupStore == nil {
		r.sendGroupError(sender, proto.CmdGroupList, "group chat not enabled")
		return
	}

	groups, err := r.groupStore.GetUserGroups(ctx, sender.UID)
	if err != nil {
		r.sendGroupError(sender, proto.CmdGroupList, err.Error())
		return
	}

	type groupSummary struct {
		ID          string `json:"id"`
		Name        string `json:"name"`
		OwnerUID    string `json:"owner_uid"`
		MemberCount int    `json:"member_count"`
		CreatedAt   int64  `json:"created_at"`
	}

	summaries := make([]groupSummary, 0, len(groups))
	for _, g := range groups {
		summaries = append(summaries, groupSummary{
			ID:          g.ID,
			Name:        g.Name,
			OwnerUID:    g.OwnerUID,
			MemberCount: len(g.Members),
			CreatedAt:   g.CreatedAt,
		})
	}

	data, _ := json.Marshal(map[string]interface{}{
		"uid":    sender.UID,
		"groups": summaries,
	})

	sender.Send(&proto.Message{
		Cmd:       proto.CmdGroupList,
		MsgId:     r.snow.Next(),
		Content:   string(data),
		Timestamp: time.Now().UnixMilli(),
	})

	log.Printf("[router] group list for %s: %d groups", sender.UID, len(summaries))
}

// sendGroupError sends an error response for group management commands.
func (r *Router) sendGroupError(sender *Client, cmd int32, errMsg string) {
	data, _ := json.Marshal(map[string]string{"error": errMsg})
	sender.Send(&proto.Message{
		Cmd:       cmd,
		MsgId:     r.snow.Next(),
		Content:   string(data),
		Timestamp: time.Now().UnixMilli(),
	})
}

// handleFriendRequest processes a friend request. The sender asks to add the
// target (msg.To) as a friend. The Router persists the request and forwards
// a notification to the target if they are online.
func (r *Router) handleFriendRequest(ctx context.Context, sender *Client, msg *proto.Message) {
	if r.friendStore == nil {
		log.Printf("[router] friend request from %s dropped: friend store not available", sender.UID)
		return
	}
	msg.From = sender.UID

	if msg.To == "" || msg.To == sender.UID {
		log.Printf("[router] friend request from %s dropped: invalid target %q", sender.UID, msg.To)
		return
	}

	if err := r.friendStore.SendRequest(ctx, sender.UID, msg.To); err != nil {
		// Notify sender of the error
		data, _ := json.Marshal(map[string]string{"error": err.Error()})
		sender.Send(&proto.Message{
			Cmd:       proto.CmdFriendRequest,
			MsgId:     r.snow.Next(),
			From:      msg.To,
			To:        sender.UID,
			Content:   string(data),
			Timestamp: time.Now().UnixMilli(),
		})
		return
	}

	log.Printf("[router] friend request: %s → %s", sender.UID, msg.To)

	// Notify the target if online (local or cross-gateway).
	notify := &proto.Message{
		Cmd:       proto.CmdFriendRequest,
		MsgId:     r.snow.Next(),
		From:      sender.UID,
		To:        msg.To,
		Content:   fmt.Sprintf(`{"from_uid":"%s","username":"%s"}`, sender.UID, sender.UID),
		Timestamp: time.Now().UnixMilli(),
	}
	r.routeOrStoreOffline(ctx, msg.To, notify)

	// ACK to sender
	sender.Send(&proto.Message{
		Cmd:       proto.CmdFriendRequest,
		MsgId:     r.snow.Next(),
		From:      msg.To,
		To:        sender.UID,
		Content:   `{"status":"sent"}`,
		Timestamp: time.Now().UnixMilli(),
	})
}

// handleFriendResponse processes a response to a friend request (accept or reject).
// The response status is in msg.Content as JSON: {"action":"accept"} or {"action":"reject"}.
func (r *Router) handleFriendResponse(ctx context.Context, sender *Client, msg *proto.Message) {
	if r.friendStore == nil {
		log.Printf("[router] friend response from %s dropped: friend store not available", sender.UID)
		return
	}
	msg.From = sender.UID

	// Parse action from content.
	var payload struct {
		Action string `json:"action"`
	}
	action := "accept" // default
	if msg.Content != "" {
		if err := json.Unmarshal([]byte(msg.Content), &payload); err == nil && payload.Action != "" {
			action = payload.Action
		}
	}

	targetUID := msg.To // the original requester
	if targetUID == "" || targetUID == sender.UID {
		log.Printf("[router] friend response from %s dropped: invalid target", sender.UID)
		return
	}

	var err error
	switch action {
	case "accept":
		err = r.friendStore.AcceptRequest(ctx, sender.UID, targetUID)
	case "reject":
		err = r.friendStore.RejectRequest(ctx, sender.UID, targetUID)
	default:
		err = fmt.Errorf("unknown action: %s", action)
	}

	if err != nil {
		log.Printf("[router] friend response from %s error: %v", sender.UID, err)
		return
	}

	log.Printf("[router] friend response: %s %s request from %s", sender.UID, action, targetUID)

	// Notify the original requester.
	notify := &proto.Message{
		Cmd:       proto.CmdFriendResponse,
		MsgId:     r.snow.Next(),
		From:      sender.UID,
		To:        targetUID,
		Content:   fmt.Sprintf(`{"action":"%s","from_uid":"%s"}`, action, sender.UID),
		Timestamp: time.Now().UnixMilli(),
	}
	r.routeOrStoreOffline(ctx, targetUID, notify)
}

// handleTyping forwards a typing indicator to the target user or group members.
// Typing events are ephemeral — they are never persisted and only forwarded to online peers.
func (r *Router) handleTyping(ctx context.Context, sender *Client, msg *proto.Message) {
	if msg.To == "" || msg.To == sender.UID {
		return
	}

	// Overwrite From with authenticated sender.
	msg.From = sender.UID

	// Single chat: forward to the target.
	if msg.ChatType != proto.ChatTypeGroup || r.groupStore == nil {
		target := r.clients.Get(ctx, msg.To)
		if target != nil {
			target.Send(msg)
		}
		return
	}

	// Group chat: fan out to all online members except sender.
	members, err := r.groupStore.GetMembers(ctx, msg.To)
	if err != nil {
		return
	}
	for _, memberUID := range members {
		if memberUID == sender.UID {
			continue
		}
		target := r.clients.Get(ctx, memberUID)
		if target != nil {
			target.Send(msg)
		}
	}
}

// handleForward forwards a message to another conversation. The sender
// provides the message to forward (in msg.Content) and the target in msg.To.
// A new MsgID is assigned so the forwarded message is distinct.
func (r *Router) handleForward(ctx context.Context, sender *Client, msg *proto.Message) {
	if err := msg.Validate(); err != nil {
		log.Printf("[router] forward from %s invalid: %v", sender.UID, err)
		return
	}
	if msg.To == sender.UID {
		log.Printf("[router] forward from %s dropped: self-target", sender.UID)
		return
	}
	if msg.Content == "" {
		log.Printf("[router] forward from %s dropped: empty content", sender.UID)
		return
	}

	// Overwrite From with authenticated sender (security).
	msg.From = sender.UID

	// Assign a new MsgID and timestamp.
	msg.MsgId = r.snow.Next()
	msg.Timestamp = time.Now().UnixMilli()

	// Deliver to the target(s) — same pipeline as handleChat.
	if msg.ChatType == proto.ChatTypeGroup && r.groupStore != nil {
		r.fanoutGroup(ctx, sender, msg)
	} else {
		target := r.clients.Get(ctx, msg.To)
		if target != nil {
			if err := target.Send(msg); err != nil {
				log.Printf("[router] forward send to %s failed, storing offline: %v", msg.To, err)
				r.offline.StoreOffline(ctx, msg.To, msg)
			}
		} else {
			r.routeOrStoreOffline(ctx, msg.To, msg)
		}
	}

	// Increment unread for target(s).
	if r.unreadTracker != nil {
		if msg.ChatType == proto.ChatTypeGroup && r.groupStore != nil {
			members, err := r.groupStore.GetMembers(ctx, msg.To)
			if err == nil {
				for _, memberUID := range members {
					if memberUID != sender.UID {
						r.unreadTracker.Increment(ctx, memberUID, sender.UID)
					}
				}
			}
		} else if msg.To != sender.UID {
			r.unreadTracker.Increment(ctx, msg.To, sender.UID)
		}
	}

	// ACK the forward request.
	if msg.NeedAck {
		ack := &proto.Message{
			Cmd:       proto.CmdAck,
			MsgId:     msg.MsgId,
			Seq:       msg.Seq,
			To:        sender.UID,
			Timestamp: time.Now().UnixMilli(),
		}
		sender.Send(ack)
	}

	// Persist the forwarded message asynchronously.
	r.persistAsync(ctx, msg)

	log.Printf("[router] forward from %s to %s (new msgId=%d)", sender.UID, msg.To, msg.MsgId)
}

// handleEdit processes a message edit request. The sender asks to edit a
// previously sent message. Seq carries the original message's MsgID.
// The edit notification is forwarded to the target peer.
func (r *Router) handleEdit(ctx context.Context, sender *Client, msg *proto.Message) {
	if err := msg.Validate(); err != nil {
		log.Printf("[router] edit from %s invalid: %v", sender.UID, err)
		return
	}
	if msg.To == sender.UID {
		log.Printf("[router] edit from %s dropped: self-target", sender.UID)
		return
	}
	if msg.Seq == 0 {
		log.Printf("[router] edit from %s dropped: missing original MsgId (seq=0)", sender.UID)
		sender.Send(&proto.Message{
			Cmd:       proto.CmdEdit,
			MsgId:     r.snow.Next(),
			To:        sender.UID,
			Content:   `{"error":"missing original message ID"}`,
			Timestamp: time.Now().UnixMilli(),
		})
		return
	}
	if msg.Content == "" {
		log.Printf("[router] edit from %s dropped: empty content", sender.UID)
		return
	}

	// Overwrite From with authenticated sender.
	msg.From = sender.UID

	// Record the original MsgID before overwriting Seq.
	originalMsgID := msg.Seq

	// Assign a new MsgID for the edit notification.
	msg.MsgId = r.snow.Next()
	msg.Timestamp = time.Now().UnixMilli()

	// Build the edit notification for the target peer.
	editContent, _ := json.Marshal(map[string]interface{}{
		"edited":   true,
		"msg_id":   originalMsgID,
		"new_text": msg.Content,
	})
	msg.Content = string(editContent)

	// Try local delivery of the edit notification.
	if msg.ChatType == proto.ChatTypeGroup && r.groupStore != nil {
		r.fanoutGroup(ctx, sender, msg)
	} else {
		target := r.clients.Get(ctx, msg.To)
		if target != nil {
			if err := target.Send(msg); err != nil {
				log.Printf("[router] edit send to %s failed, storing offline: %v", msg.To, err)
				r.offline.StoreOffline(ctx, msg.To, msg)
			}
		} else {
			r.routeOrStoreOffline(ctx, msg.To, msg)
		}
	}

	// Persist the edit: update content in MySQL if available.
	// Ownership is verified against the original message's from_uid.
	if r.msgStore != nil {
		if err := r.msgStore.UpdateMessageContent(ctx, originalMsgID, sender.UID, msg.Content); err != nil {
			log.Printf("[router] edit persist error for msg=%d: %v", originalMsgID, err)
		}
	}

	// No ACK for edit — the edit notification itself is the confirmation.

	log.Printf("[router] edit from %s for msg=%d sent to %s", sender.UID, originalMsgID, msg.To)
}
