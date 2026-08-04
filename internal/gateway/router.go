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

// Router 处理消息路由逻辑。
type Router struct {
	clients     ClientRegistry
	offline     OfflineStore
	snow        *snowflake.Generator
	dedup       *DedupCache
	rateLimit   *RateLimiter
	rateLimitMu sync.Mutex        // 在 SetRateLimit 重配置期间保护 rateLimit 字段
	msgStore    repo.MessageStore // MySQL 禁用时为 nil
	kafka       *mq.Producer      // Kafka 禁用时为 nil
	logicClient *LogicClient      // gRPC Logic 服务禁用时为 nil

	// 多网关水平扩展(nil/空 = 单节点模式)。
	hashRing   *HashRing // 多网关禁用时为 nil
	forwarder  Forwarder // 多网关禁用时为 nil
	thisNodeID string    // 多网关禁用时为 ""

	// 群聊支持(nil = 群聊禁用)。
	groupStore GroupStore // 群聊未初始化时为 nil

	// 已读/未读回执跟踪(nil = 跟踪禁用)。
	unreadTracker UnreadTracker // 未读跟踪未初始化时为 nil

	// 好友关系管理(nil = 好友系统禁用)。
	friendStore repo.FriendStore // MySQL 禁用时为 nil

	// persistSem 限制并发异步持久化 goroutine 的数量,
	// 防止高消息吞吐下 goroutine 无限增长。
	persistSem chan struct{}

	// 可配置的运行参数(原为硬编码常量)。
	recallWindow int64         // 消息撤回窗口(毫秒)
	historyLimit int           // 默认历史记录分页大小
	searchLimit  int           // 默认搜索结果上限
	rlCleanup    time.Duration // 限流器过期桶清理间隔
}

// SetKafkaProducer 注入用于异步消息持久化的 Kafka 生产者。
// 为 nil(默认值)时不使用 Kafka。
func (r *Router) SetKafkaProducer(p *mq.Producer) {
	r.kafka = p
}

// SetLogicClient 注入指向 Logic 服务的 gRPC 客户端(历史记录、用户查询)。
// 为 nil(默认值)时使用本地 MessageStore。
func (r *Router) SetLogicClient(c *LogicClient) {
	r.logicClient = c
}

// SetHashRing 注入用于多网关路由的一致性哈希环。
// 为 nil(默认值)时,所有消息都在本地投递或存储。
func (r *Router) SetHashRing(hr *HashRing) {
	r.hashRing = hr
}

// SetForwarder 注入跨网关消息转发器。
// 为 nil(默认值)时,Router 不会尝试转发给对端。
func (r *Router) SetForwarder(f Forwarder) {
	r.forwarder = f
}

// SetThisNodeID 设置本地 Gateway 的节点 ID,用于哈希环比较。
func (r *Router) SetThisNodeID(id string) {
	r.thisNodeID = id
}

// SetGroupStore 注入用于群聊消息扇出的 GroupStore。
// 为 nil(默认值)时,群聊消息按单聊处理。
func (r *Router) SetGroupStore(gs GroupStore) {
	r.groupStore = gs
}

// SetUnreadTracker 注入用于已读回执和未读计数支持的 UnreadTracker。
// 为 nil(默认值)时,未读跟踪被禁用。
func (r *Router) SetUnreadTracker(ut UnreadTracker) {
	r.unreadTracker = ut
}

// SetDedupRedis 为去重缓存启用基于 Redis 的持久化。
// 为 nil(默认值)时,去重仅使用内存。请在启动期间、
// 服务器开始接受连接之前调用。
func (r *Router) SetDedupRedis(rdb *redis.Client) {
	r.dedup.SetRedis(rdb)
}

// SetFriendStore 注入用于好友请求/响应处理的 FriendStore。
// 为 nil(默认值)时,好友管理不可用。
func (r *Router) SetFriendStore(fs repo.FriendStore) {
	r.friendStore = fs
}

// RouterConfig 保存 Router 可调运行参数。
// 这些参数原为硬编码常量,现在可配置。
type RouterConfig struct {
	DedupTTL            time.Duration // 去重缓存条目 TTL,默认 5 分钟
	PersistConcurrency  int           // 最大并发异步持久化 goroutine 数,默认 64
	RecallWindowMs      int64         // 消息撤回窗口(毫秒),默认 120000
	HistoryDefaultLimit int           // 默认历史记录分页大小,默认 30
	SearchDefaultLimit  int           // 默认搜索结果上限,默认 20
	RateLimitCleanup    time.Duration // 限流器过期桶清理间隔,默认 5 分钟
}

// DefaultRouterConfig 返回 RouterConfig 的合理默认值。
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

// NewRouter 创建一个 Router。限流通过 SetRateLimitConfig 单独设置。
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

// SetRateLimit 配置限流。当 rate <= 0 时限流被禁用。
// 会停止先前运行的 RateLimiter,防止 goroutine 泄漏。
func (r *Router) SetRateLimit(rate, burst int) {
	r.rateLimitMu.Lock()
	defer r.rateLimitMu.Unlock()

	// 停止旧的限流器,防止 goroutine 泄漏。
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

// Search 通过本地 MessageStore 执行全文搜索。
// 未配置 MessageStore 时返回 nil, nil。
func (r *Router) Search(ctx context.Context, params *repo.SearchParams) (*repo.SearchResult, error) {
	if r.msgStore == nil {
		return nil, nil
	}
	return r.msgStore.SearchMessages(ctx, params)
}

// Stop 优雅地停止 Router 拥有的后台 goroutine
// (去重缓存清理、限流器清理)。
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

// checkRateLimit 如果用户超出限流则返回 true。
// 线程安全:获取 rateLimitMu 以读取限流器指针。
func (r *Router) checkRateLimit(uid string) bool {
	r.rateLimitMu.Lock()
	rl := r.rateLimit
	r.rateLimitMu.Unlock()
	if rl != nil {
		return !rl.Allow(uid)
	}
	return false
}

// persistAsync 将消息异步持久化到 Kafka 或 MySQL。
// 它是非阻塞的尽力而为操作 —— 失败仅记录日志,绝不
// 传递给调用方。信号量限制并发持久化 goroutine。
func (r *Router) persistAsync(ctx context.Context, msg *proto.Message) {
	// Kafka 异步持久化(即发即忘)。
	if r.kafka != nil {
		msgCopy := pbproto.Clone(msg).(*proto.Message)
		log.Printf("[router] persistAsync: publishing msgId=%d cmd=%d via Kafka", msg.MsgId, msg.Cmd)
		go func() {
			r.persistSem <- struct{}{}
			defer func() { <-r.persistSem }()
			r.kafka.Publish(context.WithoutCancel(ctx), msgCopy)
		}()
	}
	// 直接写 MySQL 作为安全网(可用时始终尝试)。
	// 这确保即使 Kafka 出问题,消息也不会丢失。
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

// Route 分发来自客户端的消息。
func (r *Router) Route(ctx context.Context, sender *Client, msg *proto.Message) {
	// 尽早拒绝未初始化或超出范围的命令。这会捕获
	// protobuf 零值(CmdNone=0),否则它会被静默丢弃。
	if err := msg.Validate(); err != nil {
		log.Printf("[router] invalid message from %s: %v (cmd=%d)", sender.UID, err, msg.Cmd)
		return
	}

	switch msg.Cmd {
	case proto.CmdNone:
		// 不应到达此处 —— Validate() 会拒绝 CmdNone。保留作为
		// 绕过校验的消息(如内部构造)的防御性回退。
		log.Printf("[router] received CmdNone (uninitialized message) from %s — this should not happen", sender.UID)
	case proto.CmdHeartbeat:
		r.handleHeartbeat(ctx, sender, msg)
	case proto.CmdChat:
		r.handleChat(ctx, sender, msg)
	case proto.CmdFile:
		r.handleChat(ctx, sender, msg) // 文件消息走完整的聊天处理流程
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
		// 仅服务器发起;客户端不应发送此命令
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

// handleRecall 处理消息撤回请求。发送方请求删除一条已发送的消息。
// Seq 携带原消息的 MsgID。
// 仅支持单聊撤回;发送方必须是原消息作者,
// 且消息必须在 2 分钟撤回窗口内。
func (r *Router) handleRecall(ctx context.Context, sender *Client, msg *proto.Message) {
	// 校验基本字段(Cmd 范围、必须有目标)。
	if err := msg.Validate(); err != nil {
		log.Printf("[router] recall from %s invalid: %v", sender.UID, err)
		return
	}
	// 不允许对自己发起撤回。
	if msg.To == sender.UID {
		log.Printf("[router] recall from %s dropped: self-target", sender.UID)
		return
	}
	// Seq 必须携带原消息的 MsgID。
	if msg.Seq == 0 {
		log.Printf("[router] recall from %s dropped: missing original MsgId (seq=0)", sender.UID)
		r.sendRecallError(sender, "missing original message ID")
		return
	}

	// 安全:用已认证的发送方 UID 覆盖 From 字段。
	msg.From = sender.UID

	// 在持久化存储中将原消息标记为已撤回。
	// 撤回窗口由存储层通过消息时间戳强制执行。
	if r.msgStore != nil {
		if err := r.msgStore.RecallMessage(ctx, msg.Seq, sender.UID, r.recallWindow); err != nil {
			log.Printf("[router] recall from %s for msg=%d failed: %v", sender.UID, msg.Seq, err)
			r.sendRecallError(sender, err.Error())
			return
		}
	}

	// 为撤回通知分配消息 ID 和时间戳。
	msg.MsgId = r.snow.Next()
	msg.Timestamp = time.Now().UnixMilli()

	// 为对端构建撤回通知。
	// Seq 字段携带原消息的 MsgID,使客户端知道要移除哪条消息。
	// Content 有意留空 —— CmdRecall 本身就是信号。
	msg.Content = fmt.Sprintf(`{"recalled":true,"msg_id":%d}`, msg.Seq)

	// 先尝试本地投递。
	target := r.clients.Get(ctx, msg.To)
	if target != nil {
		if err := target.Send(msg); err != nil {
			log.Printf("[router] recall send to %s failed, storing offline: %v", msg.To, err)
			r.offline.StoreOffline(ctx, msg.To, msg)
		}
		return
	}

	// 目标不在本地 —— 转发到对端 Gateway 或存储离线。
	r.routeOrStoreOffline(ctx, msg.To, msg)
}

// sendRecallError 为失败的撤回请求发送错误响应。
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
	// --- 去重检查 ---
	if msg.Seq > 0 {
		if isDup, existingMsgID := r.dedup.IsDuplicate(sender.UID, msg.Seq); isDup {
			// 用先前分配的 MsgID 重新发送 ACK
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

	// --- 校验 ---
	if err := msg.Validate(); err != nil {
		log.Printf("[router] invalid message from %s: %v", sender.UID, err)
		return
	}

	// --- 限流 ---
	if r.checkRateLimit(sender.UID) {
		log.Printf("[router] rate limited uid=%s", sender.UID)
		return
	}

	// 分配全局唯一的消息 ID
	msg.MsgId = r.snow.Next()
	msg.Timestamp = time.Now().UnixMilli()

	// 投递给目标。
	// 注意:去重 Mark 在投递之后调用,而非之前。
	// 如果发送缓冲区已满而我们先做了标记,客户端的重试会被静默丢弃
	// (消息丢失)。投递后再标记可确保在线目标的至少一次语义。
	if msg.ChatType == proto.ChatTypeGroup && r.groupStore != nil {
		r.fanoutGroup(ctx, sender, msg)
		// 群扇出:所有成员处理完后再标记。
		if msg.Seq > 0 {
			r.dedup.Mark(sender.UID, msg.Seq, msg.MsgId)
		}
	} else {
		target := r.clients.Get(ctx, msg.To)
		if target != nil {
			if err := target.Send(msg); err != nil {
				// 发送失败(缓冲区满)—— 转存离线,但暂不标记,
				// 以便客户端重试在线投递。
				log.Printf("[router] send failed for %s, storing offline: %v", msg.To, err)
				r.offline.StoreOffline(ctx, msg.To, msg)
			} else if msg.Seq > 0 {
				// 在线投递成功 —— 可以安全标记。
				r.dedup.Mark(sender.UID, msg.Seq, msg.MsgId)
			}
		} else {
			// 目标不在本地在线 —— 路由或转存离线。
			r.routeOrStoreOffline(ctx, msg.To, msg)
			if msg.Seq > 0 {
				r.dedup.Mark(sender.UID, msg.Seq, msg.MsgId)
			}
		}
	}

	// 增加目标的未读计数。
	// 单聊:目标获得来自发送方的 +1 未读。
	// 群聊:除发送方外的所有成员获得来自发送方的 +1 未读。
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

	// 向发送方发送 ACK
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

// 当目标未在本地连接时,routeOrStoreOffline 决定消息的投递或存储位置。
// 单节点模式(未配置哈希环)下,消息在本地转存离线。
// 多节点模式下,哈希环决定归属节点:如果是本节点则转存离线;
// 否则转发给对端。如果转发失败,消息在本地转存离线作为回退。
func (r *Router) routeOrStoreOffline(ctx context.Context, targetUID string, msg *proto.Message) {
	// 未配置多网关 —— 在本地转存离线(向后兼容)。
	if r.hashRing == nil || r.thisNodeID == "" {
		r.offline.StoreOffline(ctx, targetUID, msg)
		log.Printf("[router] stored offline message for %s from %s", targetUID, msg.From)
		return
	}

	ownerNode := r.hashRing.Get(targetUID)
	if ownerNode == "" || ownerNode == r.thisNodeID {
		// 环为空或本节点拥有该用户 —— 用户确实离线。
		r.offline.StoreOffline(ctx, targetUID, msg)
		log.Printf("[router] stored offline message for %s from %s (this node owns %s)",
			targetUID, msg.From, targetUID)
		return
	}

	// 转发给拥有目标用户的对端 Gateway。
	if r.forwarder == nil {
		log.Printf("[router] forwarder not configured, storing offline locally for %s", targetUID)
		r.offline.StoreOffline(ctx, targetUID, msg)
		return
	}

	delivered, err := r.forwarder.Forward(ctx, targetUID, msg)
	if err != nil {
		// 转发失败(网络错误、对端宕机)—— 回退到本地离线存储。
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

// fanoutGroup 将群聊消息投递给除发送方外的所有成员。
// 每个成员独立投递:在线成员实时推送;离线成员转存;
// 一个成员失败不影响其他成员。
func (r *Router) fanoutGroup(ctx context.Context, sender *Client, msg *proto.Message) {
	members, err := r.groupStore.GetMembers(ctx, msg.To)
	if err != nil {
		log.Printf("[router] group chat: get members for group %s failed: %v", msg.To, err)
		return
	}

	delivered := 0
	for _, memberUID := range members {
		if memberUID == sender.UID {
			continue // 不投递给自己
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
			// 成员未在本地连接 —— 转发或转存离线。
			r.routeOrStoreOffline(ctx, memberUID, msg)
		}
	}

	log.Printf("[router] group chat: fanout for group %s — %d/%d members delivered online",
		msg.To, delivered, len(members)-1)
}

// fanoutGroupWithMembers 将消息投递给显式给定的群成员列表。
// 与 fanoutGroup 不同,它不查询本地 GroupStore —— 由调用方直接
// 提供成员 UID。当群成员关系由外部管理时(例如通过 gRPC Logic 服务)
// 使用此方法,以避免双重写入。
func (r *Router) fanoutGroupWithMembers(ctx context.Context, sender *Client, msg *proto.Message, members []string) {
	delivered := 0
	for _, memberUID := range members {
		if memberUID == sender.UID {
			continue // 不投递给自己
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
			// 成员未在本地连接 —— 转发或转存离线。
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
			// 将未送达的消息重新放回离线存储。
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
	// 校验 —— 确保 To 字段存在。
	if err := msg.Validate(); err != nil {
		log.Printf("[router] invalid history request from %s: %v", sender.UID, err)
		return
	}

	// 从消息中解析分页参数。
	limit := int(msg.Seq)
	if limit <= 0 {
		limit = r.historyLimit // 默认分页大小
	}
	if limit > 100 {
		limit = 100 // 上限
	}

	before := msg.Timestamp
	if before <= 0 {
		before = time.Now().UnixMilli()
	}

	// 查询会话历史。
	// 优先 gRPC Logic 服务,回退到本地 MessageStore,再退化为空。
	// 群聊历史直接走本地 MessageStore(gRPC 路径是第 3 步)。
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
		// 完全没有持久化层 —— 返回空完成信号。
		sender.Send(&proto.Message{
			Cmd:       proto.CmdHistory,
			MsgId:     r.snow.Next(),
			Timestamp: time.Now().UnixMilli(),
		})
		return
	}

	// 逐条发送历史消息,保留原始 MsgId、From、Timestamp 等。
	delivered := 0
	for _, m := range msgs {
		if err := sender.Send(m); err != nil {
			log.Printf("[router] send buffer full for %s during history, sent %d/%d",
				sender.UID, delivered, len(msgs))
			break
		}
		delivered++
	}

	// 在 Seq 中携带已投递消息数量作为完成信号。
	sender.Send(&proto.Message{
		Cmd:       proto.CmdHistory,
		Seq:       int64(delivered),
		MsgId:     r.snow.Next(),
		Timestamp: time.Now().UnixMilli(),
	})

	log.Printf("[router] delivered %d history messages to %s (with=%s)", delivered, sender.UID, msg.To)
}

// handleReadReceipt 处理来自客户端的已读回执。
// 它清除对方发给阅读者的未读计数,然后将回执转发给原发送方(peerUID),
// 使对方客户端知道消息已被阅读。
// 已读回执是临时的:如果对方离线,回执会被丢弃。
func (r *Router) handleReadReceipt(ctx context.Context, sender *Client, msg *proto.Message) {
	peerUID := msg.To // 被发送方阅读了消息的用户

	// 校验:peerUID 必填且不能与发送方相同。
	if peerUID == "" || peerUID == sender.UID {
		log.Printf("[router] invalid read receipt from %s: peer=%q", sender.UID, peerUID)
		return
	}

	// 1. 清除对方发给阅读者(发送方)的未读计数。
	// 优先 gRPC Logic 服务,回退到本地跟踪器。
	if r.logicClient != nil {
		if err := r.logicClient.MarkReadClient(ctx, sender.UID, peerUID); err != nil {
			log.Printf("[router] gRPC MarkRead error: %v", err)
			// gRPC 失败时回退到本地跟踪器。
			if r.unreadTracker != nil {
				r.unreadTracker.MarkRead(ctx, sender.UID, peerUID)
			}
		}
	} else if r.unreadTracker != nil {
		r.unreadTracker.MarkRead(ctx, sender.UID, peerUID)
	}

	// 2. 为原发送方(peerUID)构建已读回执通知。
	receipt := &proto.Message{
		Cmd:       proto.CmdReadReceipt,
		MsgId:     r.snow.Next(),
		From:      sender.UID, // 谁阅读了消息
		To:        peerUID,    // 应被通知的人
		Seq:       msg.MsgId,  // 携带最后一条已读消息的 ID
		Timestamp: time.Now().UnixMilli(),
	}

	// 3. 先尝试本地投递。
	target := r.clients.Get(ctx, peerUID)
	if target != nil {
		if err := target.Send(receipt); err != nil {
			log.Printf("[router] read receipt send to %s failed: %v", peerUID, err)
		}
		return
	}

	// 4. 尝试通过哈希环跨网关转发。
	if r.hashRing != nil && r.thisNodeID != "" {
		ownerNode := r.hashRing.Get(peerUID)
		if ownerNode != "" && ownerNode != r.thisNodeID && r.forwarder != nil {
			if _, err := r.forwarder.Forward(ctx, peerUID, receipt); err != nil {
				log.Printf("[router] read receipt forward to %s failed: %v", peerUID, err)
			}
			return
		}
	}

	// 5. 对方离线或不可达 —— 丢弃回执。
	log.Printf("[router] read receipt from %s for %s dropped (peer offline)",
		sender.UID, peerUID)
}

// handleUnreadCount 返回请求用户的各会话未读计数。
func (r *Router) handleUnreadCount(ctx context.Context, sender *Client, msg *proto.Message) {
	// 优先 gRPC Logic 服务,回退到本地跟踪器。
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

// handleSearch 对消息内容执行全文搜索。
// 搜索查询和过滤条件以 JSON 编码在 msg.Content 中。
// 结果逐条发送,随后发送完成信号。
func (r *Router) handleSearch(ctx context.Context, sender *Client, msg *proto.Message) {
	// 从 Content(JSON)中解析搜索参数。
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

	// 默认值。
	if params.Limit <= 0 {
		params.Limit = r.searchLimit
	}
	if params.Limit > 50 {
		params.Limit = 50
	}

	// 优先 gRPC Logic 服务,回退到本地 MessageStore。
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
		// 无结果 —— 发送空的完成信号。
		sender.Send(&proto.Message{
			Cmd:       proto.CmdSearch,
			MsgId:     r.snow.Next(),
			Seq:       0,
			Timestamp: time.Now().UnixMilli(),
		})
		return
	}

	// 逐条发送匹配的消息。
	delivered := 0
	for _, m := range result.Messages {
		if err := sender.Send(m); err != nil {
			break
		}
		delivered++
	}

	// 发送带数量和下一个游标的完成信号。
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

// sendGroupNotification 为群事件(成员加入/离开)构造系统通知,
// 并通过扇出 + 持久化投递给所有群成员。
// 它从本地 GroupStore 获取成员列表。
func (r *Router) sendGroupNotification(ctx context.Context, fromUID, groupID, notifType string) {
	r.sendGroupNotificationWithMembers(ctx, fromUID, groupID, notifType, nil)
}

// sendGroupNotificationWithMembers 与 sendGroupNotification 类似,但当提供了
// 显式成员列表时使用之(members != nil)。这避免了与本地 GroupStore 的
// 往返,是群成员关系由外部管理(例如通过 gRPC Logic 服务)时的首选路径。
// 当 members 为 nil 时,回退到从本地 GroupStore 获取成员列表。
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
		sender = &Client{UID: fromUID} // 最小客户端,用于扇出时跳过自己
	}
	if members != nil {
		r.fanoutGroupWithMembers(ctx, sender, notif, members)
	} else {
		r.fanoutGroup(ctx, sender, notif)
	}
	r.persistAsync(ctx, notif)
}

// --- 群管理处理器(Phase 5 实现)---

// handleGroupCreate 创建新群。发送方成为群主和第一个成员。
// 请求:Content = {"name": "My Group"}
// 响应:Content = {"id":"g_123","name":"My Group","owner_uid":"alice","members":["alice"],"created_at":123}
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

	// 优先尝试 gRPC Logic 服务。
	if r.logicClient != nil {
		groupInfo, err := r.logicClient.CreateGroupClient(ctx, req.Name, sender.UID)
		if err != nil {
			r.sendGroupError(sender, proto.CmdGroupCreate, err.Error())
			return
		}
		if groupInfo != nil {
			// 通过 gRPC 添加初始成员(群主已是成员)。
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
			// 通知被邀请的成员(跳过自己)。
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

	// 回退到本地 GroupStore。
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

	// 通知被邀请的成员(跳过自己)。
	if len(req.Members) > 0 {
		r.sendGroupNotificationWithMembers(ctx, sender.UID, group.ID, "member_joined", members)
	}

	log.Printf("[router] group created: id=%s name=%q owner=%s members=%d", group.ID, req.Name, sender.UID, len(members))
}

// handleGroupJoin 将发送方加入群。
// 请求:To = group_id,Content = 可选(未使用)
// 响应:Content = {"group_id":"g_123","uid":"bob","members":["alice","bob"]}
func (r *Router) handleGroupJoin(ctx context.Context, sender *Client, msg *proto.Message) {
	groupID := msg.To
	if groupID == "" {
		r.sendGroupError(sender, proto.CmdGroupJoin, "group_id is required (set 'to' field)")
		return
	}

	// 优先尝试 gRPC Logic 服务。
	if r.logicClient != nil {
		if err := r.logicClient.JoinGroupClient(ctx, groupID, sender.UID); err != nil {
			r.sendGroupError(sender, proto.CmdGroupJoin, err.Error())
			return
		}
		// 获取成员列表用于响应。
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
		// 使用 gRPC 返回的成员列表进行通知扇出,而不是写入
		// 本地 GroupStore —— 避免 groupStore 基于 MySQL 时的双重写入。
		r.sendGroupNotificationWithMembers(ctx, sender.UID, groupID, "member_joined", members)
		log.Printf("[router] %s joined group %s via gRPC", sender.UID, groupID)
		return
	}

	// 回退到本地 GroupStore。
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

	// 通知所有群成员有新成员加入。
	r.sendGroupNotification(ctx, sender.UID, groupID, "member_joined")

	log.Printf("[router] %s joined group %s", sender.UID, groupID)
}

// handleGroupInviteMember 邀请第三方用户入群。只有群主可以邀请。
// 请求:To = group_id,Content = {"target_uid":"bob"}
// 响应:Content = {"group_id":"g_123","target_uid":"bob","inviter_uid":"alice","members":[...]}
func (r *Router) handleGroupInviteMember(ctx context.Context, sender *Client, msg *proto.Message) {
	groupID := msg.To
	if groupID == "" {
		r.sendGroupError(sender, proto.CmdGroupInviteMember, "group_id is required (set 'to' field)")
		return
	}

	// 从内容中解析 target_uid。
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

	// 优先尝试 gRPC Logic 服务。
	if r.logicClient != nil {
		// 校验发送方是群主。
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

		// 添加目标用户。
		if err := r.logicClient.JoinGroupClient(ctx, groupID, targetUID); err != nil {
			r.sendGroupError(sender, proto.CmdGroupInviteMember, err.Error())
			return
		}

		// 获取更新后的成员列表。
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
		// 通知所有成员有新成员加入。
		r.sendGroupNotificationWithMembers(ctx, targetUID, groupID, "member_joined", members)
		log.Printf("[router] %s invited %s to group %s via gRPC", sender.UID, targetUID, groupID)
		return
	}

	// 回退到本地 GroupStore。
	if r.groupStore == nil {
		r.sendGroupError(sender, proto.CmdGroupInviteMember, "group chat not enabled")
		return
	}

	// 校验发送方是群主。
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

	// 添加目标用户。
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

	// 通知所有群成员有新成员加入。
	r.sendGroupNotification(ctx, targetUID, groupID, "member_joined")

	log.Printf("[router] %s invited %s to group %s", sender.UID, targetUID, groupID)
}

// handleGroupLeave 将发送方移出群。如果群变为空,则删除该群。
// 请求:To = group_id
// 响应:Content = {"group_id":"g_123","uid":"bob","deleted":false}
func (r *Router) handleGroupLeave(ctx context.Context, sender *Client, msg *proto.Message) {
	groupID := msg.To
	if groupID == "" {
		r.sendGroupError(sender, proto.CmdGroupLeave, "group_id is required (set 'to' field)")
		return
	}

	// 优先尝试 gRPC Logic 服务。
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
		// 从 gRPC 获取剩余成员用于通知扇出,而不是写入
		// 本地 GroupStore —— 避免与基于 MySQL 的存储双重写入。
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

	// 回退到本地 GroupStore。
	if r.groupStore == nil {
		r.sendGroupError(sender, proto.CmdGroupLeave, "group chat not enabled")
		return
	}

	if err := r.groupStore.RemoveMember(ctx, groupID, sender.UID); err != nil {
		r.sendGroupError(sender, proto.CmdGroupLeave, err.Error())
		return
	}

	// 检查群是否仍然存在(最后一名成员退出时群被删除)。
	_, getErr := r.groupStore.Get(ctx, groupID)
	wasDeleted := getErr != nil
	data, _ := json.Marshal(map[string]interface{}{
		"group_id": groupID,
		"uid":      sender.UID,
		"deleted":  wasDeleted, // 群被删除时为 true(最后一名成员已退出)
	})

	sender.Send(&proto.Message{
		Cmd:       proto.CmdGroupLeave,
		MsgId:     r.snow.Next(),
		Content:   string(data),
		Timestamp: time.Now().UnixMilli(),
	})

	// 通知剩余群成员有人离开。
	// 仅在群仍然存在(还有剩余成员)时发送。
	if !wasDeleted {
		r.sendGroupNotification(ctx, sender.UID, groupID, "member_left")
	}

	log.Printf("[router] %s left group %s", sender.UID, groupID)
}

// handleGroupInfo 返回完整的群信息,包括成员列表。
// 请求:To = group_id
// 响应:Content = {"id":"g_123","name":"My Group","owner_uid":"alice","members":["alice","bob"],"created_at":123}
func (r *Router) handleGroupInfo(ctx context.Context, sender *Client, msg *proto.Message) {
	groupID := msg.To
	if groupID == "" {
		r.sendGroupError(sender, proto.CmdGroupInfo, "group_id is required (set 'to' field)")
		return
	}

	// 优先尝试 gRPC Logic 服务。
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

	// 回退到本地 GroupStore。
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

// handleGroupList 返回发送方所属的所有群。
// 请求:无特殊字段
// 响应:Content = {"uid":"alice","groups":[{"id":"g_1","name":"...","owner_uid":"...","member_count":2,"created_at":123},...]}
func (r *Router) handleGroupList(ctx context.Context, sender *Client, msg *proto.Message) {
	// 优先尝试 gRPC Logic 服务。
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

	// 回退到本地 GroupStore。
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

// sendGroupError 为群管理命令发送错误响应。
func (r *Router) sendGroupError(sender *Client, cmd int32, errMsg string) {
	data, _ := json.Marshal(map[string]string{"error": errMsg})
	sender.Send(&proto.Message{
		Cmd:       cmd,
		MsgId:     r.snow.Next(),
		Content:   string(data),
		Timestamp: time.Now().UnixMilli(),
	})
}

// handleFriendRequest 处理好友请求。发送方请求将目标(msg.To)添加为好友。
// Router 持久化该请求,并在目标在线时将通知转发给目标。
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
		// 通知发送方错误信息
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

	// 如果目标在线(本地或跨网关)则通知它。
	notify := &proto.Message{
		Cmd:       proto.CmdFriendRequest,
		MsgId:     r.snow.Next(),
		From:      sender.UID,
		To:        msg.To,
		Content:   fmt.Sprintf(`{"from_uid":"%s","username":"%s"}`, sender.UID, sender.UID),
		Timestamp: time.Now().UnixMilli(),
	}
	r.routeOrStoreOffline(ctx, msg.To, notify)

	// 向发送方发送 ACK
	sender.Send(&proto.Message{
		Cmd:       proto.CmdFriendRequest,
		MsgId:     r.snow.Next(),
		From:      msg.To,
		To:        sender.UID,
		Content:   `{"status":"sent"}`,
		Timestamp: time.Now().UnixMilli(),
	})
}

// handleFriendResponse 处理好友请求的响应(接受或拒绝)。
// 响应状态在 msg.Content 中,为 JSON:{"action":"accept"} 或 {"action":"reject"}。
func (r *Router) handleFriendResponse(ctx context.Context, sender *Client, msg *proto.Message) {
	if r.friendStore == nil {
		log.Printf("[router] friend response from %s dropped: friend store not available", sender.UID)
		return
	}
	msg.From = sender.UID

	// 从内容中解析动作。
	var payload struct {
		Action string `json:"action"`
	}
	action := "accept" // 默认值
	if msg.Content != "" {
		if err := json.Unmarshal([]byte(msg.Content), &payload); err == nil && payload.Action != "" {
			action = payload.Action
		}
	}

	targetUID := msg.To // 原始请求者
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

	// 通知原始请求者。
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

// handleTyping 将正在输入的状态转发给目标用户或群成员。
// 输入事件是临时的 —— 它们从不持久化,只转发给在线对端。
func (r *Router) handleTyping(ctx context.Context, sender *Client, msg *proto.Message) {
	if msg.To == "" || msg.To == sender.UID {
		return
	}

	// 用已认证的发送方覆盖 From 字段。
	msg.From = sender.UID

	// 单聊:转发给目标。
	if msg.ChatType != proto.ChatTypeGroup || r.groupStore == nil {
		target := r.clients.Get(ctx, msg.To)
		if target != nil {
			target.Send(msg)
		}
		return
	}

	// 群聊:扇出给除发送方外的所有在线成员。
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

// handleForward 将消息转发到另一个会话。发送方在 msg.Content 中
// 提供要转发的消息,在 msg.To 中提供目标。
// 会分配新的 MsgID,使转发后的消息互不相同。
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

	// 用已认证的发送方覆盖 From(安全)。
	msg.From = sender.UID

	// 分配新的 MsgID 和时间戳。
	msg.MsgId = r.snow.Next()
	msg.Timestamp = time.Now().UnixMilli()

	// 投递给目标 —— 与 handleChat 相同的处理流程。
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

	// 增加目标的未读计数。
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

	// 对转发请求发送 ACK。
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

	// 异步持久化转发的消息。
	r.persistAsync(ctx, msg)

	log.Printf("[router] forward from %s to %s (new msgId=%d)", sender.UID, msg.To, msg.MsgId)
}

// handleEdit 处理消息编辑请求。发送方请求编辑一条已发送的消息。
// Seq 携带原消息的 MsgID。
// 编辑通知会转发给目标对端。
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

	// 用已认证的发送方覆盖 From 字段。
	msg.From = sender.UID

	// 在覆盖 Seq 之前记录原始 MsgID。
	originalMsgID := msg.Seq

	// 为编辑通知分配新的 MsgID。
	msg.MsgId = r.snow.Next()
	msg.Timestamp = time.Now().UnixMilli()

	// 为对端构建编辑通知。
	editContent, _ := json.Marshal(map[string]interface{}{
		"edited":   true,
		"msg_id":   originalMsgID,
		"new_text": msg.Content,
	})
	msg.Content = string(editContent)

	// 尝试本地投递编辑通知。
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

	// 持久化编辑:可用时更新 MySQL 中的内容。
	// 所有权会根据原消息的 from_uid 进行校验。
	if r.msgStore != nil {
		if err := r.msgStore.UpdateMessageContent(ctx, originalMsgID, sender.UID, msg.Content); err != nil {
			log.Printf("[router] edit persist error for msg=%d: %v", originalMsgID, err)
		}
	}

	// 编辑无 ACK —— 编辑通知本身就是确认。

	log.Printf("[router] edit from %s for msg=%d sent to %s", sender.UID, originalMsgID, msg.To)
}
