package gateway

import (
	"context"
	"log"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// ClusterManager 处理动态多网关集群:健康检查对端节点,并(可选地)通过 Redis
// 发现它们。它让哈希环和转发器与当前健康、可达的对端集合保持同步。
//
// 支持两种模式:
//   - 静态:对端来自配置(peer_addrs)。健康检查仍然运行,不健康的对端会
//     暂时从哈希环中移除。
//   - Redis 发现:对端通过带 TTL 心跳的 Redis 键被发现。新对端自动加入;
//     过期的对端被移除。
type ClusterManager struct {
	hr        *HashRing
	forwarder *GrpcForwarder
	thisNode  string
	thisAddr  string

	// Redis 服务发现(禁用时为 nil)。
	redis       *redis.Client
	redisPrefix string // 默认为 "im:gateway:node:"
	ttl         time.Duration

	// 健康检查。
	healthInterval time.Duration
	probeTimeout   time.Duration

	// 生命周期。
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 跟踪当前在哈希环中的对端(健康状态)。
	mu         sync.RWMutex
	peerHealth map[string]bool // nodeID → 当前是否健康(在哈希环中)
}

// ClusterConfig 保存 ClusterManager 的设置。
type ClusterConfig struct {
	ThisNodeID     string
	ThisAddr       string
	HealthInterval time.Duration // 0 表示默认为 5s
	ProbeTimeout   time.Duration // 0 表示默认为 2s

	// Redis 服务发现。将 Redis 留空(nil)则仅使用静态 peer_addrs。
	Redis       *redis.Client
	RedisPrefix string        // 空串默认为 "im:gateway:node:"
	TTL         time.Duration // 0 表示默认为 15s
}

// NewClusterManager 创建一个 ClusterManager。
func NewClusterManager(hr *HashRing, forwarder *GrpcForwarder, cfg ClusterConfig) *ClusterManager {
	if cfg.HealthInterval <= 0 {
		cfg.HealthInterval = 5 * time.Second
	}
	if cfg.ProbeTimeout <= 0 {
		cfg.ProbeTimeout = 2 * time.Second
	}
	if cfg.RedisPrefix == "" {
		cfg.RedisPrefix = "im:gateway:node:"
	}
	if cfg.TTL <= 0 {
		cfg.TTL = 15 * time.Second
	}

	return &ClusterManager{
		hr:             hr,
		forwarder:      forwarder,
		thisNode:       cfg.ThisNodeID,
		thisAddr:       cfg.ThisAddr,
		redis:          cfg.Redis,
		redisPrefix:    cfg.RedisPrefix,
		ttl:            cfg.TTL,
		healthInterval: cfg.HealthInterval,
		probeTimeout:   cfg.ProbeTimeout,
		peerHealth:     make(map[string]bool),
	}
}

// Start 启动健康检查以及(配置了 Redis 时)服务发现的后台循环。
// 它只阻塞到在 Redis 中完成注册并执行完初始对端发现。
// 只能调用一次;后续调用为无操作,以防 context 泄漏。
func (cm *ClusterManager) Start(ctx context.Context) {
	if cm.cancel != nil {
		log.Printf("[cluster] Start called but manager is already running — ignored")
		return
	}
	cm.ctx, cm.cancel = context.WithCancel(ctx)

	// 将所有初始对端标记为健康(它们在启动时已加入哈希环)。
	for nodeID := range cm.forwarder.PeerAddrs() {
		if nodeID != cm.thisNode {
			cm.mu.Lock()
			cm.peerHealth[nodeID] = true
			cm.mu.Unlock()
		}
	}

	if cm.redis != nil {
		// 在 Redis 中注册本节点并发现初始对端。
		cm.registerSelf()
		cm.discoverPeers()

		cm.wg.Add(2)
		go cm.refreshLoop()
		go cm.discoveryLoop()
	}

	cm.wg.Add(1)
	go cm.healthCheckLoop()

	log.Printf("[cluster] manager started: node=%s addr=%s peers=%d redis=%v",
		cm.thisNode, cm.thisAddr, len(cm.forwarder.PeerAddrs()), cm.redis != nil)
}

// Stop 优雅地关闭后台循环并从 Redis 注销。
func (cm *ClusterManager) Stop() {
	if cm.cancel != nil {
		cm.cancel()
	}

	// 从 Redis 注销,让对端不再路由消息到本节点。
	// 使用短超时,避免阻塞优雅关闭。
	if cm.redis != nil {
		key := cm.redisPrefix + cm.thisNode
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := cm.redis.Del(ctx, key).Err(); err != nil {
			log.Printf("[cluster] redis deregister error: %v", err)
		}
		cancel()
	}

	cm.wg.Wait()
	log.Printf("[cluster] manager stopped: node=%s", cm.thisNode)
}

// ---------------------------------------------------------------------------
// Redis 服务发现
// ---------------------------------------------------------------------------

// nodeKey 返回给定节点 ID 对应的 Redis 键。
func (cm *ClusterManager) nodeKey(nodeID string) string {
	return cm.redisPrefix + nodeID
}

// registerSelf 在 Redis 中注册本 Gateway 节点并附带 TTL。
func (cm *ClusterManager) registerSelf() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	key := cm.nodeKey(cm.thisNode)
	if err := cm.redis.Set(ctx, key, cm.thisAddr, cm.ttl).Err(); err != nil {
		log.Printf("[cluster] redis register self failed: %v", err)
		return
	}
	log.Printf("[cluster] registered self in Redis: key=%s addr=%s ttl=%s", key, cm.thisAddr, cm.ttl)
}

// refreshLoop 定期刷新本节点在 Redis 中的 TTL。
func (cm *ClusterManager) refreshLoop() {
	defer cm.wg.Done()

	// 按 TTL/3 的间隔刷新,在过期前留出充足余量。
	interval := cm.ttl / 3
	if interval < 2*time.Second {
		interval = 2 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-cm.ctx.Done():
			return
		case <-ticker.C:
			cm.registerSelf()
		}
	}
}

// discoverPeers 扫描 Redis 中所有已注册的 Gateway 节点,并协调本地对端集合:
// 新节点被加入,缺失的节点被移除。
func (cm *ClusterManager) discoverPeers() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pattern := cm.redisPrefix + "*"
	keys, err := cm.redis.Keys(ctx, pattern).Result()
	if err != nil {
		log.Printf("[cluster] redis scan error: %v", err)
		return
	}

	found := make(map[string]string, len(keys)) // nodeID → 地址
	for _, key := range keys {
		nodeID := key[len(cm.redisPrefix):]
		if nodeID == "" {
			continue
		}
		addr, err := cm.redis.Get(ctx, key).Result()
		if err != nil {
			continue // 键在 SCAN 与 GET 之间已过期
		}
		found[nodeID] = addr
	}

	cm.reconcilePeers(found)
}

// reconcilePeers 添加新对端并移除过期对端。
func (cm *ClusterManager) reconcilePeers(found map[string]string) {
	currentPeers := cm.forwarder.PeerAddrs()

	// 添加新对端(并重新添加曾被移除、现已恢复的对端)。
	for nodeID, addr := range found {
		if nodeID == cm.thisNode {
			continue
		}
		if _, exists := currentPeers[nodeID]; !exists {
			log.Printf("[cluster] discovered new peer: %s at %s", nodeID, addr)
			cm.addPeer(nodeID, addr)
		} else if currentPeers[nodeID] != addr {
			// 地址已变更 —— 更新。
			log.Printf("[cluster] peer %s address changed: %s → %s", nodeID, currentPeers[nodeID], addr)
			cm.addPeer(nodeID, addr)
		}
	}

	// 移除已从 Redis 消失的对端。
	for nodeID := range currentPeers {
		if _, exists := found[nodeID]; !exists {
			log.Printf("[cluster] peer %s removed from Redis, removing locally", nodeID)
			cm.removePeer(nodeID)
		}
	}
}

// discoveryLoop 定期扫描 Redis 以发现对端变化。
func (cm *ClusterManager) discoveryLoop() {
	defer cm.wg.Done()

	interval := cm.ttl / 2
	if interval < 3*time.Second {
		interval = 3 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-cm.ctx.Done():
			return
		case <-ticker.C:
			cm.discoverPeers()
		}
	}
}

// ---------------------------------------------------------------------------
// 健康检查
// ---------------------------------------------------------------------------

// healthCheckLoop 定期探测每个已知对端并相应地更新哈希环:
// 不健康的对端被暂时移除,健康的对端被重新加入。
func (cm *ClusterManager) healthCheckLoop() {
	defer cm.wg.Done()

	ticker := time.NewTicker(cm.healthInterval)
	defer ticker.Stop()

	for {
		select {
		case <-cm.ctx.Done():
			return
		case <-ticker.C:
			cm.runHealthCheck()
		}
	}
}

// runHealthCheck 探测所有对端(除自身外)并协调哈希环。
func (cm *ClusterManager) runHealthCheck() {
	currentPeers := cm.forwarder.PeerAddrs()

	for nodeID, addr := range currentPeers {
		if nodeID == cm.thisNode {
			continue
		}

		healthy := cm.probePeer(addr)

		cm.mu.Lock()
		wasHealthy := cm.peerHealth[nodeID]
		cm.mu.Unlock()

		if healthy && !wasHealthy {
			slog.Info("peer recovered, adding back to hash ring", "peer", nodeID)
			cm.hr.Add(nodeID)
			cm.mu.Lock()
			cm.peerHealth[nodeID] = true
			cm.mu.Unlock()
		} else if !healthy && wasHealthy {
			slog.Warn("peer unhealthy, removing from hash ring", "peer", nodeID)
			cm.hr.Remove(nodeID)
			cm.mu.Lock()
			cm.peerHealth[nodeID] = false
			cm.mu.Unlock()
		}
	}
}

// probePeer 尝试以短超时向对端发起 gRPC 拨号。
// 如果对端可达则返回 true。
func (cm *ClusterManager) probePeer(addr string) bool {
	// 创建子 context,以便拨号后可以等待连接就绪。
	ctx, cancel := context.WithTimeout(context.Background(), cm.probeTimeout)
	defer cancel()

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return false
	}
	defer conn.Close()

	// 发起连接并等待其变为就绪状态。
	conn.Connect()
	state := conn.GetState()
	for state != connectivity.Ready {
		if !conn.WaitForStateChange(ctx, state) {
			return false // context 过期或连接已关闭
		}
		state = conn.GetState()
	}
	return true
}

// ---------------------------------------------------------------------------
// 对端管理
// ---------------------------------------------------------------------------

// addPeer 将对端同时加入转发器的地址映射和哈希环。
func (cm *ClusterManager) addPeer(nodeID, addr string) {
	cm.forwarder.AddPeer(nodeID, addr)
	cm.hr.Add(nodeID)
	cm.mu.Lock()
	cm.peerHealth[nodeID] = true
	cm.mu.Unlock()
}

// removePeer 从哈希环、转发器及本地跟踪中移除对端。
func (cm *ClusterManager) removePeer(nodeID string) {
	cm.hr.Remove(nodeID)
	cm.forwarder.RemovePeer(nodeID)
	cm.mu.Lock()
	delete(cm.peerHealth, nodeID)
	cm.mu.Unlock()
}

// HealthyPeers 返回当前标记为健康(在环中)的对端数量。
func (cm *ClusterManager) HealthyPeers() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	count := 0
	for _, healthy := range cm.peerHealth {
		if healthy {
			count++
		}
	}
	return count
}
