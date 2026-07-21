package gateway

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// ClusterManager handles dynamic multi-gateway clustering: health checking peers
// and (optionally) discovering them via Redis. It keeps the hash ring and
// forwarder in sync with the current set of healthy, reachable peers.
//
// Two modes are supported:
//   - Static: peers come from config (peer_addrs). Health checks still run and
//     unhealthy peers are temporarily removed from the hash ring.
//   - Redis discovery: peers are discovered via Redis keys with TTL-based
//     heartbeat. New peers are added automatically; expired peers are removed.
type ClusterManager struct {
	hr        *HashRing
	forwarder *GrpcForwarder
	thisNode  string
	thisAddr  string

	// Redis discovery (nil when disabled).
	redis       *redis.Client
	redisPrefix string // default "im:gateway:node:"
	ttl         time.Duration

	// Health checking.
	healthInterval time.Duration
	probeTimeout   time.Duration

	// Lifecycle.
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Track which peers are currently in the hash ring (healthy).
	mu          sync.RWMutex
	peerHealth  map[string]bool // nodeID → currently healthy (in hash ring)
}

// ClusterConfig holds settings for the ClusterManager.
type ClusterConfig struct {
	ThisNodeID     string
	ThisAddr       string
	HealthInterval time.Duration // 0 defaults to 5s
	ProbeTimeout   time.Duration // 0 defaults to 2s

	// Redis discovery. Leave Redis nil to use static peer_addrs only.
	Redis       *redis.Client
	RedisPrefix string        // 0 defaults to "im:gateway:node:"
	TTL         time.Duration // 0 defaults to 15s
}

// NewClusterManager creates a ClusterManager.
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

// Start begins the background loops for health checking and (when Redis is
// configured) service discovery. It blocks only long enough to register in
// Redis and perform the initial peer discovery.
// Safe to call only once; subsequent calls are no-ops to prevent context leaks.
func (cm *ClusterManager) Start(ctx context.Context) {
	if cm.cancel != nil {
		log.Printf("[cluster] Start called but manager is already running — ignored")
		return
	}
	cm.ctx, cm.cancel = context.WithCancel(ctx)

	// Mark all initial peers as healthy (they were added to the ring at startup).
	for nodeID := range cm.forwarder.PeerAddrs() {
		if nodeID != cm.thisNode {
			cm.mu.Lock()
			cm.peerHealth[nodeID] = true
			cm.mu.Unlock()
		}
	}

	if cm.redis != nil {
		// Register this node and discover initial peers.
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

// Stop gracefully shuts down background loops and deregisters from Redis.
func (cm *ClusterManager) Stop() {
	if cm.cancel != nil {
		cm.cancel()
	}

	// Deregister from Redis so peers stop routing to us.
	// Use a short timeout to avoid blocking graceful shutdown.
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
// Redis service discovery
// ---------------------------------------------------------------------------

// nodeKey returns the Redis key for a given node ID.
func (cm *ClusterManager) nodeKey(nodeID string) string {
	return cm.redisPrefix + nodeID
}

// registerSelf registers this Gateway node in Redis with a TTL.
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

// refreshLoop periodically refreshes this node's TTL in Redis.
func (cm *ClusterManager) refreshLoop() {
	defer cm.wg.Done()

	// Refresh at TTL/3 to have a healthy margin before expiry.
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

// discoverPeers scans Redis for all registered Gateway nodes and reconciles
// the set of local peers: new nodes are added, missing nodes are removed.
func (cm *ClusterManager) discoverPeers() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pattern := cm.redisPrefix + "*"
	keys, err := cm.redis.Keys(ctx, pattern).Result()
	if err != nil {
		log.Printf("[cluster] redis scan error: %v", err)
		return
	}

	found := make(map[string]string, len(keys)) // nodeID → addr
	for _, key := range keys {
		nodeID := key[len(cm.redisPrefix):]
		if nodeID == "" {
			continue
		}
		addr, err := cm.redis.Get(ctx, key).Result()
		if err != nil {
			continue // key expired between SCAN and GET
		}
		found[nodeID] = addr
	}

	cm.reconcilePeers(found)
}

// reconcilePeers adds new peers and removes stale ones.
func (cm *ClusterManager) reconcilePeers(found map[string]string) {
	currentPeers := cm.forwarder.PeerAddrs()

	// Add new peers (and re-add peers that were removed but are back).
	for nodeID, addr := range found {
		if nodeID == cm.thisNode {
			continue
		}
		if _, exists := currentPeers[nodeID]; !exists {
			log.Printf("[cluster] discovered new peer: %s at %s", nodeID, addr)
			cm.addPeer(nodeID, addr)
		} else if currentPeers[nodeID] != addr {
			// Address changed — update.
			log.Printf("[cluster] peer %s address changed: %s → %s", nodeID, currentPeers[nodeID], addr)
			cm.addPeer(nodeID, addr)
		}
	}

	// Remove peers that disappeared from Redis.
	for nodeID := range currentPeers {
		if _, exists := found[nodeID]; !exists {
			log.Printf("[cluster] peer %s removed from Redis, removing locally", nodeID)
			cm.removePeer(nodeID)
		}
	}
}

// discoveryLoop periodically scans Redis for peer changes.
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
// Health checking
// ---------------------------------------------------------------------------

// healthCheckLoop periodically probes every known peer and updates the hash
// ring accordingly: unhealthy peers are temporarily removed, healthy peers are
// added back.
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

// runHealthCheck probes all peers (except self) and reconciles the hash ring.
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
			log.Printf("[cluster] peer %s recovered — adding back to hash ring", nodeID)
			cm.hr.Add(nodeID)
			cm.mu.Lock()
			cm.peerHealth[nodeID] = true
			cm.mu.Unlock()
		} else if !healthy && wasHealthy {
			log.Printf("[cluster] peer %s is unhealthy — removing from hash ring", nodeID)
			cm.hr.Remove(nodeID)
			cm.mu.Lock()
			cm.peerHealth[nodeID] = false
			cm.mu.Unlock()
		}
	}
}

// probePeer attempts a gRPC dial to the peer with a short timeout.
// Returns true if the peer is reachable.
func (cm *ClusterManager) probePeer(addr string) bool {
	// Create a sub-context so we can wait for the connection after dialing.
	ctx, cancel := context.WithTimeout(context.Background(), cm.probeTimeout)
	defer cancel()

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return false
	}
	defer conn.Close()

	// Kick off the connection and wait for it to become ready.
	conn.Connect()
	state := conn.GetState()
	for state != connectivity.Ready {
		if !conn.WaitForStateChange(ctx, state) {
			return false // context expired or connection shut down
		}
		state = conn.GetState()
	}
	return true
}

// ---------------------------------------------------------------------------
// Peer management
// ---------------------------------------------------------------------------

// addPeer adds a peer to both the forwarder's address map and the hash ring.
func (cm *ClusterManager) addPeer(nodeID, addr string) {
	cm.forwarder.AddPeer(nodeID, addr)
	cm.hr.Add(nodeID)
	cm.mu.Lock()
	cm.peerHealth[nodeID] = true
	cm.mu.Unlock()
}

// removePeer removes a peer from the hash ring, the forwarder, and local tracking.
func (cm *ClusterManager) removePeer(nodeID string) {
	cm.hr.Remove(nodeID)
	cm.forwarder.RemovePeer(nodeID)
	cm.mu.Lock()
	delete(cm.peerHealth, nodeID)
	cm.mu.Unlock()
}

// HealthyPeers returns the number of peers currently marked healthy (in the ring).
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
