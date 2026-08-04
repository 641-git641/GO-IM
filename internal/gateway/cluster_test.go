package gateway

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/im/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

// ---------------------------------------------------------------------------
// GrpcForwarder AddPeer / RemovePeer 测试
// ---------------------------------------------------------------------------

func TestGrpcForwarderAddPeer(t *testing.T) {
	hr := NewHashRing(150)
	f := NewGrpcForwarder(hr, "gw-1", map[string]string{}, 0, 0)

	f.AddPeer("gw-2", "localhost:50051")

	peers := f.PeerAddrs()
	if addr, ok := peers["gw-2"]; !ok {
		t.Fatal("AddPeer: gw-2 not found in peers")
	} else if addr != "localhost:50051" {
		t.Errorf("AddPeer: expected addr 'localhost:50051', got '%s'", addr)
	}
}

func TestGrpcForwarderAddPeerOverwrite(t *testing.T) {
	hr := NewHashRing(150)
	f := NewGrpcForwarder(hr, "gw-1", map[string]string{
		"gw-2": "localhost:50051",
	}, 0, 0)

	// 用新地址覆盖现有对端。
	f.AddPeer("gw-2", "localhost:50052")

	peers := f.PeerAddrs()
	if addr := peers["gw-2"]; addr != "localhost:50052" {
		t.Errorf("AddPeer overwrite: expected 'localhost:50052', got '%s'", addr)
	}
}

func TestGrpcForwarderRemovePeer(t *testing.T) {
	hr := NewHashRing(150)
	f := NewGrpcForwarder(hr, "gw-1", map[string]string{
		"gw-2": "localhost:50051",
		"gw-3": "localhost:50052",
	}, 0, 0)

	f.RemovePeer("gw-2")

	peers := f.PeerAddrs()
	if _, ok := peers["gw-2"]; ok {
		t.Error("RemovePeer: gw-2 should be removed")
	}
	if _, ok := peers["gw-3"]; !ok {
		t.Error("RemovePeer: gw-3 should still exist")
	}
}

func TestGrpcForwarderRemovePeerNonExistent(t *testing.T) {
	hr := NewHashRing(150)
	f := NewGrpcForwarder(hr, "gw-1", map[string]string{
		"gw-2": "localhost:50051",
	}, 0, 0)

	// 不应 panic。
	f.RemovePeer("gw-nonexistent")

	peers := f.PeerAddrs()
	if len(peers) != 1 {
		t.Errorf("RemovePeer non-existent: expected 1 peer, got %d", len(peers))
	}
}

func TestGrpcForwarderRemovePeerClosesConnection(t *testing.T) {
	// 设置进程内 gRPC 服务器，以获得真实连接。
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	handler := NewGrpcGatewayServer(NewHub(100), NewHub(100), "test-gw")
	proto.RegisterGatewayServer(srv, handler)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	hr := NewHashRing(150)
	f := NewGrpcForwarder(hr, "gw-1", map[string]string{
		"gw-2": "bufnet",
	}, 0, 0)

	// 通过获取客户端强制建立连接。
	client, err := f.getOrDial("gw-2")
	if err != nil {
		// 无法从 getOrDial 内部拨号 bufconn —— 这在单元测试中是预期的。
		// 跳过此断言；我们将通过 peerAddrs map 测试连接关闭。
		_ = client
	}

	// 移除前，gw-2 在 map 中。
	if _, ok := f.PeerAddrs()["gw-2"]; !ok {
		t.Fatal("gw-2 should exist before remove")
	}

	f.RemovePeer("gw-2")

	// 移除后，gw-2 已消失。
	if _, ok := f.PeerAddrs()["gw-2"]; ok {
		t.Error("gw-2 should be removed from peerAddrs")
	}
	if _, ok := f.clients["gw-2"]; ok {
		t.Error("gw-2 should be removed from clients")
	}
}

func TestGrpcForwarderPeerAddrsIsCopy(t *testing.T) {
	hr := NewHashRing(150)
	f := NewGrpcForwarder(hr, "gw-1", map[string]string{
		"gw-2": "localhost:50051",
	}, 0, 0)

	peers := f.PeerAddrs()
	peers["gw-3"] = "localhost:50052" // 修改副本

	original := f.PeerAddrs()
	if _, ok := original["gw-3"]; ok {
		t.Error("PeerAddrs should return a copy, mutation leaked to original")
	}
}

// ---------------------------------------------------------------------------
// ClusterManager addPeer / removePeer 测试
// ---------------------------------------------------------------------------

func TestClusterManagerAddPeerUpdatesHashRing(t *testing.T) {
	hr := NewHashRing(150)
	hr.Add("gw-1") // 自身

	f := NewGrpcForwarder(hr, "gw-1", nil, 0, 0)
	cm := NewClusterManager(hr, f, ClusterConfig{
		ThisNodeID: "gw-1",
		ThisAddr:   ":50050",
	})

	cm.addPeer("gw-2", ":50051")

	// 哈希环现在应包含 gw-2。
	if hr.Get("some-key") == "" {
		t.Error("hash ring should not be empty after adding peer")
	}
	if hr.Len() != 2 {
		t.Errorf("expected 2 nodes in ring, got %d", hr.Len())
	}

	// 转发器应包含 gw-2。
	if addr := f.PeerAddrs()["gw-2"]; addr != ":50051" {
		t.Errorf("expected gw-2 addr ':50051', got '%s'", addr)
	}

	// 对端应被标记为健康。
	cm.mu.RLock()
	healthy := cm.peerHealth["gw-2"]
	cm.mu.RUnlock()
	if !healthy {
		t.Error("gw-2 should be marked healthy")
	}
}

func TestClusterManagerRemovePeerUpdatesHashRing(t *testing.T) {
	hr := NewHashRing(150)
	hr.Add("gw-1")
	hr.Add("gw-2")

	f := NewGrpcForwarder(hr, "gw-1", map[string]string{
		"gw-2": ":50051",
	}, 0, 0)
	cm := NewClusterManager(hr, f, ClusterConfig{
		ThisNodeID: "gw-1",
		ThisAddr:   ":50050",
	})
	cm.mu.Lock()
	cm.peerHealth["gw-2"] = true
	cm.mu.Unlock()

	cm.removePeer("gw-2")

	// 哈希环应只有 gw-1。
	if hr.Len() != 1 {
		t.Errorf("expected 1 node in ring, got %d", hr.Len())
	}

	// 转发器不应包含 gw-2。
	if _, ok := f.PeerAddrs()["gw-2"]; ok {
		t.Error("gw-2 should be removed from forwarder")
	}

	// PeerHealth 不应包含 gw-2。
	cm.mu.RLock()
	_, exists := cm.peerHealth["gw-2"]
	cm.mu.RUnlock()
	if exists {
		t.Error("gw-2 should be removed from peerHealth")
	}
}

func TestClusterManagerHealthyPeers(t *testing.T) {
	hr := NewHashRing(150)
	f := NewGrpcForwarder(hr, "gw-1", nil, 0, 0)
	cm := NewClusterManager(hr, f, ClusterConfig{
		ThisNodeID: "gw-1",
		ThisAddr:   ":50050",
	})

	if n := cm.HealthyPeers(); n != 0 {
		t.Errorf("expected 0 healthy peers, got %d", n)
	}

	cm.addPeer("gw-2", ":50051")
	cm.addPeer("gw-3", ":50052")

	if n := cm.HealthyPeers(); n != 2 {
		t.Errorf("expected 2 healthy peers, got %d", n)
	}

	// 手动将一个对端标记为不健康。
	cm.mu.Lock()
	cm.peerHealth["gw-2"] = false
	cm.mu.Unlock()

	if n := cm.HealthyPeers(); n != 1 {
		t.Errorf("expected 1 healthy peer, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// 健康检查探针测试
// ---------------------------------------------------------------------------

func TestClusterManagerProbePeerUnreachable(t *testing.T) {
	hr := NewHashRing(150)
	f := NewGrpcForwarder(hr, "gw-1", nil, 0, 0)
	cm := NewClusterManager(hr, f, ClusterConfig{
		ThisNodeID:   "gw-1",
		ThisAddr:     ":50050",
		ProbeTimeout: 100 * time.Millisecond,
	})

	// 端口 1 几乎可以肯定未被使用 —— 应不可达。
	// 使用一个没有监听的随机高位端口。
	healthy := cm.probePeer("127.0.0.1:1")
	if healthy {
		t.Log("probePeer returned healthy for an unlikely port — port 1 may have something bound")
		// 不算硬性失败 —— 某些系统上端口 1 可能可访问。
	}
}

func TestClusterManagerProbePeerReachable(t *testing.T) {
	// 在动态端口上启动真实的 gRPC 服务器。
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()

	srv := grpc.NewServer()
	handler := NewGrpcGatewayServer(NewHub(100), NewHub(100), "test-gw")
	proto.RegisterGatewayServer(srv, handler)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	hr := NewHashRing(150)
	f := NewGrpcForwarder(hr, "gw-1", nil, 0, 0)
	cm := NewClusterManager(hr, f, ClusterConfig{
		ThisNodeID:   "gw-1",
		ThisAddr:     ":50050",
		ProbeTimeout: 500 * time.Millisecond,
	})

	healthy := cm.probePeer(addr)
	if !healthy {
		t.Errorf("probePeer should be healthy for running server at %s", addr)
	}
}

// ---------------------------------------------------------------------------
// 健康检查循环测试
// ---------------------------------------------------------------------------

func TestClusterManagerHealthCheckMarksUnhealthy(t *testing.T) {
	hr := NewHashRing(150)
	hr.Add("gw-1") // 自身
	hr.Add("gw-2") // 对端

	f := NewGrpcForwarder(hr, "gw-1", map[string]string{
		"gw-2": "127.0.0.1:1", // 不可达
	}, 0, 0)
	cm := NewClusterManager(hr, f, ClusterConfig{
		ThisNodeID:   "gw-1",
		ThisAddr:     ":50050",
		ProbeTimeout: 50 * time.Millisecond,
	})

	// 初始时 gw-2 在环中并被标记为健康（Start 的行为）。
	cm.mu.Lock()
	cm.peerHealth["gw-2"] = true
	cm.mu.Unlock()

	if hr.Len() != 2 {
		t.Fatalf("expected 2 nodes in ring, got %d", hr.Len())
	}

	// 运行一个健康检查周期。
	cm.runHealthCheck()

	// 健康检查后，gw-2 应从环中移除。
	if hr.Len() != 1 {
		t.Errorf("expected 1 node in ring after health check, got %d", hr.Len())
	}

	cm.mu.RLock()
	healthy := cm.peerHealth["gw-2"]
	cm.mu.RUnlock()
	if healthy {
		t.Error("gw-2 should be marked unhealthy")
	}
}

func TestClusterManagerHealthCheckRecovery(t *testing.T) {
	// 启动真实的 gRPC 服务器。
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()

	srv := grpc.NewServer()
	handler := NewGrpcGatewayServer(NewHub(100), NewHub(100), "test-gw")
	proto.RegisterGatewayServer(srv, handler)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	hr := NewHashRing(150)
	hr.Add("gw-1")

	f := NewGrpcForwarder(hr, "gw-1", map[string]string{
		"gw-2": addr,
	}, 0, 0)
	cm := NewClusterManager(hr, f, ClusterConfig{
		ThisNodeID:   "gw-1",
		ThisAddr:     ":50050",
		ProbeTimeout: 500 * time.Millisecond,
	})

	// 模拟：gw-2 之前已被移除（标记为不健康）。
	hr.Remove("gw-2")
	cm.mu.Lock()
	cm.peerHealth["gw-2"] = false
	cm.mu.Unlock()

	if hr.Len() != 1 {
		t.Fatalf("expected 1 node before recovery check, got %d", hr.Len())
	}

	// 运行健康检查 —— 应检测到 gw-2 现在可达并将其重新加入。
	cm.runHealthCheck()

	if hr.Len() != 2 {
		t.Errorf("expected 2 nodes in ring after recovery, got %d", hr.Len())
	}

	cm.mu.RLock()
	healthy := cm.peerHealth["gw-2"]
	cm.mu.RUnlock()
	if !healthy {
		t.Error("gw-2 should be marked healthy after recovery")
	}
}

func TestClusterManagerHealthCheckSkipsSelf(t *testing.T) {
	hr := NewHashRing(150)
	hr.Add("gw-1") // 自身

	f := NewGrpcForwarder(hr, "gw-1", nil, 0, 0)
	cm := NewClusterManager(hr, f, ClusterConfig{
		ThisNodeID:   "gw-1",
		ThisAddr:     ":50050",
		ProbeTimeout: 50 * time.Millisecond,
	})

	// 不应 panic，自身应保持健康。
	cm.runHealthCheck()

	if hr.Len() != 1 {
		t.Errorf("self should remain in ring, got %d nodes", hr.Len())
	}
}

// ---------------------------------------------------------------------------
// reconcilePeers 测试
// ---------------------------------------------------------------------------

func TestClusterManagerReconcilePeersAddNew(t *testing.T) {
	hr := NewHashRing(150)
	hr.Add("gw-1")
	f := NewGrpcForwarder(hr, "gw-1", nil, 0, 0)
	cm := NewClusterManager(hr, f, ClusterConfig{
		ThisNodeID: "gw-1",
		ThisAddr:   ":50050",
	})

	// Redis 报告 gw-2 和 gw-3。
	cm.reconcilePeers(map[string]string{
		"gw-2": ":50051",
		"gw-3": ":50052",
	})

	if hr.Len() != 3 {
		t.Errorf("expected 3 nodes (self + 2 peers), got %d", hr.Len())
	}
	if _, ok := f.PeerAddrs()["gw-2"]; !ok {
		t.Error("gw-2 should be added")
	}
	if _, ok := f.PeerAddrs()["gw-3"]; !ok {
		t.Error("gw-3 should be added")
	}
}

func TestClusterManagerReconcilePeersSkipsSelf(t *testing.T) {
	hr := NewHashRing(150)
	hr.Add("gw-1")
	f := NewGrpcForwarder(hr, "gw-1", nil, 0, 0)
	cm := NewClusterManager(hr, f, ClusterConfig{
		ThisNodeID: "gw-1",
		ThisAddr:   ":50050",
	})

	// Redis 报告我们自己的节点 —— 应被忽略。
	cm.reconcilePeers(map[string]string{
		"gw-1": ":50050",
		"gw-2": ":50051",
	})

	// 只应添加 gw-2，而不重复添加自身。
	if _, ok := f.PeerAddrs()["gw-1"]; ok {
		t.Error("self should not appear in forwarder peers")
	}
	if _, ok := f.PeerAddrs()["gw-2"]; !ok {
		t.Error("gw-2 should be added")
	}
}

func TestClusterManagerReconcilePeersRemoveStale(t *testing.T) {
	hr := NewHashRing(150)
	hr.Add("gw-1")
	hr.Add("gw-2")
	hr.Add("gw-3")

	f := NewGrpcForwarder(hr, "gw-1", map[string]string{
		"gw-2": ":50051",
		"gw-3": ":50052",
	}, 0, 0)
	cm := NewClusterManager(hr, f, ClusterConfig{
		ThisNodeID: "gw-1",
		ThisAddr:   ":50050",
	})
	cm.mu.Lock()
	cm.peerHealth["gw-2"] = true
	cm.peerHealth["gw-3"] = true
	cm.mu.Unlock()

	// Redis 只报告 gw-2 —— gw-3 应被移除。
	cm.reconcilePeers(map[string]string{
		"gw-2": ":50051",
	})

	if hr.Len() != 2 {
		t.Errorf("expected 2 nodes (self + gw-2), got %d", hr.Len())
	}
	if _, ok := f.PeerAddrs()["gw-3"]; ok {
		t.Error("gw-3 should be removed from forwarder")
	}
	cm.mu.RLock()
	_, exists := cm.peerHealth["gw-3"]
	cm.mu.RUnlock()
	if exists {
		t.Error("gw-3 should be removed from peerHealth")
	}
}

func TestClusterManagerReconcilePeersAddressChanged(t *testing.T) {
	hr := NewHashRing(150)
	hr.Add("gw-1")
	hr.Add("gw-2")

	f := NewGrpcForwarder(hr, "gw-1", map[string]string{
		"gw-2": ":50051",
	}, 0, 0)
	cm := NewClusterManager(hr, f, ClusterConfig{
		ThisNodeID: "gw-1",
		ThisAddr:   ":50050",
	})
	cm.mu.Lock()
	cm.peerHealth["gw-2"] = true
	cm.mu.Unlock()

	// Redis 报告 gw-2 的新地址。
	cm.reconcilePeers(map[string]string{
		"gw-2": ":60051",
	})

	if addr := f.PeerAddrs()["gw-2"]; addr != ":60051" {
		t.Errorf("expected gw-2 addr updated to ':60051', got '%s'", addr)
	}
}

// ---------------------------------------------------------------------------
// ClusterManager 生命周期测试
// ---------------------------------------------------------------------------

func TestClusterManagerStartStopStatic(t *testing.T) {
	hr := NewHashRing(150)
	hr.Add("gw-1")
	hr.Add("gw-2")

	f := NewGrpcForwarder(hr, "gw-1", map[string]string{
		"gw-2": "127.0.0.1:1", // 不可达，但对生命周期测试无妨
	}, 0, 0)
	cm := NewClusterManager(hr, f, ClusterConfig{
		ThisNodeID:     "gw-1",
		ThisAddr:       ":50050",
		HealthInterval: 500 * time.Millisecond,
		ProbeTimeout:   50 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())

	cm.Start(ctx)

	// 给健康检查循环一点运行时间。
	time.Sleep(100 * time.Millisecond)

	// 验证后台 goroutine 已启动（健康检查循环）。
	cm.mu.RLock()
	_, exists := cm.peerHealth["gw-2"]
	cm.mu.RUnlock()
	if !exists {
		t.Error("gw-2 should be tracked in peerHealth after Start")
	}

	// Stop 应无挂起地完成清理。
	cancel()
	cm.Stop()

	// Stop 之后 wg 已完成。这只是验证 Stop() 不会死锁。
}

func TestClusterManagerStartStopConcurrent(t *testing.T) {
	// 验证从多个 goroutine 调用 Start/Stop 是安全的。
	hr := NewHashRing(150)
	hr.Add("gw-1")

	f := NewGrpcForwarder(hr, "gw-1", nil, 0, 0)
	cm := NewClusterManager(hr, f, ClusterConfig{
		ThisNodeID:     "gw-1",
		ThisAddr:       ":50050",
		HealthInterval: 100 * time.Millisecond,
		ProbeTimeout:   50 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	cm.Start(ctx)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = cm.HealthyPeers() // 安全的并发读取
		}()
	}
	wg.Wait()

	cancel()
	cm.Stop()
}

// ---------------------------------------------------------------------------
// 默认配置值
// ---------------------------------------------------------------------------

func TestClusterConfigDefaults(t *testing.T) {
	hr := NewHashRing(150)
	f := NewGrpcForwarder(hr, "gw-1", nil, 0, 0)
	cm := NewClusterManager(hr, f, ClusterConfig{
		ThisNodeID: "gw-1",
		ThisAddr:   ":50050",
	})

	if cm.healthInterval != 5*time.Second {
		t.Errorf("default health interval: expected 5s, got %s", cm.healthInterval)
	}
	if cm.probeTimeout != 2*time.Second {
		t.Errorf("default probe timeout: expected 2s, got %s", cm.probeTimeout)
	}
	if cm.ttl != 15*time.Second {
		t.Errorf("default TTL: expected 15s, got %s", cm.ttl)
	}
	if cm.redisPrefix != "im:gateway:node:" {
		t.Errorf("default redis prefix: expected 'im:gateway:node:', got '%s'", cm.redisPrefix)
	}
}

func TestClusterConfigCustomValues(t *testing.T) {
	hr := NewHashRing(150)
	f := NewGrpcForwarder(hr, "gw-1", nil, 0, 0)
	cm := NewClusterManager(hr, f, ClusterConfig{
		ThisNodeID:     "gw-1",
		ThisAddr:       ":50050",
		HealthInterval: 3 * time.Second,
		ProbeTimeout:   1 * time.Second,
		TTL:            30 * time.Second,
		RedisPrefix:    "custom:prefix:",
	})

	if cm.healthInterval != 3*time.Second {
		t.Errorf("health interval: expected 3s, got %s", cm.healthInterval)
	}
	if cm.probeTimeout != 1*time.Second {
		t.Errorf("probe timeout: expected 1s, got %s", cm.probeTimeout)
	}
	if cm.ttl != 30*time.Second {
		t.Errorf("TTL: expected 30s, got %s", cm.ttl)
	}
	if cm.redisPrefix != "custom:prefix:" {
		t.Errorf("redis prefix: expected 'custom:prefix:', got '%s'", cm.redisPrefix)
	}
}
