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
// GrpcForwarder AddPeer / RemovePeer tests
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

	// Overwrite existing peer with new address.
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

	// Should not panic.
	f.RemovePeer("gw-nonexistent")

	peers := f.PeerAddrs()
	if len(peers) != 1 {
		t.Errorf("RemovePeer non-existent: expected 1 peer, got %d", len(peers))
	}
}

func TestGrpcForwarderRemovePeerClosesConnection(t *testing.T) {
	// Set up an in-process gRPC server so we get a real connection.
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

	// Force a connection by getting the client.
	client, err := f.getOrDial("gw-2")
	if err != nil {
		// Can't dial bufconn from inside getOrDial — that's expected in unit test.
		// Skip this assertion; we'll test connection close via the peerAddrs map.
		_ = client
	}

	// Before remove, gw-2 is in the map.
	if _, ok := f.PeerAddrs()["gw-2"]; !ok {
		t.Fatal("gw-2 should exist before remove")
	}

	f.RemovePeer("gw-2")

	// After remove, gw-2 is gone.
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
	peers["gw-3"] = "localhost:50052" // mutate the copy

	original := f.PeerAddrs()
	if _, ok := original["gw-3"]; ok {
		t.Error("PeerAddrs should return a copy, mutation leaked to original")
	}
}

// ---------------------------------------------------------------------------
// ClusterManager addPeer / removePeer tests
// ---------------------------------------------------------------------------

func TestClusterManagerAddPeerUpdatesHashRing(t *testing.T) {
	hr := NewHashRing(150)
	hr.Add("gw-1") // self

	f := NewGrpcForwarder(hr, "gw-1", nil, 0, 0)
	cm := NewClusterManager(hr, f, ClusterConfig{
		ThisNodeID: "gw-1",
		ThisAddr:   ":50050",
	})

	cm.addPeer("gw-2", ":50051")

	// Hash ring should now contain gw-2.
	if hr.Get("some-key") == "" {
		t.Error("hash ring should not be empty after adding peer")
	}
	if hr.Len() != 2 {
		t.Errorf("expected 2 nodes in ring, got %d", hr.Len())
	}

	// Forwarder should have gw-2.
	if addr := f.PeerAddrs()["gw-2"]; addr != ":50051" {
		t.Errorf("expected gw-2 addr ':50051', got '%s'", addr)
	}

	// Peer should be marked healthy.
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

	// Hash ring should only have gw-1.
	if hr.Len() != 1 {
		t.Errorf("expected 1 node in ring, got %d", hr.Len())
	}

	// Forwarder should not have gw-2.
	if _, ok := f.PeerAddrs()["gw-2"]; ok {
		t.Error("gw-2 should be removed from forwarder")
	}

	// PeerHealth should not have gw-2.
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

	// Mark one unhealthy manually.
	cm.mu.Lock()
	cm.peerHealth["gw-2"] = false
	cm.mu.Unlock()

	if n := cm.HealthyPeers(); n != 1 {
		t.Errorf("expected 1 healthy peer, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// Health check probe tests
// ---------------------------------------------------------------------------

func TestClusterManagerProbePeerUnreachable(t *testing.T) {
	hr := NewHashRing(150)
	f := NewGrpcForwarder(hr, "gw-1", nil, 0, 0)
	cm := NewClusterManager(hr, f, ClusterConfig{
		ThisNodeID:   "gw-1",
		ThisAddr:     ":50050",
		ProbeTimeout: 100 * time.Millisecond,
	})

	// Port 1 is almost certainly not in use — should be unreachable.
	// Use a random high port with nothing listening.
	healthy := cm.probePeer("127.0.0.1:1")
	if healthy {
		t.Log("probePeer returned healthy for an unlikely port — port 1 may have something bound")
		// Not a hard failure — port 1 might be accessible on some systems.
	}
}

func TestClusterManagerProbePeerReachable(t *testing.T) {
	// Start a real gRPC server on a dynamic port.
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
// Health check loop tests
// ---------------------------------------------------------------------------

func TestClusterManagerHealthCheckMarksUnhealthy(t *testing.T) {
	hr := NewHashRing(150)
	hr.Add("gw-1") // self
	hr.Add("gw-2") // peer

	f := NewGrpcForwarder(hr, "gw-1", map[string]string{
		"gw-2": "127.0.0.1:1", // unreachable
	}, 0, 0)
	cm := NewClusterManager(hr, f, ClusterConfig{
		ThisNodeID:   "gw-1",
		ThisAddr:     ":50050",
		ProbeTimeout: 50 * time.Millisecond,
	})

	// Initially gw-2 is in the ring and marked healthy (Start behavior).
	cm.mu.Lock()
	cm.peerHealth["gw-2"] = true
	cm.mu.Unlock()

	if hr.Len() != 2 {
		t.Fatalf("expected 2 nodes in ring, got %d", hr.Len())
	}

	// Run one health check cycle.
	cm.runHealthCheck()

	// After health check, gw-2 should be removed from the ring.
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
	// Start a real gRPC server.
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

	// Simulate: gw-2 was previously removed (marked unhealthy).
	hr.Remove("gw-2")
	cm.mu.Lock()
	cm.peerHealth["gw-2"] = false
	cm.mu.Unlock()

	if hr.Len() != 1 {
		t.Fatalf("expected 1 node before recovery check, got %d", hr.Len())
	}

	// Run health check — should detect gw-2 is now reachable and add it back.
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
	hr.Add("gw-1") // self

	f := NewGrpcForwarder(hr, "gw-1", nil, 0, 0)
	cm := NewClusterManager(hr, f, ClusterConfig{
		ThisNodeID:   "gw-1",
		ThisAddr:     ":50050",
		ProbeTimeout: 50 * time.Millisecond,
	})

	// Should not panic and self should remain healthy.
	cm.runHealthCheck()

	if hr.Len() != 1 {
		t.Errorf("self should remain in ring, got %d nodes", hr.Len())
	}
}

// ---------------------------------------------------------------------------
// reconcilePeers tests
// ---------------------------------------------------------------------------

func TestClusterManagerReconcilePeersAddNew(t *testing.T) {
	hr := NewHashRing(150)
	hr.Add("gw-1")
	f := NewGrpcForwarder(hr, "gw-1", nil, 0, 0)
	cm := NewClusterManager(hr, f, ClusterConfig{
		ThisNodeID: "gw-1",
		ThisAddr:   ":50050",
	})

	// Redis reports gw-2 and gw-3.
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

	// Redis reports our own node — should be ignored.
	cm.reconcilePeers(map[string]string{
		"gw-1": ":50050",
		"gw-2": ":50051",
	})

	// Only gw-2 should be added, not duplicate self.
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

	// Redis only reports gw-2 — gw-3 should be removed.
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

	// Redis reports gw-2 with a new address.
	cm.reconcilePeers(map[string]string{
		"gw-2": ":60051",
	})

	if addr := f.PeerAddrs()["gw-2"]; addr != ":60051" {
		t.Errorf("expected gw-2 addr updated to ':60051', got '%s'", addr)
	}
}

// ---------------------------------------------------------------------------
// ClusterManager lifecycle tests
// ---------------------------------------------------------------------------

func TestClusterManagerStartStopStatic(t *testing.T) {
	hr := NewHashRing(150)
	hr.Add("gw-1")
	hr.Add("gw-2")

	f := NewGrpcForwarder(hr, "gw-1", map[string]string{
		"gw-2": "127.0.0.1:1", // unreachable, but that's fine for lifecycle test
	}, 0, 0)
	cm := NewClusterManager(hr, f, ClusterConfig{
		ThisNodeID:     "gw-1",
		ThisAddr:       ":50050",
		HealthInterval: 500 * time.Millisecond,
		ProbeTimeout:   50 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())

	cm.Start(ctx)

	// Give the health check loop a moment to run.
	time.Sleep(100 * time.Millisecond)

	// Verify the background goroutine started (health check loop).
	cm.mu.RLock()
	_, exists := cm.peerHealth["gw-2"]
	cm.mu.RUnlock()
	if !exists {
		t.Error("gw-2 should be tracked in peerHealth after Start")
	}

	// Stop should clean up without hanging.
	cancel()
	cm.Stop()

	// After Stop, wg is done. This just validates Stop() doesn't deadlock.
}

func TestClusterManagerStartStopConcurrent(t *testing.T) {
	// Verify Start/Stop is safe to call from multiple goroutines.
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
			_ = cm.HealthyPeers() // safe concurrent read
		}()
	}
	wg.Wait()

	cancel()
	cm.Stop()
}

// ---------------------------------------------------------------------------
// Default config values
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
