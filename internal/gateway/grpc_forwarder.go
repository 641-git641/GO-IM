package gateway

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/im/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Forwarder abstracts cross-node message delivery. GrpcForwarder is the gRPC
// implementation; tests use a mock.
type Forwarder interface {
	// Forward delivers a message to the target UID on a peer Gateway.
	// Returns true if the peer delivered the message to an online client.
	Forward(ctx context.Context, targetUID string, msg *proto.Message) (bool, error)
}

// GrpcForwarder manages gRPC connections to peer Gateway nodes and forwards
// messages via the Gateway.ForwardMessage RPC.
type GrpcForwarder struct {
	hashRing   *HashRing
	thisNodeID string
	peerAddrs  map[string]string // nodeID → gRPC address

	mu      sync.RWMutex
	conns   map[string]*grpc.ClientConn    // nodeID → connection
	clients map[string]proto.GatewayClient // nodeID → gRPC client

	dialTimeout time.Duration
	rpcTimeout  time.Duration
}

// NewGrpcForwarder creates a GrpcForwarder. The hash ring determines which
// peer owns a given UID; peerAddrs maps node IDs to gRPC addresses.
// dialTimeout and rpcTimeout default to 3s and 2s respectively if zero.
func NewGrpcForwarder(hr *HashRing, thisNodeID string, peerAddrs map[string]string, dialTimeout, rpcTimeout time.Duration) *GrpcForwarder {
	if dialTimeout <= 0 {
		dialTimeout = 3 * time.Second
	}
	if rpcTimeout <= 0 {
		rpcTimeout = 2 * time.Second
	}
	return &GrpcForwarder{
		hashRing:    hr,
		thisNodeID:  thisNodeID,
		peerAddrs:   peerAddrs,
		conns:       make(map[string]*grpc.ClientConn),
		clients:     make(map[string]proto.GatewayClient),
		dialTimeout: dialTimeout,
		rpcTimeout:  rpcTimeout,
	}
}

// Forward determines the owner node for targetUID via the hash ring and
// forwards the message via gRPC. If this node owns the target, it returns
// (false, nil) so the caller handles delivery locally.
func (f *GrpcForwarder) Forward(ctx context.Context, targetUID string, msg *proto.Message) (bool, error) {
	ownerNode := f.hashRing.Get(targetUID)
	if ownerNode == "" {
		return false, nil
	}
	if ownerNode == f.thisNodeID {
		// This node owns the user — caller handles locally.
		return false, nil
	}

	client, err := f.getOrDial(ownerNode)
	if err != nil {
		return false, err
	}

	rpcCtx, cancel := context.WithTimeout(ctx, f.rpcTimeout)
	defer cancel()

	req := &proto.ForwardRequest{
		Message: msg,
		Uid:     targetUID,
	}

	resp, err := client.ForwardMessage(rpcCtx, req)
	if err != nil {
		// Read peer address under lock to avoid data race with AddPeer/RemovePeer.
		f.mu.RLock()
		peerAddr := f.peerAddrs[ownerNode]
		f.mu.RUnlock()
		log.Printf("[grpc-forwarder] ForwardMessage to %s (%s) failed: %v", ownerNode, peerAddr, err)

		// Evict the broken connection so the next forward re-dials.
		f.mu.Lock()
		if conn, ok := f.conns[ownerNode]; ok {
			conn.Close()
			delete(f.conns, ownerNode)
		}
		delete(f.clients, ownerNode)
		f.mu.Unlock()

		return false, err
	}

	if resp.Delivered {
		log.Printf("[grpc-forwarder] forwarded message for %s to %s — delivered online", targetUID, ownerNode)
	} else {
		log.Printf("[grpc-forwarder] forwarded message for %s to %s — peer stored offline: %s", targetUID, ownerNode, resp.Error)
	}

	return resp.Delivered, nil
}

// getOrDial returns a gRPC client for the given node, lazily establishing the
// connection on first use (double-checked locking).
func (f *GrpcForwarder) getOrDial(nodeID string) (proto.GatewayClient, error) {
	// Fast path: client already exists.
	f.mu.RLock()
	client, ok := f.clients[nodeID]
	f.mu.RUnlock()
	if ok {
		return client, nil
	}

	// Slow path: establish connection.
	f.mu.Lock()
	defer f.mu.Unlock()

	// Double-check after acquiring write lock.
	if client, ok = f.clients[nodeID]; ok {
		return client, nil
	}

	addr, ok := f.peerAddrs[nodeID]
	if !ok {
		return nil, grpc.ErrClientConnClosing // shouldn't happen if hash ring is consistent
	}

	dialCtx, cancel := context.WithTimeout(context.Background(), f.dialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(dialCtx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		log.Printf("[grpc-forwarder] failed to dial peer %s at %s: %v", nodeID, addr, err)
		return nil, err
	}

	f.conns[nodeID] = conn
	f.clients[nodeID] = proto.NewGatewayClient(conn)
	log.Printf("[grpc-forwarder] connected to peer %s at %s", nodeID, addr)

	return f.clients[nodeID], nil
}

// AddPeer adds or updates a peer's gRPC address. Thread-safe.
// If the peer already exists, its address is updated but the existing
// connection is NOT closed — it will be re-dialed on the next forward.
func (f *GrpcForwarder) AddPeer(nodeID, addr string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.peerAddrs == nil {
		f.peerAddrs = make(map[string]string)
	}
	f.peerAddrs[nodeID] = addr
}

// RemovePeer removes a peer and closes any existing gRPC connection to it.
// Thread-safe. If the peer does not exist, this is a no-op.
func (f *GrpcForwarder) RemovePeer(nodeID string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.peerAddrs, nodeID)

	if conn, ok := f.conns[nodeID]; ok {
		conn.Close()
		delete(f.conns, nodeID)
	}
	delete(f.clients, nodeID)
}

// PeerAddrs returns a copy of the current peer address map. Thread-safe.
func (f *GrpcForwarder) PeerAddrs() map[string]string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[string]string, len(f.peerAddrs))
	for k, v := range f.peerAddrs {
		out[k] = v
	}
	return out
}

// Close shuts down all gRPC connections to peer Gateways.
func (f *GrpcForwarder) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	var lastErr error
	for nodeID, conn := range f.conns {
		if err := conn.Close(); err != nil {
			log.Printf("[grpc-forwarder] close connection to %s: %v", nodeID, err)
			lastErr = err
		}
	}
	f.conns = make(map[string]*grpc.ClientConn)
	f.clients = make(map[string]proto.GatewayClient)
	return lastErr
}
