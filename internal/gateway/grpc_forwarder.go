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

// Forwarder 抽象跨节点消息投递。GrpcForwarder 是 gRPC 实现;
// 测试中使用 mock。
type Forwarder interface {
	// Forward 将对端 Gateway 上的消息投递给目标 UID。
	// 如果对端将消息投递给了在线客户端则返回 true。
	Forward(ctx context.Context, targetUID string, msg *proto.Message) (bool, error)
}

// GrpcForwarder 管理到对端 Gateway 节点的 gRPC 连接,
// 并通过 Gateway.ForwardMessage RPC 转发消息。
type GrpcForwarder struct {
	hashRing   *HashRing
	thisNodeID string
	peerAddrs  map[string]string // nodeID → gRPC 地址

	mu      sync.RWMutex
	conns   map[string]*grpc.ClientConn    // nodeID → 连接
	clients map[string]proto.GatewayClient // nodeID → gRPC 客户端

	dialTimeout time.Duration
	rpcTimeout  time.Duration
}

// NewGrpcForwarder 创建一个 GrpcForwarder。哈希环决定哪个对端拥有给定的 UID;
// peerAddrs 将节点 ID 映射到 gRPC 地址。
// dialTimeout 和 rpcTimeout 为 0 时分别默认为 3s 和 2s。
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

// Forward 通过哈希环确定 targetUID 的归属节点,并通过 gRPC 转发消息。
// 如果本节点拥有该目标,则返回 (false, nil),
// 由调用方在本地处理投递。
func (f *GrpcForwarder) Forward(ctx context.Context, targetUID string, msg *proto.Message) (bool, error) {
	ownerNode := f.hashRing.Get(targetUID)
	if ownerNode == "" {
		return false, nil
	}
	if ownerNode == f.thisNodeID {
		// 本节点拥有该用户 —— 由调用方在本地处理。
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
		// 在锁内读取对端地址,避免与 AddPeer/RemovePeer 产生数据竞争。
		f.mu.RLock()
		peerAddr := f.peerAddrs[ownerNode]
		f.mu.RUnlock()
		log.Printf("[grpc-forwarder] ForwardMessage to %s (%s) failed: %v", ownerNode, peerAddr, err)

		// 逐出失效连接,使下次转发时重新拨号。
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

// getOrDial 返回指定节点的 gRPC 客户端,首次使用时惰性建立
// 连接(双重检查锁)。
func (f *GrpcForwarder) getOrDial(nodeID string) (proto.GatewayClient, error) {
	// 快路径:客户端已存在。
	f.mu.RLock()
	client, ok := f.clients[nodeID]
	f.mu.RUnlock()
	if ok {
		return client, nil
	}

	// 慢路径:建立连接。
	f.mu.Lock()
	defer f.mu.Unlock()

	// 获取写锁后再次检查。
	if client, ok = f.clients[nodeID]; ok {
		return client, nil
	}

	addr, ok := f.peerAddrs[nodeID]
	if !ok {
		return nil, grpc.ErrClientConnClosing // 如果哈希环一致,不应发生
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

// AddPeer 添加或更新对端的 gRPC 地址。线程安全。
// 如果对端已存在,只更新其地址,但不会关闭现有连接 ——
// 它会在下次转发时重新拨号。
func (f *GrpcForwarder) AddPeer(nodeID, addr string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.peerAddrs == nil {
		f.peerAddrs = make(map[string]string)
	}
	f.peerAddrs[nodeID] = addr
}

// RemovePeer 移除对端并关闭与其已有的 gRPC 连接。
// 线程安全。如果对端不存在,则为无操作。
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

// PeerAddrs 返回当前对端地址映射的副本。线程安全。
func (f *GrpcForwarder) PeerAddrs() map[string]string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[string]string, len(f.peerAddrs))
	for k, v := range f.peerAddrs {
		out[k] = v
	}
	return out
}

// Close 关闭所有到对端 Gateway 的 gRPC 连接。
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
