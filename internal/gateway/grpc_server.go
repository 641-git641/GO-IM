package gateway

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/im/api/proto"
	"google.golang.org/grpc"
)

// GrpcGatewayServer 实现 proto.GatewayServer,用于网关间的消息转发。
// 当对端 Gateway 向本节点转发消息时,该处理器尝试本地投递;
// 如果目标未在本节点连接,则消息存入离线存储。
type GrpcGatewayServer struct {
	proto.UnimplementedGatewayServer
	clients ClientRegistry
	offline OfflineStore
	nodeID  string
}

// NewGrpcGatewayServer 创建一个 GrpcGatewayServer,
// 由给定的连接注册表和离线存储提供支持。
func NewGrpcGatewayServer(clients ClientRegistry, offline OfflineStore, nodeID string) *GrpcGatewayServer {
	return &GrpcGatewayServer{
		clients: clients,
		offline: offline,
		nodeID:  nodeID,
	}
}

// ForwardMessage 处理来自对端的网关间消息转发请求。
// 它尝试将消息投递给本地连接的客户端;如果目标离线,
// 消息将被存储以便稍后投递。
func (s *GrpcGatewayServer) ForwardMessage(ctx context.Context, req *proto.ForwardRequest) (*proto.ForwardResponse, error) {
	if req.Message == nil {
		return &proto.ForwardResponse{Delivered: false, Error: "message is nil"}, nil
	}
	if req.Uid == "" {
		return &proto.ForwardResponse{Delivered: false, Error: "uid is empty"}, nil
	}

	// 路由时信任转发网关提供的 req.Uid,但保留
	// 原始 message.To 用于显示。
	target := s.clients.Get(ctx, req.Uid)
	if target != nil {
		if err := target.Send(req.Message); err != nil {
			// 本地目标在线但发送缓冲区已满 —— 转存离线。
			s.offline.StoreOffline(ctx, req.Uid, req.Message)
			log.Printf("[grpc-server] node=%s: target %s online but send failed, stored offline: %v",
				s.nodeID, req.Uid, err)
			return &proto.ForwardResponse{Delivered: false, Error: err.Error()}, nil
		}
		return &proto.ForwardResponse{Delivered: true}, nil
	}

	// 目标未在本节点连接 —— 转存离线。
	s.offline.StoreOffline(ctx, req.Uid, req.Message)
	log.Printf("[grpc-server] node=%s: target %s not connected, stored offline", s.nodeID, req.Uid)
	return &proto.ForwardResponse{Delivered: false, Error: "target not locally connected, stored offline"}, nil
}

// StartGrpcServer 启动监听 addr 的 gRPC 服务器,注册
// Gateway 服务处理器,并返回服务器。生命周期由调用方负责
// (关闭时调用 GracefulStop)。
func StartGrpcServer(addr string, handler proto.GatewayServer) (*grpc.Server, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("grpc listen on %s: %w", addr, err)
	}

	srv := grpc.NewServer()
	proto.RegisterGatewayServer(srv, handler)

	go func() {
		log.Printf("[grpc-server] listening on %s", addr)
		if err := srv.Serve(lis); err != nil {
			log.Printf("[grpc-server] serve error: %v", err)
		}
	}()

	return srv, nil
}
