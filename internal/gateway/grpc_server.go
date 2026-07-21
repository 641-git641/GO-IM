package gateway

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/im/api/proto"
	"google.golang.org/grpc"
)

// GrpcGatewayServer implements proto.GatewayServer for inter-gateway message
// forwarding. When a peer Gateway forwards a message to this node, the handler
// attempts local delivery; if the target is not locally connected, the message
// is stored in the offline store.
type GrpcGatewayServer struct {
	proto.UnimplementedGatewayServer
	clients ClientRegistry
	offline OfflineStore
	nodeID  string
}

// NewGrpcGatewayServer creates a GrpcGatewayServer backed by the given
// connection registry and offline store.
func NewGrpcGatewayServer(clients ClientRegistry, offline OfflineStore, nodeID string) *GrpcGatewayServer {
	return &GrpcGatewayServer{
		clients: clients,
		offline: offline,
		nodeID:  nodeID,
	}
}

// ForwardMessage handles an inter-gateway message forward request from a peer.
// It tries to deliver the message to a locally connected client; if the target
// is offline, the message is stored for later delivery.
func (s *GrpcGatewayServer) ForwardMessage(ctx context.Context, req *proto.ForwardRequest) (*proto.ForwardResponse, error) {
	if req.Message == nil {
		return &proto.ForwardResponse{Delivered: false, Error: "message is nil"}, nil
	}
	if req.Uid == "" {
		return &proto.ForwardResponse{Delivered: false, Error: "uid is empty"}, nil
	}

	// Trust req.Uid from the forwarding gateway for routing, but preserve
	// the original message.To for display purposes.
	target := s.clients.Get(ctx, req.Uid)
	if target != nil {
		if err := target.Send(req.Message); err != nil {
			// Local target online but send buffer full — store offline.
			s.offline.StoreOffline(ctx, req.Uid, req.Message)
			log.Printf("[grpc-server] node=%s: target %s online but send failed, stored offline: %v",
				s.nodeID, req.Uid, err)
			return &proto.ForwardResponse{Delivered: false, Error: err.Error()}, nil
		}
		return &proto.ForwardResponse{Delivered: true}, nil
	}

	// Target not locally connected — store offline.
	s.offline.StoreOffline(ctx, req.Uid, req.Message)
	log.Printf("[grpc-server] node=%s: target %s not connected, stored offline", s.nodeID, req.Uid)
	return &proto.ForwardResponse{Delivered: false, Error: "target not locally connected, stored offline"}, nil
}

// StartGrpcServer starts a gRPC server listening on addr, registers the
// Gateway service handler, and returns the server. The caller owns lifecycle
// (GracefulStop on shutdown).
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
