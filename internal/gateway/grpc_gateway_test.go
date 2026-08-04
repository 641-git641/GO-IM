package gateway

import (
	"context"
	"net"
	"testing"

	"github.com/im/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// setupGrpcGatewayServer 使用 bufconn 监听器创建进程内 gRPC 服务器，
// 返回客户端连接和清理函数。
func setupGrpcGatewayServer(t *testing.T, clients ClientRegistry, offline OfflineStore) (*grpc.ClientConn, func()) {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	handler := NewGrpcGatewayServer(clients, offline, "test-gw")
	proto.RegisterGatewayServer(srv, handler)

	go func() {
		if err := srv.Serve(lis); err != nil {
			// srv.Stop() 会触发该错误；不是真正的错误。
			return
		}
	}()

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("bufconn dial: %v", err)
	}

	cleanup := func() {
		conn.Close()
		srv.Stop()
	}

	return conn, cleanup
}

func TestGrpcForwardMessageDelivered(t *testing.T) {
	h := NewHub(100)

	// 在 hub 中注册目标客户端。
	target := newTestClient(t, "bob", "Bob")
	h.Register(context.Background(), target)

	conn, cleanup := setupGrpcGatewayServer(t, h, h)
	defer cleanup()

	client := proto.NewGatewayClient(conn)

	resp, err := client.ForwardMessage(context.Background(), &proto.ForwardRequest{
		Message: &proto.Message{
			Cmd:      proto.CmdChat,
			From:     "alice",
			To:       "bob",
			ChatType: proto.ChatTypeSingle,
			MsgType:  proto.MsgTypeText,
			Content:  "hello from peer",
		},
		Uid: "bob",
	})

	if err != nil {
		t.Fatalf("ForwardMessage gRPC error: %v", err)
	}
	if !resp.Delivered {
		t.Errorf("expected Delivered=true, got false: %s", resp.Error)
	}

	// 验证目标客户端收到了消息。
	delivered := readMessageFromChan(t, target.send)
	if delivered.Content != "hello from peer" {
		t.Errorf("expected 'hello from peer', got '%s'", delivered.Content)
	}
}

func TestGrpcForwardMessageOffline(t *testing.T) {
	h := NewHub(100)
	// 没有注册客户端 → 目标离线。

	conn, cleanup := setupGrpcGatewayServer(t, h, h)
	defer cleanup()

	client := proto.NewGatewayClient(conn)

	resp, err := client.ForwardMessage(context.Background(), &proto.ForwardRequest{
		Message: &proto.Message{
			Cmd:      proto.CmdChat,
			From:     "alice",
			To:       "bob",
			ChatType: proto.ChatTypeSingle,
			MsgType:  proto.MsgTypeText,
			Content:  "bob is offline",
		},
		Uid: "bob",
	})

	if err != nil {
		t.Fatalf("ForwardMessage gRPC error: %v", err)
	}
	if resp.Delivered {
		t.Error("expected Delivered=false for offline user")
	}

	// 验证消息已存储在离线存储中。
	msgs := h.DrainOffline(context.Background(), "bob")
	if len(msgs) != 1 {
		t.Fatalf("expected 1 offline message, got %d", len(msgs))
	}
	if msgs[0].Content != "bob is offline" {
		t.Errorf("wrong content: '%s'", msgs[0].Content)
	}
}

func TestGrpcForwardMessageNilMessage(t *testing.T) {
	h := NewHub(100)

	conn, cleanup := setupGrpcGatewayServer(t, h, h)
	defer cleanup()

	client := proto.NewGatewayClient(conn)

	resp, err := client.ForwardMessage(context.Background(), &proto.ForwardRequest{
		Message: nil,
		Uid:     "bob",
	})

	if err != nil {
		t.Fatalf("ForwardMessage gRPC error: %v", err)
	}
	if resp.Delivered {
		t.Error("expected Delivered=false for nil message")
	}
}

func TestGrpcForwardMessageEmptyUID(t *testing.T) {
	h := NewHub(100)

	conn, cleanup := setupGrpcGatewayServer(t, h, h)
	defer cleanup()

	client := proto.NewGatewayClient(conn)

	resp, err := client.ForwardMessage(context.Background(), &proto.ForwardRequest{
		Message: &proto.Message{
			Cmd:     proto.CmdChat,
			From:    "alice",
			Content: "no target",
		},
		Uid: "",
	})

	if err != nil {
		t.Fatalf("ForwardMessage gRPC error: %v", err)
	}
	if resp.Delivered {
		t.Error("expected Delivered=false for empty UID")
	}
}

func TestGrpcForwardMessageSendBufferFull(t *testing.T) {
	h := NewHub(100)

	// 创建一个发送缓冲区极小且已满的目标客户端。
	target := newTestSmallBufClient(t, "bob", "Bob", 0)
	h.Register(context.Background(), target)

	conn, cleanup := setupGrpcGatewayServer(t, h, h)
	defer cleanup()

	client := proto.NewGatewayClient(conn)

	resp, err := client.ForwardMessage(context.Background(), &proto.ForwardRequest{
		Message: &proto.Message{
			Cmd:      proto.CmdChat,
			From:     "alice",
			To:       "bob",
			ChatType: proto.ChatTypeSingle,
			MsgType:  proto.MsgTypeText,
			Content:  "hello",
		},
		Uid: "bob",
	})

	if err != nil {
		t.Fatalf("ForwardMessage gRPC error: %v", err)
	}
	if resp.Delivered {
		t.Error("expected Delivered=false, send buffer is full")
	}

	// 消息应作为回退存储在离线存储中。
	msgs := h.DrainOffline(context.Background(), "bob")
	if len(msgs) != 1 {
		t.Fatalf("expected 1 fallback offline message, got %d", len(msgs))
	}
}

// newTestSmallBufClient 创建一个指定通道大小的客户端（0 = 从一开始就满）。
func newTestSmallBufClient(t *testing.T, uid, name string, bufSize int) *Client {
	t.Helper()
	return &Client{
		UID:      uid,
		Username: name,
		send:     make(chan []byte, bufSize),
		closed:   make(chan struct{}),
	}
}
