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

// setupGrpcGatewayServer creates an in-process gRPC server with a bufconn listener,
// returning a client connection and a cleanup function.
func setupGrpcGatewayServer(t *testing.T, clients ClientRegistry, offline OfflineStore) (*grpc.ClientConn, func()) {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	handler := NewGrpcGatewayServer(clients, offline, "test-gw")
	proto.RegisterGatewayServer(srv, handler)

	go func() {
		if err := srv.Serve(lis); err != nil {
			// srv.Stop() triggers this; not a real error.
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

	// Register target client in hub.
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

	// Verify target received the message.
	delivered := readMessageFromChan(t, target.send)
	if delivered.Content != "hello from peer" {
		t.Errorf("expected 'hello from peer', got '%s'", delivered.Content)
	}
}

func TestGrpcForwardMessageOffline(t *testing.T) {
	h := NewHub(100)
	// No client registered → target is offline.

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

	// Verify message was stored in offline store.
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

	// Create a target with a very small send buffer that's already full.
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

	// Message should be stored offline as fallback.
	msgs := h.DrainOffline(context.Background(), "bob")
	if len(msgs) != 1 {
		t.Fatalf("expected 1 fallback offline message, got %d", len(msgs))
	}
}

// newTestSmallBufClient creates a client with a channel of the given size (0 = full from start).
func newTestSmallBufClient(t *testing.T, uid, name string, bufSize int) *Client {
	t.Helper()
	return &Client{
		UID:      uid,
		Username: name,
		send:     make(chan []byte, bufSize),
		closed:   make(chan struct{}),
	}
}
