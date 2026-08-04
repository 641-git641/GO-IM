package main

// 本文件包含针对 gnet TCP 传输的附加集成测试。
// 它作为 main 包的一部分编译。

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/im/api/proto"
	"github.com/im/configs"
	pb "google.golang.org/protobuf/proto"
)

// ---------- gnet TCP 辅助函数 ----------

// startTestServerGNet 以双传输模式启动服务器，用于 TCP 测试。
func startTestServerGNet(t *testing.T) (*App, func()) {
	t.Helper()
	cfg := configs.Default()
	cfg.Gateway.HTTPAddr = ":18080" // HTTP/WebSocket
	cfg.Gateway.TCPAddr = ":18081"  // gnet TCP
	cfg.Gateway.Transport = "both"  // 启用双传输
	cfg.JWT.Secret = "test-secret"
	cfg.Gateway.RateLimit.Enabled = false

	app, err := NewApp(cfg)
	if err != nil {
		t.Fatalf("NewApp: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		if err := app.Run(ctx); err != nil && err != http.ErrServerClosed {
			t.Logf("server stopped: %v", err)
		}
	}()

	// 等待服务器就绪（HTTP 和 gnet 均需就绪）。
	time.Sleep(500 * time.Millisecond)
	for i := 0; i < 50; i++ {
		time.Sleep(100 * time.Millisecond)
		_, err := http.Get("http://localhost:18080/health")
		if err == nil {
			break
		}
	}
	if err != nil {
		cancel()
		t.Fatalf("server did not start: %v", err)
	}

	return app, func() {
		cancel()
		// 等待 HTTP 和 gnet 端口都被释放。
		for i := 0; i < 30; i++ {
			time.Sleep(100 * time.Millisecond)
			// 检查 HTTP 端口。
			httpConn, httpErr := net.DialTimeout("tcp", "localhost:18080", 50*time.Millisecond)
			if httpErr == nil {
				httpConn.Close()
			}
			// 检查 gnet TCP 端口。
			tcpConn, tcpErr := net.DialTimeout("tcp", "localhost:18081", 50*time.Millisecond)
			if tcpErr == nil {
				tcpConn.Close()
			}
			if httpErr != nil && tcpErr != nil {
				break // 两个端口都已释放
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// connectTCP 拨号连接 gnet TCP 端口。
func connectTCP(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("TCP dial to %s: %v", addr, err)
	}
	return conn
}

// readTCPFrame 从 TCP 连接读取一条带帧的 protobuf 消息。
func readTCPFrame(t *testing.T, conn net.Conn) *proto.Message {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	// 读取 4 字节长度头。
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		t.Fatalf("read TCP header: %v", err)
	}
	length := binary.BigEndian.Uint32(header)
	if length > 65536 {
		t.Fatalf("TCP frame too large: %d", length)
	}

	// 读取载荷。
	payload := make([]byte, length)
	if _, err := io.ReadFull(conn, payload); err != nil {
		t.Fatalf("read TCP payload (%d bytes): %v", length, err)
	}

	msg := &proto.Message{}
	if err := pb.Unmarshal(payload, msg); err != nil {
		t.Fatalf("unmarshal TCP frame: %v", err)
	}
	return msg
}

// writeTCPFrame 将 proto.Message 作为带帧的 TCP 消息发送。
func writeTCPFrame(t *testing.T, conn net.Conn, msg *proto.Message) {
	t.Helper()
	data, err := pb.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal TCP message: %v", err)
	}

	// 4 字节大端长度前缀 + 载荷。
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(data)))
	frame := append(header, data...)

	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write(frame); err != nil {
		t.Fatalf("write TCP frame: %v", err)
	}
}

// ---------- gnet TCP 集成测试 ----------

// TestIntegrationGNetEndToEnd 测试完整的 gnet TCP 流程：连接 -> 登录 -> 聊天 -> ACK。
func TestIntegrationGNetEndToEnd(t *testing.T) {
	_, cleanup := startTestServerGNet(t)
	defer cleanup()

	// 通过 HTTP 获取 JWT 令牌（同一台服务器）。
	aliceToken, aliceUID := login(t, "alice_tcp", "Alice")
	bobToken, bobUID := login(t, "bob_tcp", "Bob")
	t.Logf("Tokens: alice=%s bob=%s", aliceUID, bobUID)

	// Alice 通过 gnet TCP 连接。
	aliceConn := connectTCP(t, "localhost:18081")
	defer aliceConn.Close()

	// Bob 通过 WebSocket 连接（混合传输测试）。
	bobConn := connectWS(t, bobToken)
	defer bobConn.Close()

	// 排空 Bob 的登录响应（WebSocket）。
	bobLoginResp := readMessage(t, bobConn)
	if bobLoginResp.Cmd != proto.CmdLoginResp {
		t.Fatalf("Bob: expected login response, got cmd=%d", bobLoginResp.Cmd)
	}
	t.Log("Bob logged in via WebSocket")

	// Alice 通过 TCP 发送登录帧。
	writeTCPFrame(t, aliceConn, &proto.Message{
		Cmd:     proto.CmdLogin,
		Content: aliceToken,
	})

	// Alice 通过 TCP 读取登录响应。
	aliceLoginResp := readTCPFrame(t, aliceConn)
	if aliceLoginResp.Cmd != proto.CmdLoginResp {
		t.Fatalf("Alice: expected CmdLoginResp, got cmd=%d", aliceLoginResp.Cmd)
	}
	t.Logf("Alice logged in via TCP: uid=%s", aliceLoginResp.To)

	// Alice 通过 TCP 向 Bob 发送聊天消息。
	writeTCPFrame(t, aliceConn, &proto.Message{
		Cmd:      proto.CmdChat,
		To:       bobUID,
		ChatType: proto.ChatTypeSingle,
		MsgType:  proto.MsgTypeText,
		Content:  "Hello from gnet TCP!",
		NeedAck:  true,
		Seq:      1,
	})
	t.Log("Alice sent chat via TCP")

	// Bob 通过 WebSocket 收到消息。
	received := readMessage(t, bobConn)
	if received.Cmd != proto.CmdChat {
		t.Fatalf("expected chat cmd, got %d", received.Cmd)
	}
	if received.From != aliceUID {
		t.Errorf("expected From=%s, got %s", aliceUID, received.From)
	}
	if received.Content != "Hello from gnet TCP!" {
		t.Errorf("expected 'Hello from gnet TCP!', got '%s'", received.Content)
	}
	if received.MsgId == 0 {
		t.Error("expected non-zero MsgID")
	}
	t.Logf("Bob received via WS: msgId=%d content=%s", received.MsgId, received.Content)

	// Alice 通过 TCP 收到 ACK。
	ack := readTCPFrame(t, aliceConn)
	if ack.Cmd != proto.CmdAck {
		t.Fatalf("expected ACK cmd, got %d", ack.Cmd)
	}
	if ack.MsgId != received.MsgId {
		t.Errorf("ACK MsgID mismatch: expected %d, got %d", received.MsgId, ack.MsgId)
	}
	t.Logf("Alice received ACK via TCP: msgId=%d", ack.MsgId)
}

// TestIntegrationGNetOfflineMessage 测试通过 gnet TCP 投递离线消息。
func TestIntegrationGNetOfflineMessage(t *testing.T) {
	_, cleanup := startTestServerGNet(t)
	defer cleanup()

	aliceToken, aliceUID := login(t, "alice_gnet_off", "Alice")
	bobToken, bobUID := login(t, "bob_gnet_off", "Bob")

	// Alice 通过 gnet TCP 连接。
	aliceConn := connectTCP(t, "localhost:18081")
	defer aliceConn.Close()

	// Alice 通过 TCP 登录。
	writeTCPFrame(t, aliceConn, &proto.Message{
		Cmd:     proto.CmdLogin,
		Content: aliceToken,
	})
	aliceLoginResp := readTCPFrame(t, aliceConn)
	if aliceLoginResp.Cmd != proto.CmdLoginResp {
		t.Fatalf("Alice: expected CmdLoginResp, got cmd=%d", aliceLoginResp.Cmd)
	}
	t.Log("Alice logged in via TCP")

	// Alice 通过 TCP 向离线的 Bob 发送消息。
	writeTCPFrame(t, aliceConn, &proto.Message{
		Cmd:      proto.CmdChat,
		To:       bobUID,
		ChatType: proto.ChatTypeSingle,
		MsgType:  proto.MsgTypeText,
		Content:  "Offline TCP message",
		NeedAck:  true,
		Seq:      1,
	})

	// Alice 收到 ACK（消息已离线存储）。
	ack := readTCPFrame(t, aliceConn)
	if ack.Cmd != proto.CmdAck {
		t.Fatalf("expected ACK, got cmd=%d", ack.Cmd)
	}
	t.Log("Alice got ACK via TCP")

	// Bob 通过 gnet TCP 连接。
	bobConn := connectTCP(t, "localhost:18081")
	defer bobConn.Close()

	// Bob 通过 TCP 登录。
	writeTCPFrame(t, bobConn, &proto.Message{
		Cmd:     proto.CmdLogin,
		Content: bobToken,
	})
	bobLoginResp := readTCPFrame(t, bobConn)
	if bobLoginResp.Cmd != proto.CmdLoginResp {
		t.Fatalf("Bob: expected CmdLoginResp, got cmd=%d", bobLoginResp.Cmd)
	}
	t.Log("Bob logged in via TCP")

	// Bob 通过 TCP 请求离线消息。
	writeTCPFrame(t, bobConn, &proto.Message{
		Cmd: proto.CmdOffline,
	})

	// Bob 通过 TCP 收到离线消息。
	received := readTCPFrame(t, bobConn)
	if received.Cmd != proto.CmdChat {
		t.Fatalf("expected chat cmd, got %d", received.Cmd)
	}
	if received.From != aliceUID {
		t.Errorf("expected From=%s, got %s", aliceUID, received.From)
	}
	if received.Content != "Offline TCP message" {
		t.Errorf("expected 'Offline TCP message', got '%s'", received.Content)
	}
	t.Logf("Bob received offline message via TCP: %s", received.Content)
}

// TestIntegrationGNetHeartbeat 测试 TCP 应用层心跳。
func TestIntegrationGNetHeartbeat(t *testing.T) {
	_, cleanup := startTestServerGNet(t)
	defer cleanup()

	token, uid := login(t, "hb_tcp", "Heartbeat")
	_ = uid

	conn := connectTCP(t, "localhost:18081")
	defer conn.Close()

	// 通过 TCP 登录。
	writeTCPFrame(t, conn, &proto.Message{
		Cmd:     proto.CmdLogin,
		Content: token,
	})
	loginResp := readTCPFrame(t, conn)
	if loginResp.Cmd != proto.CmdLoginResp {
		t.Fatalf("expected CmdLoginResp, got cmd=%d", loginResp.Cmd)
	}
	t.Log("Logged in via TCP")

	// 通过 TCP 发送心跳。
	writeTCPFrame(t, conn, &proto.Message{
		Cmd: proto.CmdHeartbeat,
	})

	// 通过 TCP 接收心跳响应。
	hbResp := readTCPFrame(t, conn)
	if hbResp.Cmd != proto.CmdHeartbeat {
		t.Fatalf("expected CmdHeartbeat response, got cmd=%d", hbResp.Cmd)
	}
	if hbResp.MsgId == 0 {
		t.Error("expected non-zero MsgID in heartbeat response")
	}
	t.Logf("Heartbeat response via TCP: msgId=%d", hbResp.MsgId)
}
