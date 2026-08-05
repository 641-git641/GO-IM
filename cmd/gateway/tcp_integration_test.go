package main

// 本文件包含针对 gnet TCP 传输的附加集成测试。
// 它作为 main 包的一部分编译。客户端辅助函数复用 bench/kit。

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/im/api/proto"
	"github.com/im/bench/kit"
	"github.com/im/configs"
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
	if err := kit.WaitHealthy("http://localhost:18080/health", 5*time.Second); err != nil {
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

// ---------- gnet TCP 集成测试 ----------

// TestIntegrationGNetEndToEnd 测试完整的 gnet TCP 流程：连接 -> 登录 -> 聊天 -> ACK。
func TestIntegrationGNetEndToEnd(t *testing.T) {
	_, cleanup := startTestServerGNet(t)
	defer cleanup()

	// 通过 HTTP 获取 JWT 令牌（同一台服务器）。
	aliceToken, aliceUID, err := kit.LoginDev(testLoginURL, "alice_tcp", "Alice")
	if err != nil {
		t.Fatal(err)
	}
	bobToken, bobUID, err := kit.LoginDev(testLoginURL, "bob_tcp", "Bob")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Tokens: alice=%s bob=%s", aliceUID, bobUID)

	// Alice 通过 gnet TCP 连接。
	aliceConn, err := kit.ConnectTCP(testTCPAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer aliceConn.Close()

	// Bob 通过 WebSocket 连接（混合传输测试）。
	bobConn, err := kit.ConnectWS(testWSURL, bobToken)
	if err != nil {
		t.Fatal(err)
	}
	defer bobConn.Close()

	// 排空 Bob 的登录响应（WebSocket）。
	if _, err := bobConn.DrainLoginResp(0); err != nil {
		t.Fatal(err)
	}
	t.Log("Bob logged in via WebSocket")

	// Alice 通过 TCP 发送登录帧。
	aliceLoginResp, err := aliceConn.Login(aliceToken, 0)
	if err != nil {
		t.Fatal(err)
	}
	if aliceLoginResp.Cmd != proto.CmdLoginResp {
		t.Fatalf("Alice: expected CmdLoginResp, got cmd=%d", aliceLoginResp.Cmd)
	}
	t.Logf("Alice logged in via TCP: uid=%s", aliceLoginResp.To)

	// Alice 通过 TCP 向 Bob 发送聊天消息。
	if err := aliceConn.WriteFrame(&proto.Message{
		Cmd:      proto.CmdChat,
		To:       bobUID,
		ChatType: proto.ChatTypeSingle,
		MsgType:  proto.MsgTypeText,
		Content:  "Hello from gnet TCP!",
		NeedAck:  true,
		Seq:      1,
	}, 0); err != nil {
		t.Fatal(err)
	}
	t.Log("Alice sent chat via TCP")

	// Bob 通过 WebSocket 收到消息。
	received, err := bobConn.ReadMessage(0)
	if err != nil {
		t.Fatal(err)
	}
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
	ack, err := aliceConn.ReadFrame(0)
	if err != nil {
		t.Fatal(err)
	}
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

	aliceToken, aliceUID, err := kit.LoginDev(testLoginURL, "alice_gnet_off", "Alice")
	if err != nil {
		t.Fatal(err)
	}
	bobToken, bobUID, err := kit.LoginDev(testLoginURL, "bob_gnet_off", "Bob")
	if err != nil {
		t.Fatal(err)
	}

	// Alice 通过 gnet TCP 连接。
	aliceConn, err := kit.ConnectTCP(testTCPAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer aliceConn.Close()

	// Alice 通过 TCP 登录。
	aliceLoginResp, err := aliceConn.Login(aliceToken, 0)
	if err != nil {
		t.Fatal(err)
	}
	if aliceLoginResp.Cmd != proto.CmdLoginResp {
		t.Fatalf("Alice: expected CmdLoginResp, got cmd=%d", aliceLoginResp.Cmd)
	}
	t.Log("Alice logged in via TCP")

	// Alice 通过 TCP 向离线的 Bob 发送消息。
	if err := aliceConn.WriteFrame(&proto.Message{
		Cmd:      proto.CmdChat,
		To:       bobUID,
		ChatType: proto.ChatTypeSingle,
		MsgType:  proto.MsgTypeText,
		Content:  "Offline TCP message",
		NeedAck:  true,
		Seq:      1,
	}, 0); err != nil {
		t.Fatal(err)
	}

	// Alice 收到 ACK（消息已离线存储）。
	ack, err := aliceConn.ReadFrame(0)
	if err != nil {
		t.Fatal(err)
	}
	if ack.Cmd != proto.CmdAck {
		t.Fatalf("expected ACK, got cmd=%d", ack.Cmd)
	}
	t.Log("Alice got ACK via TCP")

	// Bob 通过 gnet TCP 连接。
	bobConn, err := kit.ConnectTCP(testTCPAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer bobConn.Close()

	// Bob 通过 TCP 登录。
	bobLoginResp, err := bobConn.Login(bobToken, 0)
	if err != nil {
		t.Fatal(err)
	}
	if bobLoginResp.Cmd != proto.CmdLoginResp {
		t.Fatalf("Bob: expected CmdLoginResp, got cmd=%d", bobLoginResp.Cmd)
	}
	t.Log("Bob logged in via TCP")

	// Bob 通过 TCP 请求离线消息。
	if err := bobConn.WriteFrame(&proto.Message{Cmd: proto.CmdOffline}, 0); err != nil {
		t.Fatal(err)
	}

	// Bob 通过 TCP 收到离线消息。
	received, err := bobConn.ReadFrame(0)
	if err != nil {
		t.Fatal(err)
	}
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

	token, uid := mustLogin(t, "hb_tcp", "Heartbeat")
	_ = uid

	conn, err := kit.ConnectTCP(testTCPAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// 通过 TCP 登录。
	loginResp, err := conn.Login(token, 0)
	if err != nil {
		t.Fatal(err)
	}
	if loginResp.Cmd != proto.CmdLoginResp {
		t.Fatalf("expected CmdLoginResp, got cmd=%d", loginResp.Cmd)
	}
	t.Log("Logged in via TCP")

	// 通过 TCP 发送心跳。
	if err := conn.SendHeartbeat(0); err != nil {
		t.Fatal(err)
	}
	t.Logf("Heartbeat response via TCP ✓")
}

// mustLogin 登录并返回 JWT 令牌,失败即终止测试。
func mustLogin(t *testing.T, uid, username string) (token, returnedUID string) {
	t.Helper()
	token, returnedUID, err := kit.LoginDev(testLoginURL, uid, username)
	if err != nil {
		t.Fatal(err)
	}
	return token, returnedUID
}
