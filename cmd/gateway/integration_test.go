package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/im/api/proto"
	"github.com/im/bench/kit"
	"github.com/im/configs"
)

// 与 bench/kit 共享的测试目标地址。
const (
	testLoginURL = "http://localhost:18080/login"
	testWSURL    = "ws://localhost:18080/ws"
	testTCPAddr  = "localhost:18081"
)

// startTestServer 在 goroutine 中启动服务器并返回清理函数。
func startTestServer(t *testing.T) (*App, func()) {
	t.Helper()
	cfg := configs.Default()
	cfg.Gateway.HTTPAddr = ":18080" // 测试端口
	cfg.JWT.Secret = "test-secret"
	cfg.Gateway.RateLimit.Enabled = false // 测试中禁用限流

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

	// 等待服务器就绪
	if err := kit.WaitHealthy("http://localhost:18080/health", 5*time.Second); err != nil {
		cancel()
		t.Fatalf("server did not start: %v", err)
	}

	return app, func() {
		cancel()
		time.Sleep(100 * time.Millisecond)
	}
}

// TestIntegrationMetrics 验证 /metrics 端点可用并暴露 IM 指标。
func TestIntegrationMetrics(t *testing.T) {
	_, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Get("http://localhost:18080/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics: expected 200, got %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	for _, want := range []string{"im_online_connections", "im_rate_limit_allowed_total", "go_goroutines"} {
		if !bytes.Contains(body, []byte(want)) {
			t.Errorf("GET /metrics body missing %q", want)
		}
	}
}

// TestIntegrationEndToEnd 测试：登录 → 连接 → 发送 → 接收 → ACK
func TestIntegrationEndToEnd(t *testing.T) {
	_, cleanup := startTestServer(t)
	defer cleanup()

	// 登录
	aliceToken, aliceUID, err := kit.LoginDev(testLoginURL, "alice", "Alice")
	if err != nil {
		t.Fatal(err)
	}
	bobToken, bobUID, err := kit.LoginDev(testLoginURL, "bob", "Bob")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Logged in: alice=%s bob=%s", aliceUID, bobUID)

	// 双方通过 WebSocket 连接
	aliceConn, err := kit.ConnectWS(testWSURL, aliceToken)
	if err != nil {
		t.Fatal(err)
	}
	defer aliceConn.Close()
	bobConn, err := kit.ConnectWS(testWSURL, bobToken)
	if err != nil {
		t.Fatal(err)
	}
	defer bobConn.Close()
	t.Log("Both clients connected")

	// 排空登录响应（Cmd=CmdLoginResp）
	aliceLoginResp, err := aliceConn.DrainLoginResp(0)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Alice login response: uid=%s", aliceLoginResp.To)
	bobLoginResp, err := bobConn.DrainLoginResp(0)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Bob login response: uid=%s", bobLoginResp.To)

	// Alice 向 Bob 发送一条聊天消息
	chatMsg := &proto.Message{
		Cmd:      proto.CmdChat,
		To:       bobUID,
		ChatType: proto.ChatTypeSingle,
		MsgType:  proto.MsgTypeText,
		Content:  "Hello Bob!",
		NeedAck:  true,
	}
	if err := aliceConn.WriteMessage(chatMsg, 0); err != nil {
		t.Fatal(err)
	}
	t.Log("Alice sent message to Bob")

	// Bob 应当收到该消息
	received, err := bobConn.ReadMessage(0)
	if err != nil {
		t.Fatal(err)
	}
	if received.Cmd != proto.CmdChat {
		t.Fatalf("expected chat cmd, got %d", received.Cmd)
	}
	if received.From != aliceUID {
		t.Errorf("expected from=%s, got %s", aliceUID, received.From)
	}
	if received.Content != "Hello Bob!" {
		t.Errorf("expected content='Hello Bob!', got '%s'", received.Content)
	}
	if received.MsgId == 0 {
		t.Error("expected non-zero MsgID")
	}
	t.Logf("Bob received: msgId=%d from=%s content=%s", received.MsgId, received.From, received.Content)

	// Alice 应当收到 ACK
	ack, err := aliceConn.ReadMessage(0)
	if err != nil {
		t.Fatal(err)
	}
	if ack.Cmd != proto.CmdAck {
		t.Fatalf("expected ACK cmd, got %d", ack.Cmd)
	}
	if ack.MsgId != received.MsgId {
		t.Errorf("ACK MsgID mismatch: expected %d, got %d", received.MsgId, ack.MsgId)
	}
	t.Logf("Alice received ACK for msgId=%d ✓", ack.MsgId)
}

// TestOfflineMessage 测试离线存储与重连后的投递。
func TestOfflineMessage(t *testing.T) {
	_, cleanup := startTestServer(t)
	defer cleanup()

	// 两个用户都登录
	aliceToken, aliceUID, err := kit.LoginDev(testLoginURL, "alice_offline", "Alice")
	if err != nil {
		t.Fatal(err)
	}
	bobToken, bobUID, err := kit.LoginDev(testLoginURL, "bob_offline", "Bob")
	if err != nil {
		t.Fatal(err)
	}

	// Alice 连接，Bob 保持离线
	aliceConn, err := kit.ConnectWS(testWSURL, aliceToken)
	if err != nil {
		t.Fatal(err)
	}
	defer aliceConn.Close()

	// 排空登录响应
	if _, err := aliceConn.DrainLoginResp(0); err != nil {
		t.Fatal(err)
	}
	t.Log("Alice connected (Bob is offline)")

	// Alice 向离线的 Bob 发送一条消息
	chatMsg := &proto.Message{
		Cmd:      proto.CmdChat,
		To:       bobUID,
		ChatType: proto.ChatTypeSingle,
		MsgType:  proto.MsgTypeText,
		Content:  "Are you there, Bob?",
		NeedAck:  true,
	}
	if err := aliceConn.WriteMessage(chatMsg, 0); err != nil {
		t.Fatal(err)
	}

	// Alice 收到 ACK（消息已离线存储）
	ack, err := aliceConn.ReadMessage(0)
	if err != nil {
		t.Fatal(err)
	}
	if ack.Cmd != proto.CmdAck {
		t.Fatalf("expected ACK, got %d", ack.Cmd)
	}
	t.Log("Alice got ACK (offline message stored) ✓")

	// Bob 连接并请求离线消息
	bobConn, err := kit.ConnectWS(testWSURL, bobToken)
	if err != nil {
		t.Fatal(err)
	}
	defer bobConn.Close()

	// 排空登录响应
	if _, err := bobConn.DrainLoginResp(0); err != nil {
		t.Fatal(err)
	}

	// 请求离线消息
	if err := bobConn.WriteMessage(&proto.Message{Cmd: proto.CmdOffline}, 0); err != nil {
		t.Fatal(err)
	}

	// Bob 应当收到离线消息
	received, err := bobConn.ReadMessage(0)
	if err != nil {
		t.Fatal(err)
	}
	if received.Cmd != proto.CmdChat {
		t.Fatalf("expected chat cmd, got %d", received.Cmd)
	}
	if received.From != aliceUID {
		t.Errorf("expected from=%s, got %s", aliceUID, received.From)
	}
	if received.Content != "Are you there, Bob?" {
		t.Errorf("expected content='Are you there, Bob?', got '%s'", received.Content)
	}
	t.Logf("Bob received offline message: %s ✓", received.Content)
}
