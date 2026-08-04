package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/im/api/proto"
	"github.com/im/configs"
	pb "google.golang.org/protobuf/proto"
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
	for i := 0; i < 50; i++ {
		time.Sleep(100 * time.Millisecond)
		_, err = http.Get("http://localhost:18080/health")
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
		time.Sleep(100 * time.Millisecond)
	}
}

// login 为测试用户获取 JWT 令牌。
func login(t *testing.T, uid, username string) (token, returnedUID string) {
	t.Helper()
	resp, err := http.Post("http://localhost:18080/login",
		"application/x-www-form-urlencoded",
		bytes.NewBufferString(fmt.Sprintf("uid=%s&username=%s", uid, username)))
	if err != nil {
		t.Fatalf("login failed: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("login returned %d: %s", resp.StatusCode, body)
	}

	var result struct {
		UID      string `json:"uid"`
		Username string `json:"username"`
		Token    string `json:"token"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("decode login response: %v", err)
	}
	return result.Token, result.UID
}

// connectWS 使用 JWT 令牌建立 WebSocket 连接。
func connectWS(t *testing.T, token string) *websocket.Conn {
	t.Helper()
	u := url.URL{Scheme: "ws", Host: "localhost:18080", Path: "/ws", RawQuery: "token=" + token}
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		t.Fatalf("ws connect failed: %v", err)
	}
	return conn
}

// readRawWS 读取一条原始 WebSocket 消息。
func readRawWS(t *testing.T, conn *websocket.Conn) (int, []byte, error) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	return conn.ReadMessage()
}

// readMessage 从 WebSocket 读取 proto.Message。
func readMessage(t *testing.T, conn *websocket.Conn) *proto.Message {
	t.Helper()
	_, raw, err := readRawWS(t, conn)
	if err != nil {
		t.Fatalf("read ws: %v", err)
	}
	msg := &proto.Message{}
	if err := pb.Unmarshal(raw, msg); err != nil {
		t.Fatalf("unmarshal ws message (%s): %v", string(raw), err)
	}
	return msg
}

// writeMessage 通过 WebSocket 发送 proto.Message。
func writeMessage(t *testing.T, conn *websocket.Conn, msg *proto.Message) {
	t.Helper()
	data, err := pb.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal message: %v", err)
	}
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err := conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
		t.Fatalf("write ws message: %v", err)
	}
}

// TestIntegrationEndToEnd 测试：登录 → 连接 → 发送 → 接收 → ACK
func TestIntegrationEndToEnd(t *testing.T) {
	_, cleanup := startTestServer(t)
	defer cleanup()

	// 登录
	aliceToken, aliceUID := login(t, "alice", "Alice")
	bobToken, bobUID := login(t, "bob", "Bob")
	t.Logf("Logged in: alice=%s bob=%s", aliceUID, bobUID)

	// 双方通过 WebSocket 连接
	aliceConn := connectWS(t, aliceToken)
	defer aliceConn.Close()
	bobConn := connectWS(t, bobToken)
	defer bobConn.Close()
	t.Log("Both clients connected")

	// 排空登录响应（Cmd=CmdLoginResp）
	aliceLoginResp := readMessage(t, aliceConn)
	if aliceLoginResp.Cmd != proto.CmdLoginResp {
		t.Fatalf("expected login response, got cmd=%d", aliceLoginResp.Cmd)
	}
	t.Logf("Alice login response: uid=%s", aliceLoginResp.To)
	bobLoginResp := readMessage(t, bobConn)
	if bobLoginResp.Cmd != proto.CmdLoginResp {
		t.Fatalf("expected login response, got cmd=%d", bobLoginResp.Cmd)
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
	writeMessage(t, aliceConn, chatMsg)
	t.Log("Alice sent message to Bob")

	// Bob 应当收到该消息
	received := readMessage(t, bobConn)
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
	ack := readMessage(t, aliceConn)
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
	aliceToken, aliceUID := login(t, "alice_offline", "Alice")
	bobToken, bobUID := login(t, "bob_offline", "Bob")

	// Alice 连接，Bob 保持离线
	aliceConn := connectWS(t, aliceToken)
	defer aliceConn.Close()

	// 排空登录响应
	aliceLoginResp := readMessage(t, aliceConn)
	if aliceLoginResp.Cmd != proto.CmdLoginResp {
		t.Fatalf("expected login response, got cmd=%d", aliceLoginResp.Cmd)
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
	writeMessage(t, aliceConn, chatMsg)

	// Alice 收到 ACK（消息已离线存储）
	ack := readMessage(t, aliceConn)
	if ack.Cmd != proto.CmdAck {
		t.Fatalf("expected ACK, got %d", ack.Cmd)
	}
	t.Log("Alice got ACK (offline message stored) ✓")

	// Bob 连接并请求离线消息
	bobConn := connectWS(t, bobToken)
	defer bobConn.Close()

	// 排空登录响应
	bobLoginResp := readMessage(t, bobConn)
	if bobLoginResp.Cmd != proto.CmdLoginResp {
		t.Fatalf("expected login response, got cmd=%d", bobLoginResp.Cmd)
	}

	// 请求离线消息
	writeMessage(t, bobConn, &proto.Message{Cmd: proto.CmdOffline})

	// Bob 应当收到离线消息
	received := readMessage(t, bobConn)
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
