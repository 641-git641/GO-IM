// Package kit 提供 IM 客户端的通用辅助函数,供压测工具 (bench/loadtest)
// 与集成测试 (cmd/gateway/*_test.go) 共用。
//
// 逻辑照搬自 cmd/gateway/integration_test.go 与 cmd/gateway/tcp_integration_test.go,
// 但返回 error 而非调用 testing.T 的失败方法,因此可在非 _test 代码中复用。
package kit

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
	"github.com/im/api/proto"
	pb "google.golang.org/protobuf/proto"
)

// 默认读写超时。压测中每个场景可覆盖。
const defaultTimeout = 5 * time.Second

// ---------- WebSocket ----------

// WSClient 包装一个已连接的 WebSocket,提供消息级读写。
type WSClient struct {
	Conn *websocket.Conn
}

// Login 通过 HTTP 登录接口获取 JWT 令牌。
// 支持 dev_mode(仅 uid+username)与生产模式(uid+password)。
func Login(baseURL, uid, username, password string) (token, returnedUID string, err error) {
	form := url.Values{}
	form.Set("uid", uid)
	if username != "" {
		form.Set("username", username)
	}
	if password != "" {
		form.Set("password", password)
	}

	resp, err := http.Post(baseURL, "application/x-www-form-urlencoded", bytes.NewBufferString(form.Encode()))
	if err != nil {
		return "", "", fmt.Errorf("login http: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", fmt.Errorf("login read body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("login returned %d: %s", resp.StatusCode, body)
	}

	var result struct {
		UID      string `json:"uid"`
		Username string `json:"username"`
		Token    string `json:"token"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", "", fmt.Errorf("decode login response: %w", err)
	}
	return result.Token, result.UID, nil
}

// LoginDev 是 Login 在 dev_mode 下的便捷包装(无密码)。
func LoginDev(baseURL, uid, username string) (token, returnedUID string, err error) {
	return Login(baseURL, uid, username, "")
}

// ConnectWS 使用 JWT 令牌建立 WebSocket 连接。
func ConnectWS(wsURL, token string) (*WSClient, error) {
	u, err := url.Parse(wsURL)
	if err != nil {
		return nil, fmt.Errorf("parse ws url: %w", err)
	}
	q := u.Query()
	q.Set("token", token)
	u.RawQuery = q.Encode()

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("ws connect: %w", err)
	}
	return &WSClient{Conn: conn}, nil
}

// ReadRaw 读取一条原始 WebSocket 消息(类型 + 载荷)。
func (c *WSClient) ReadRaw(timeout time.Duration) (int, []byte, error) {
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	c.Conn.SetReadDeadline(time.Now().Add(timeout))
	return c.Conn.ReadMessage()
}

// ReadMessage 从 WebSocket 读取并解码一条 proto.Message。
func (c *WSClient) ReadMessage(timeout time.Duration) (*proto.Message, error) {
	_, raw, err := c.ReadRaw(timeout)
	if err != nil {
		return nil, fmt.Errorf("read ws: %w", err)
	}
	msg := &proto.Message{}
	if err := pb.Unmarshal(raw, msg); err != nil {
		return nil, fmt.Errorf("unmarshal ws message: %w", err)
	}
	return msg, nil
}

// WriteMessage 将 proto.Message 编码并通过 WebSocket 发送。
func (c *WSClient) WriteMessage(msg *proto.Message, timeout time.Duration) error {
	data, err := pb.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	c.Conn.SetWriteDeadline(time.Now().Add(timeout))
	if err := c.Conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
		return fmt.Errorf("write ws message: %w", err)
	}
	return nil
}

// Close 关闭 WebSocket 连接。
func (c *WSClient) Close() error {
	if c == nil || c.Conn == nil {
		return nil
	}
	return c.Conn.Close()
}

// DrainLoginResp 读取并返回登录响应(第一条消息应为 CmdLoginResp)。
func (c *WSClient) DrainLoginResp(timeout time.Duration) (*proto.Message, error) {
	msg, err := c.ReadMessage(timeout)
	if err != nil {
		return nil, err
	}
	if msg.Cmd != proto.CmdLoginResp {
		return nil, fmt.Errorf("expected login response, got cmd=%d", msg.Cmd)
	}
	return msg, nil
}

// SendHeartbeat 发送一条心跳并等待响应。
func (c *WSClient) SendHeartbeat(timeout time.Duration) error {
	if err := c.WriteMessage(&proto.Message{Cmd: proto.CmdHeartbeat}, timeout); err != nil {
		return err
	}
	_, err := c.ReadMessage(timeout)
	return err
}

// ---------- gnet TCP ----------

// TCPClient 包装一条 gnet TCP 连接,提供帧级读写
// (4 字节大端长度前缀 + protobuf 载荷)。
type TCPClient struct {
	Conn net.Conn
}

// ConnectTCP 拨号连接 gnet TCP 端口。
func ConnectTCP(addr string) (*TCPClient, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("tcp dial to %s: %w", addr, err)
	}
	return &TCPClient{Conn: conn}, nil
}

// ReadFrame 从 TCP 连接读取一条带帧的 protobuf 消息。
func (c *TCPClient) ReadFrame(timeout time.Duration) (*proto.Message, error) {
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	c.Conn.SetReadDeadline(time.Now().Add(timeout))

	// 读取 4 字节长度头。
	header := make([]byte, 4)
	if _, err := io.ReadFull(c.Conn, header); err != nil {
		return nil, fmt.Errorf("read tcp header: %w", err)
	}
	length := binary.BigEndian.Uint32(header)
	if length > 65536 {
		return nil, fmt.Errorf("tcp frame too large: %d", length)
	}

	// 读取载荷。
	payload := make([]byte, length)
	if _, err := io.ReadFull(c.Conn, payload); err != nil {
		return nil, fmt.Errorf("read tcp payload (%d bytes): %w", length, err)
	}

	msg := &proto.Message{}
	if err := pb.Unmarshal(payload, msg); err != nil {
		return nil, fmt.Errorf("unmarshal tcp frame: %w", err)
	}
	return msg, nil
}

// WriteFrame 将 proto.Message 作为带帧的 TCP 消息发送。
func (c *TCPClient) WriteFrame(msg *proto.Message, timeout time.Duration) error {
	data, err := pb.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal tcp message: %w", err)
	}

	// 4 字节大端长度前缀 + 载荷。
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(data)))
	frame := append(header, data...)

	if timeout <= 0 {
		timeout = defaultTimeout
	}
	c.Conn.SetWriteDeadline(time.Now().Add(timeout))
	if _, err := c.Conn.Write(frame); err != nil {
		return fmt.Errorf("write tcp frame: %w", err)
	}
	return nil
}

// Close 关闭 TCP 连接。
func (c *TCPClient) Close() error {
	if c == nil || c.Conn == nil {
		return nil
	}
	return c.Conn.Close()
}

// Login 通过 TCP 发送登录帧并读取登录响应。
func (c *TCPClient) Login(token string, timeout time.Duration) (*proto.Message, error) {
	if err := c.WriteFrame(&proto.Message{Cmd: proto.CmdLogin, Content: token}, timeout); err != nil {
		return nil, err
	}
	return c.ReadFrame(timeout)
}

// SendHeartbeat 通过 TCP 发送心跳并等待响应。
func (c *TCPClient) SendHeartbeat(timeout time.Duration) error {
	if err := c.WriteFrame(&proto.Message{Cmd: proto.CmdHeartbeat}, timeout); err != nil {
		return err
	}
	_, err := c.ReadFrame(timeout)
	return err
}

// ---------- HTTP 辅助 ----------

// WaitHealthy 轮询 /health 直到返回 200,或超时。
func WaitHealthy(healthURL string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(healthURL)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("health endpoint %s not ready within %s", healthURL, timeout)
}

// GetHealth 获取 /health 的 JSON 响应结构(供压测记录服务端状态)。
func GetHealth(healthURL string) (*HealthStatus, error) {
	resp, err := http.Get(healthURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var h HealthStatus
	if err := json.Unmarshal(body, &h); err != nil {
		return nil, fmt.Errorf("decode health: %w", err)
	}
	return &h, nil
}

// HealthStatus 对应 /health 的响应体。
type HealthStatus struct {
	Status       string            `json:"status"`
	Connections  int               `json:"connections"`
	Dependencies map[string]string `json:"dependencies"`
	Memory       struct {
		AllocMB    int `json:"alloc_mb"`
		Goroutines int `json:"goroutines"`
	} `json:"memory"`
}

// HTTPGetJSON 带 JWT 的 GET 请求,返回响应体。用于 /search 等端点。
func HTTPGetJSON(baseURL, path string, params url.Values) ([]byte, error) {
	u, err := url.Parse(baseURL + path)
	if err != nil {
		return nil, err
	}
	u.RawQuery = params.Encode()

	resp, err := http.Get(u.String())
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s returned %d: %s", path, resp.StatusCode, body)
	}
	return body, nil
}
