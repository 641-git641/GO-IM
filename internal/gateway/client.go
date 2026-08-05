package gateway

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/im/api/proto"
	pb "google.golang.org/protobuf/proto"
)

// Client.Send 返回的哨兵错误。
var (
	ErrSendBufferFull = errors.New("send buffer full")
	ErrClientClosed   = errors.New("client closed")
)

// Client 表示单个连接(经 gnet 的 WebSocket 或原始 TCP)。
type Client struct {
	UID      string
	Username string

	transport Transport
	clients   ClientRegistry
	send      chan []byte // 出站消息的缓冲通道
	ping      chan struct{}
	closed    chan struct{}
	closeOnce sync.Once

	// 应用层心跳跟踪(两种传输方式共用)。
	lastHeartbeat time.Time
	heartbeatMu   sync.Mutex
}

// NewClient 用 Transport 创建 Client 并注册它。
func NewClient(uid, username string, transport Transport, clients ClientRegistry, sendBufSize int) *Client {
	return &Client{
		UID:           uid,
		Username:      username,
		transport:     transport,
		clients:       clients,
		send:          make(chan []byte, sendBufSize),
		ping:          make(chan struct{}, 1),
		closed:        make(chan struct{}),
		lastHeartbeat: time.Now(),
	}
}

// Heartbeat 返回最后一次应用层心跳的时间。
func (c *Client) Heartbeat() time.Time {
	c.heartbeatMu.Lock()
	defer c.heartbeatMu.Unlock()
	return c.lastHeartbeat
}

// SetHeartbeat 将心跳时间戳更新为当前时间。
func (c *Client) SetHeartbeat(t time.Time) {
	c.heartbeatMu.Lock()
	defer c.heartbeatMu.Unlock()
	c.lastHeartbeat = t
}

// Send 将序列化后的 protobuf 消息推送到出站通道。
// 如果缓冲区已满或客户端已关闭,则返回错误。
func (c *Client) Send(msg *proto.Message) error {
	data, err := pb.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	select {
	case <-c.closed:
		return ErrClientClosed
	case c.send <- data:
		return nil
	default:
		return ErrSendBufferFull
	}
}

// Close 关闭客户端连接并通知写循环退出。
func (c *Client) Close() {
	c.closeOnce.Do(func() {
		close(c.closed)
		c.transport.Close()
	})
}

// SendPing 请求写循环发送一次传输层保活信号(WebSocket 为 Ping 帧)。
// 非阻塞:若写循环忙则跳过本次 ping(保活是尽力而为的)。
func (c *Client) SendPing() {
	select {
	case c.ping <- struct{}{}:
	default:
	}
}

// WriteLoop 排空 send 通道并写入 transport。
// 该 goroutine 取代了旧的 writePump —— 与传输方式无关。
// 它同时也是唯一的写者 goroutine:数据帧与 ping 控制帧都经由这里串行
// 写入,避免对同一底层连接并发写(gorilla/websocket 不允许并发写)。
func (c *Client) WriteLoop() {
	for {
		select {
		case <-c.closed:
			return
		case data, ok := <-c.send:
			if !ok {
				return
			}
			if err := c.transport.Write(data); err != nil {
				log.Printf("[client] write error uid=%s: %v", c.UID, err)
				return
			}
		case <-c.ping:
			if err := c.transport.Ping(); err != nil {
				log.Printf("[client] ping error uid=%s: %v", c.UID, err)
				return
			}
		}
	}
}

// readPump 已移除 —— WebSocket 专用的 readPump 位于 server_ws.go。
// gnet 的 React 回调替代了 TCP 连接的 readPump。

// writePump 已移除 —— 两种传输方式均由 WriteLoop 取代。
// WebSocket 的 ping/pong 逻辑位于 wsPingLoop(server_ws.go)。
