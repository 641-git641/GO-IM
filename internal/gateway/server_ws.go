package gateway

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/im/api/proto"
	"github.com/im/configs"
	pb "google.golang.org/protobuf/proto"
)

// HandleWS 将 HTTP 升级为 WebSocket 并启动客户端读写循环。
func (s *Server) HandleWS(w http.ResponseWriter, r *http.Request) {
	// 从查询参数中提取 JWT 令牌
	tokenStr := r.URL.Query().Get("token")
	if tokenStr == "" {
		http.Error(w, "missing token", http.StatusUnauthorized)
		return
	}

	claims, err := s.jwtMgr.Validate(tokenStr)
	if err != nil {
		http.Error(w, "invalid token: "+err.Error(), http.StatusUnauthorized)
		return
	}

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[server] upgrade error: %v", err)
		return
	}

	// 连接级 context 的生命周期与 WebSocket 相同。
	// HTTP 请求 context 在升级后即结束,因此使用 context.Background()。
	connCtx := context.Background()

	transport := newWsTransport(conn)
	client := NewClient(claims.UID, claims.Username, transport, s.clients, s.connCfg.SendBufSize)
	s.clients.Register(connCtx, client)

	log.Printf("[server] client connected uid=%s username=%s", claims.UID, claims.Username)

	// 发送登录成功响应
	client.Send(&proto.Message{
		Cmd:       proto.CmdLoginResp,
		To:        claims.UID,
		Content:   claims.Username,
		Timestamp: time.Now().UnixMilli(),
	})

	// 启动与传输无关的写循环和 WebSocket 专用的读循环。
	go client.WriteLoop()
	go wsReadPump(connCtx, conn, client, s.router, s.connCfg)
}

// wsReadPump 从 WebSocket 连接读取 protobuf 消息。
// 它强制发送方身份并委托给 Router 处理。
func wsReadPump(ctx context.Context, conn *websocket.Conn, client *Client, router *Router, cfg configs.GatewayConnConfig) {
	defer func() {
		client.clients.Unregister(ctx, client)
		client.Close()
	}()

	pongWait := time.Duration(cfg.PongWait)
	conn.SetReadLimit(cfg.MaxMsgSize)
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		client.SetHeartbeat(time.Now())
		return nil
	})

	// 启动 WebSocket ping 循环(取代 writePump 的定时器)。
	// ping 帧经由 client.SendPing → WriteLoop 串行写入,不与数据帧并发写 conn。
	pingDone := make(chan struct{})
	defer close(pingDone)
	go wsPingLoop(client, time.Duration(cfg.PingPeriod), pingDone, client.closed)

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Printf("[ws] read error uid=%s: %v", client.UID, err)
			}
			return
		}

		client.SetHeartbeat(time.Now())

		msg := &proto.Message{}
		if err := pb.Unmarshal(raw, msg); err != nil {
			log.Printf("[ws] unmarshal error uid=%s: %v", client.UID, err)
			continue
		}

		// 从已认证的连接中填充发送方信息。
		// 这是安全特性 —— 客户端无法伪造其身份。
		msg.From = client.UID
		if msg.Timestamp == 0 {
			msg.Timestamp = time.Now().UnixMilli()
		}

		router.Route(ctx, client, msg)
	}
}

// wsPingLoop 定期请求发送 WebSocket Ping 帧以保活。
// 实际的 Ping 帧由 WriteLoop 串行写入(通过 client.SendPing),
// 保证写者唯一 —— 直接写 conn 会与 WriteLoop 并发写而 panic。
func wsPingLoop(client *Client, period time.Duration, done, closed chan struct{}) {
	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-closed:
			return
		case <-ticker.C:
			client.SendPing()
		}
	}
}
