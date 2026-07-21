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

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second
)

// HandleWS upgrades HTTP to WebSocket and starts the client pumps.
func (s *Server) HandleWS(w http.ResponseWriter, r *http.Request) {
	// Extract JWT token from query parameter
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

	// Connection-scoped context lives for the WebSocket lifetime.
	// HTTP request context ends after the upgrade, so we use context.Background().
	connCtx := context.Background()

	transport := newWsTransport(conn)
	client := NewClient(claims.UID, claims.Username, transport, s.clients, s.connCfg.SendBufSize)
	s.clients.Register(connCtx, client)

	log.Printf("[server] client connected uid=%s username=%s", claims.UID, claims.Username)

	// Send login success response
	client.Send(&proto.Message{
		Cmd:       proto.CmdLoginResp,
		To:        claims.UID,
		Content:   claims.Username,
		Timestamp: time.Now().UnixMilli(),
	})

	// Start transport-agnostic write loop and WebSocket-specific read loop.
	go client.WriteLoop()
	go wsReadPump(connCtx, conn, client, s.router, s.connCfg)
}

// wsReadPump reads protobuf messages from a WebSocket connection.
// It enforces sender identity and delegates to the Router.
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

	// Start WebSocket ping loop (replaces writePump's ticker).
	pingDone := make(chan struct{})
	defer close(pingDone)
	go wsPingLoop(conn, time.Duration(cfg.PingPeriod), pingDone, client.closed)

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

		// Fill in sender info from the authenticated connection.
		// This is a security feature — the client cannot forge its identity.
		msg.From = client.UID
		if msg.Timestamp == 0 {
			msg.Timestamp = time.Now().UnixMilli()
		}

		router.Route(ctx, client, msg)
	}
}

// wsPingLoop sends WebSocket Ping frames periodically for keepalive.
func wsPingLoop(conn *websocket.Conn, period time.Duration, done, closed chan struct{}) {
	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-closed:
			return
		case <-ticker.C:
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Printf("[ws] ping error: %v", err)
				return
			}
		}
	}
}
