package gateway

import (
	"time"

	"github.com/gorilla/websocket"
)

// wsTransport wraps a gorilla/websocket.Conn as a Transport.
type wsTransport struct {
	conn         *websocket.Conn
	writeTimeout time.Duration
}

// newWsTransport creates a WebSocket Transport.
func newWsTransport(conn *websocket.Conn) *wsTransport {
	return &wsTransport{conn: conn, writeTimeout: 10 * time.Second}
}

func (t *wsTransport) Close() error {
	return t.conn.Close()
}

func (t *wsTransport) Write(p []byte) error {
	if t.writeTimeout > 0 {
		t.conn.SetWriteDeadline(time.Now().Add(t.writeTimeout))
	}
	return t.conn.WriteMessage(websocket.BinaryMessage, p)
}
