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

// Sentinel errors returned by Client.Send.
var (
	ErrSendBufferFull = errors.New("send buffer full")
	ErrClientClosed   = errors.New("client closed")
)

// Client represents a single connection (WebSocket or raw TCP via gnet).
type Client struct {
	UID      string
	Username string

	transport Transport
	clients   ClientRegistry
	send      chan []byte // buffered channel of outbound messages
	closed    chan struct{}
	closeOnce sync.Once

	// Application-level heartbeat tracking (used by both transports).
	lastHeartbeat time.Time
	heartbeatMu   sync.Mutex
}

// NewClient creates a Client with a Transport and registers it.
func NewClient(uid, username string, transport Transport, clients ClientRegistry, sendBufSize int) *Client {
	return &Client{
		UID:           uid,
		Username:      username,
		transport:     transport,
		clients:       clients,
		send:          make(chan []byte, sendBufSize),
		closed:        make(chan struct{}),
		lastHeartbeat: time.Now(),
	}
}

// Heartbeat returns the last application-level heartbeat time.
func (c *Client) Heartbeat() time.Time {
	c.heartbeatMu.Lock()
	defer c.heartbeatMu.Unlock()
	return c.lastHeartbeat
}

// SetHeartbeat updates the heartbeat timestamp to now.
func (c *Client) SetHeartbeat(t time.Time) {
	c.heartbeatMu.Lock()
	defer c.heartbeatMu.Unlock()
	c.lastHeartbeat = t
}

// Send pushes a serialized protobuf message to the outbound channel.
// Returns an error if the buffer is full or the client has closed.
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

// Close shuts down the client connection and signals the write loop.
func (c *Client) Close() {
	c.closeOnce.Do(func() {
		close(c.closed)
		c.transport.Close()
	})
}

// WriteLoop drains the send channel and writes to the transport.
// This goroutine replaces the old writePump — it is transport-agnostic.
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
		}
	}
}

// readPump is removed — WebSocket-specific readPump lives in server_ws.go.
// gnet's React callback replaces readPump for TCP connections.

// writePump is removed — WriteLoop replaces it for both transports.
// WebSocket ping/pong logic lives in wsPingLoop (server_ws.go).
