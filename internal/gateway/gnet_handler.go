package gateway

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/im/api/proto"
	"github.com/im/internal/pkg/jwt"
	"github.com/panjf2000/gnet/v2"
	pb "google.golang.org/protobuf/proto"
)

// frameMaxSize is the maximum protobuf frame payload size.
const frameHeaderSize = 4

// WorkerPool is a simple goroutine pool for offloading Router.Route calls.
type WorkerPool struct {
	tasks     chan func()
	done      chan struct{} // closed by Close()
	closeOnce sync.Once
	closed    atomic.Bool
}

// NewWorkerPool creates a goroutine pool. If size <= 0, uses a default.
func NewWorkerPool(size int) *WorkerPool {
	if size <= 0 {
		size = 4
	}
	wp := &WorkerPool{
		tasks: make(chan func(), size*256),
		done:  make(chan struct{}),
	}
	for i := 0; i < size; i++ {
		go func() {
			for task := range wp.tasks {
				task()
			}
		}()
	}
	return wp
}

// Submit enqueues a task. Non-blocking; drops if the queue is full or pool is closed.
func (wp *WorkerPool) Submit(task func()) {
	if wp.closed.Load() {
		return
	}
	select {
	case wp.tasks <- task:
	default:
		log.Printf("[gnet] worker pool full, dropping message")
	}
}

// Close stops the worker pool, draining in-flight tasks before returning.
// Safe to call multiple times — uses sync.Once.
func (wp *WorkerPool) Close() {
	wp.closeOnce.Do(func() {
		wp.closed.Store(true)
		close(wp.done)
		close(wp.tasks)
	})
}

// GnetHandler implements gnet.EventHandler for the gnet TCP server.
// Framing is done manually in OnTraffic via Peek/Discard/Next.
type GnetHandler struct {
	gnet.BuiltinEventEngine

	ctx              context.Context
	cancel           context.CancelFunc // cancels heartbeat checker
	router           *Router
	clients          ClientRegistry
	jwtMgr           *jwt.Manager
	sendBufSize      int
	maxFrameSize     uint32
	heartbeatTimeout time.Duration
	workerPool       *WorkerPool

	engine    gnet.Engine // set by OnBoot, used for graceful shutdown
	engineSet bool
	connMap   sync.Map    // fd(int) → *Client
}

// NewGnetHandler creates a GnetHandler.
func NewGnetHandler(
	ctx context.Context,
	router *Router,
	clients ClientRegistry,
	jwtMgr *jwt.Manager,
	sendBufSize int,
	maxMsgSize int64,
	heartbeatTimeout time.Duration,
	workerPoolSize int,
) *GnetHandler {
	hbCtx, hbCancel := context.WithCancel(ctx)
	h := &GnetHandler{
		ctx:              ctx,
		cancel:           hbCancel,
		router:           router,
		clients:          clients,
		jwtMgr:           jwtMgr,
		sendBufSize:      sendBufSize,
		maxFrameSize:     uint32(maxMsgSize),
		heartbeatTimeout: heartbeatTimeout,
		workerPool:       NewWorkerPool(workerPoolSize),
	}

	// Start heartbeat checker with a cancelable context.
	h.startHeartbeatChecker(hbCtx, heartbeatTimeout/2)

	return h
}

// OnOpen is called when a new TCP connection is established.
func (h *GnetHandler) OnOpen(c gnet.Conn) ([]byte, gnet.Action) {
	// Store a pending marker — authentication happens on the first message.
	c.SetContext("pending")
	return nil, gnet.None
}

// OnClose is called when a connection is closed.
func (h *GnetHandler) OnClose(c gnet.Conn, _ error) (action gnet.Action) {
	ctxVal := c.Context()
	if ctxVal == nil {
		return gnet.None
	}
	// Only clean up if this connection was authenticated.
	if client, ok := ctxVal.(*Client); ok {
		h.clients.Unregister(h.ctx, client)
		client.Close()
		h.connMap.Delete(c.Fd())
		log.Printf("[gnet] client disconnected uid=%s fd=%d", client.UID, c.Fd())
	}
	return gnet.None
}

// OnTraffic is called when data arrives. It implements frame decoding manually
// because gnet v2 does not have the ICodec interface from v1.
func (h *GnetHandler) OnTraffic(c gnet.Conn) gnet.Action {
	for {
		// Peek at the 4-byte length header.
		header, err := c.Peek(frameHeaderSize)
		if err == io.ErrShortBuffer {
			// Not enough data yet — wait for more.
			return gnet.None
		}
		if err != nil {
			return gnet.Close
		}

		length := binary.BigEndian.Uint32(header)
		if length > h.maxFrameSize {
			log.Printf("[gnet] frame too large: %d > %d fd=%d", length, h.maxFrameSize, c.Fd())
			return gnet.Close
		}

		totalLen := frameHeaderSize + int(length)
		// Peek at the complete frame.
		if _, err := c.Peek(totalLen); err == io.ErrShortBuffer {
			return gnet.None
		}
		if err != nil {
			return gnet.Close
		}

		// Discard the header.
		c.Discard(frameHeaderSize)
		// Read the payload.
		payload, err := c.Next(int(length))
		if err != nil {
			log.Printf("[gnet] read payload error fd=%d: %v", c.Fd(), err)
			return gnet.Close
		}

		// Process the frame (must not block the event loop).
		h.processFrame(payload, c)
	}
}

// processFrame handles a complete protobuf frame from a gnet connection.
func (h *GnetHandler) processFrame(frame []byte, c gnet.Conn) {
	ctxVal := c.Context()
	if ctxVal == nil {
		c.Close()
		return
	}

	// Check if this is a pending (unauthenticated) connection.
	if str, ok := ctxVal.(string); ok && str == "pending" {
		client, err := h.handleLogin(frame, c)
		if err != nil {
			log.Printf("[gnet] login failed fd=%d: %v", c.Fd(), err)
			c.Close()
			return
		}
		c.SetContext(client)
		return
	}

	client, ok := ctxVal.(*Client)
	if !ok {
		c.Close()
		return
	}

	// Parse protobuf.
	msg := &proto.Message{}
	if err := pb.Unmarshal(frame, msg); err != nil {
		log.Printf("[gnet] unmarshal error uid=%s fd=%d: %v", client.UID, c.Fd(), err)
		return
	}

	// Update heartbeat timestamp.
	client.SetHeartbeat(time.Now())

	// Security: overwrite From with authenticated UID.
	msg.From = client.UID
	if msg.Timestamp == 0 {
		msg.Timestamp = time.Now().UnixMilli()
	}

	// Offload to worker pool — do NOT block the event loop.
	router := h.router
	ctx := h.ctx
	h.workerPool.Submit(func() {
		router.Route(ctx, client, msg)
	})
}

// handleLogin processes the first message from a gnet client.
// The first message must be CmdLogin with the JWT token in the Content field.
func (h *GnetHandler) handleLogin(frame []byte, c gnet.Conn) (*Client, error) {
	msg := &proto.Message{}
	if err := pb.Unmarshal(frame, msg); err != nil {
		return nil, fmt.Errorf("unmarshal login: %w", err)
	}
	if msg.Cmd != proto.CmdLogin {
		return nil, fmt.Errorf("expected CmdLogin (%d), got cmd=%d", proto.CmdLogin, msg.Cmd)
	}

	// JWT token is in the Content field.
	claims, err := h.jwtMgr.Validate(msg.Content)
	if err != nil {
		return nil, fmt.Errorf("jwt: %w", err)
	}

	transport := newGnetTransport(c)
	client := NewClient(claims.UID, claims.Username, transport, h.clients, h.sendBufSize)
	client.SetHeartbeat(time.Now())

	// Register with Hub (may kick old connection).
	h.clients.Register(h.ctx, client)
	h.connMap.Store(c.Fd(), client)

	// Send login response.
	client.Send(&proto.Message{
		Cmd:       proto.CmdLoginResp,
		To:        claims.UID,
		Content:   claims.Username,
		Timestamp: time.Now().UnixMilli(),
	})

	// Start write loop.
	go client.WriteLoop()

	log.Printf("[gnet] client connected uid=%s username=%s fd=%d", claims.UID, claims.Username, c.Fd())
	return client, nil
}

// OnBoot is called when the gnet engine starts. We save the engine reference
// so we can call eng.Stop() during graceful shutdown.
func (h *GnetHandler) OnBoot(eng gnet.Engine) gnet.Action {
	h.engine = eng
	h.engineSet = true
	return gnet.None
}

// Shutdown gracefully stops the gnet server: cancels heartbeat checker,
// stops the gnet engine (so no more events arrive), then closes the worker pool.
func (h *GnetHandler) Shutdown(ctx context.Context) error {
	h.cancel() // stop heartbeat checker

	// Stop the engine first so no more events are dispatched to the worker pool.
	if h.engineSet {
		if err := h.engine.Stop(ctx); err != nil {
			return err
		}
	}

	// Now safe to close worker pool — no more Submit calls will arrive.
	h.workerPool.Close()
	return nil
}

// startHeartbeatChecker periodically scans connections and kicks dead ones.
func (h *GnetHandler) startHeartbeatChecker(ctx context.Context, checkInterval time.Duration) {
	if checkInterval <= 0 {
		checkInterval = 45 * time.Second
	}
	go func() {
		ticker := time.NewTicker(checkInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				now := time.Now()
				h.connMap.Range(func(key, value any) bool {
					client := value.(*Client)
					if now.Sub(client.Heartbeat()) > h.heartbeatTimeout {
						log.Printf("[gnet] heartbeat timeout uid=%s", client.UID)
						h.clients.Unregister(h.ctx, client)
						client.Close()
						h.connMap.Delete(key)
					}
					return true
				})
			}
		}
	}()
}
