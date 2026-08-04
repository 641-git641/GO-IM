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

// frameMaxSize 是最大 protobuf 帧负载大小。
const frameHeaderSize = 4

// WorkerPool 是一个简单的 goroutine 池,用于分担 Router.Route 调用。
type WorkerPool struct {
	tasks     chan func()
	done      chan struct{} // 由 Close() 关闭
	closeOnce sync.Once
	closed    atomic.Bool
}

// NewWorkerPool 创建一个 goroutine 池。如果 size <= 0,使用默认值。
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

// Submit 将任务入队。非阻塞;队列已满或池已关闭时丢弃。
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

// Close 停止工作池,返回前排空正在执行的任务。
// 可安全地多次调用 —— 内部使用 sync.Once。
func (wp *WorkerPool) Close() {
	wp.closeOnce.Do(func() {
		wp.closed.Store(true)
		close(wp.done)
		close(wp.tasks)
	})
}

// GnetHandler 为 gnet TCP 服务器实现 gnet.EventHandler。
// 帧解析在 OnTraffic 中通过 Peek/Discard/Next 手动完成。
type GnetHandler struct {
	gnet.BuiltinEventEngine

	ctx              context.Context
	cancel           context.CancelFunc // 取消心跳检查器
	router           *Router
	clients          ClientRegistry
	jwtMgr           *jwt.Manager
	sendBufSize      int
	maxFrameSize     uint32
	heartbeatTimeout time.Duration
	workerPool       *WorkerPool

	engine    gnet.Engine // 由 OnBoot 设置,用于优雅关闭
	engineSet bool
	connMap   sync.Map    // fd(int) → *Client 连接映射
}

// NewGnetHandler 创建一个 GnetHandler。
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

	// 用可取消的 context 启动心跳检查器。
	h.startHeartbeatChecker(hbCtx, heartbeatTimeout/2)

	return h
}

// OnOpen 在新 TCP 连接建立时被调用。
func (h *GnetHandler) OnOpen(c gnet.Conn) ([]byte, gnet.Action) {
	// 存储待认证标记 —— 认证在第一条消息时进行。
	c.SetContext("pending")
	return nil, gnet.None
}

// OnClose 在连接关闭时被调用。
func (h *GnetHandler) OnClose(c gnet.Conn, _ error) (action gnet.Action) {
	ctxVal := c.Context()
	if ctxVal == nil {
		return gnet.None
	}
	// 仅当该连接已通过认证时才进行清理。
	if client, ok := ctxVal.(*Client); ok {
		h.clients.Unregister(h.ctx, client)
		client.Close()
		h.connMap.Delete(c.Fd())
		log.Printf("[gnet] client disconnected uid=%s fd=%d", client.UID, c.Fd())
	}
	return gnet.None
}

// OnTraffic 在数据到达时被调用。它手动实现帧解码,
// 因为 gnet v2 没有 v1 中的 ICodec 接口。
func (h *GnetHandler) OnTraffic(c gnet.Conn) gnet.Action {
	for {
		// 查看 4 字节的长度头。
		header, err := c.Peek(frameHeaderSize)
		if err == io.ErrShortBuffer {
			// 数据还不够 —— 等待更多数据。
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
		// 查看完整帧。
		if _, err := c.Peek(totalLen); err == io.ErrShortBuffer {
			return gnet.None
		}
		if err != nil {
			return gnet.Close
		}

		// 丢弃头部。
		c.Discard(frameHeaderSize)
		// 读取负载。
		payload, err := c.Next(int(length))
		if err != nil {
			log.Printf("[gnet] read payload error fd=%d: %v", c.Fd(), err)
			return gnet.Close
		}

		// 处理帧(绝不能阻塞事件循环)。
		h.processFrame(payload, c)
	}
}

// processFrame 处理来自 gnet 连接的完整 protobuf 帧。
func (h *GnetHandler) processFrame(frame []byte, c gnet.Conn) {
	ctxVal := c.Context()
	if ctxVal == nil {
		c.Close()
		return
	}

	// 检查是否为待认证(未认证)连接。
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

	// 解析 protobuf。
	msg := &proto.Message{}
	if err := pb.Unmarshal(frame, msg); err != nil {
		log.Printf("[gnet] unmarshal error uid=%s fd=%d: %v", client.UID, c.Fd(), err)
		return
	}

	// 更新心跳时间戳。
	client.SetHeartbeat(time.Now())

	// 安全:用已认证的 UID 覆盖 From 字段。
	msg.From = client.UID
	if msg.Timestamp == 0 {
		msg.Timestamp = time.Now().UnixMilli()
	}

	// 卸载到工作池 —— 绝不要阻塞事件循环。
	router := h.router
	ctx := h.ctx
	h.workerPool.Submit(func() {
		router.Route(ctx, client, msg)
	})
}

// handleLogin 处理 gnet 客户端的第一条消息。
// 第一条消息必须是 CmdLogin,且 JWT 令牌位于 Content 字段中。
func (h *GnetHandler) handleLogin(frame []byte, c gnet.Conn) (*Client, error) {
	msg := &proto.Message{}
	if err := pb.Unmarshal(frame, msg); err != nil {
		return nil, fmt.Errorf("unmarshal login: %w", err)
	}
	if msg.Cmd != proto.CmdLogin {
		return nil, fmt.Errorf("expected CmdLogin (%d), got cmd=%d", proto.CmdLogin, msg.Cmd)
	}

	// JWT 令牌位于 Content 字段中。
	claims, err := h.jwtMgr.Validate(msg.Content)
	if err != nil {
		return nil, fmt.Errorf("jwt: %w", err)
	}

	transport := newGnetTransport(c)
	client := NewClient(claims.UID, claims.Username, transport, h.clients, h.sendBufSize)
	client.SetHeartbeat(time.Now())

	// 注册到 Hub(可能踢掉旧连接)。
	h.clients.Register(h.ctx, client)
	h.connMap.Store(c.Fd(), client)

	// 发送登录响应。
	client.Send(&proto.Message{
		Cmd:       proto.CmdLoginResp,
		To:        claims.UID,
		Content:   claims.Username,
		Timestamp: time.Now().UnixMilli(),
	})

	// 启动写循环。
	go client.WriteLoop()

	log.Printf("[gnet] client connected uid=%s username=%s fd=%d", claims.UID, claims.Username, c.Fd())
	return client, nil
}

// OnBoot 在 gnet 引擎启动时被调用。我们保存引擎引用,
// 以便在优雅关闭时调用 eng.Stop()。
func (h *GnetHandler) OnBoot(eng gnet.Engine) gnet.Action {
	h.engine = eng
	h.engineSet = true
	return gnet.None
}

// Shutdown 优雅地停止 gnet 服务器:取消心跳检查器,
// 停止 gnet 引擎(不再有事件到达),然后关闭工作池。
func (h *GnetHandler) Shutdown(ctx context.Context) error {
	h.cancel() // 停止心跳检查器

	// 先停止引擎,确保不再有事件派发到工作池。
	if h.engineSet {
		if err := h.engine.Stop(ctx); err != nil {
			return err
		}
	}

	// 现在可以安全关闭工作池 —— 不再会有 Submit 调用到达。
	h.workerPool.Close()
	return nil
}

// startHeartbeatChecker 定期扫描连接并剔除失效连接。
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
