package gateway

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/im/api/proto"
	"github.com/im/internal/pkg/jwt"
	"github.com/im/internal/pkg/snowflake"
	"github.com/panjf2000/gnet/v2"
	pb "google.golang.org/protobuf/proto"
)

// =============================================================================
// mockGnetConn —— 实现 gnet.Conn，用于单元测试 GnetHandler 方法。
// =============================================================================

type mockGnetConn struct {
	ctx     any
	safeCtx any
	fd      int
	closed  bool
	mu      sync.Mutex

	// 帧解码（用于 OnTraffic 模拟）
	frameBuf []byte // 完整帧：[4 字节长度][载荷]

	// 捕获的写入（同步 Write + AsyncWrite）
	writes      [][]byte
	asyncWrites [][]byte
	asyncCbs    []gnet.AsyncCallback
}

func newMockGnetConn(fd int) *mockGnetConn {
	return &mockGnetConn{fd: fd}
}

// frameBytes 根据给定的载荷返回完整的 gnet 帧。
func frameBytes(payload []byte) []byte {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(payload)))
	return append(header, payload...)
}

// setFrame 设置 Peek/Discard/Next 将消费的帧数据。
func (m *mockGnetConn) setFrame(payload []byte) {
	m.frameBuf = frameBytes(payload)
}

// ---- Reader 接口（io.Reader、io.WriterTo、Next、Peek、Discard、InboundBuffered）----

func (m *mockGnetConn) Read(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.frameBuf) == 0 {
		return 0, io.EOF
	}
	n := copy(p, m.frameBuf)
	m.frameBuf = m.frameBuf[n:]
	return n, nil
}

func (m *mockGnetConn) WriteTo(w io.Writer) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.frameBuf) == 0 {
		return 0, io.EOF
	}
	n, err := w.Write(m.frameBuf)
	return int64(n), err
}

func (m *mockGnetConn) Next(n int) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n > len(m.frameBuf) {
		return nil, io.ErrShortBuffer
	}
	buf := make([]byte, n)
	copy(buf, m.frameBuf[:n])
	m.frameBuf = m.frameBuf[n:]
	return buf, nil
}

func (m *mockGnetConn) Peek(n int) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n > len(m.frameBuf) {
		return nil, io.ErrShortBuffer
	}
	// 返回副本（与 gnet 语义一致：Peek 的数据在 Discard 之前有效）
	buf := make([]byte, n)
	copy(buf, m.frameBuf[:n])
	return buf, nil
}

func (m *mockGnetConn) Discard(n int) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n > len(m.frameBuf) {
		n = len(m.frameBuf)
	}
	m.frameBuf = m.frameBuf[n:]
	return n, nil
}

func (m *mockGnetConn) InboundBuffered() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.frameBuf)
}

// ---- Writer 接口（io.Writer、io.ReaderFrom、SendTo、Writev、Flush、OutboundBuffered、AsyncWrite、AsyncWritev）----

func (m *mockGnetConn) Write(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	buf := make([]byte, len(p))
	copy(buf, p)
	m.writes = append(m.writes, buf)
	return len(p), nil
}

func (m *mockGnetConn) ReadFrom(r io.Reader) (int64, error) { return 0, nil }

func (m *mockGnetConn) SendTo(buf []byte, addr net.Addr) (int, error) { return 0, nil }

func (m *mockGnetConn) Writev(bs [][]byte) (int, error) {
	total := 0
	for _, b := range bs {
		n, _ := m.Write(b)
		total += n
	}
	return total, nil
}

func (m *mockGnetConn) Flush() error             { return nil }
func (m *mockGnetConn) OutboundBuffered() int     { return 0 }

func (m *mockGnetConn) AsyncWrite(buf []byte, cb gnet.AsyncCallback) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	b := make([]byte, len(buf))
	copy(b, buf)
	m.asyncWrites = append(m.asyncWrites, b)
	m.asyncCbs = append(m.asyncCbs, cb)
	return nil
}

func (m *mockGnetConn) AsyncWritev(bs [][]byte, cb gnet.AsyncCallback) error {
	for _, b := range bs {
		m.AsyncWrite(b, cb)
	}
	return nil
}

// ---- Socket 接口（Fd、Dup、SetReadBuffer、SetWriteBuffer、SetLinger、SetKeepAlivePeriod、SetKeepAlive、SetNoDelay）----

func (m *mockGnetConn) Fd() int                       { return m.fd }
func (m *mockGnetConn) Dup() (int, error)              { return m.fd + 1000, nil }
func (m *mockGnetConn) SetReadBuffer(size int) error   { return nil }
func (m *mockGnetConn) SetWriteBuffer(size int) error  { return nil }
func (m *mockGnetConn) SetLinger(secs int) error       { return nil }
func (m *mockGnetConn) SetKeepAlivePeriod(d time.Duration) error { return nil }
func (m *mockGnetConn) SetKeepAlive(enabled bool, idle, intvl time.Duration, cnt int) error {
	return nil
}
func (m *mockGnetConn) SetNoDelay(noDelay bool) error { return nil }

// ---- 连接方法 ----

func (m *mockGnetConn) Context() any     { return m.ctx }
func (m *mockGnetConn) SafeContext() any { return m.safeCtx }
func (m *mockGnetConn) SetContext(ctx any) {
	m.ctx = ctx
	m.safeCtx = ctx
}
func (m *mockGnetConn) SetSafeContext(ctx any) { m.safeCtx = ctx }

func (m *mockGnetConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 8081}
}
func (m *mockGnetConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345}
}

func (m *mockGnetConn) Wake(cb gnet.AsyncCallback) error           { return nil }
func (m *mockGnetConn) CloseWithCallback(cb gnet.AsyncCallback) error { m.closed = true; return nil }
func (m *mockGnetConn) Close() error                               { m.closed = true; return nil }
func (m *mockGnetConn) SetDeadline(t time.Time) error              { return nil }
func (m *mockGnetConn) SetReadDeadline(t time.Time) error          { return nil }
func (m *mockGnetConn) SetWriteDeadline(t time.Time) error         { return nil }

// EventLoop 返回 nil —— GnetHandler 代码路径不会调用它。
func (m *mockGnetConn) EventLoop() gnet.EventLoop { return nil }

// ---- 辅助：以 proto 消息形式捕获 AsyncWrite 载荷 ----

func (m *mockGnetConn) lastAsyncWrite() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.asyncWrites) == 0 {
		return nil
	}
	return m.asyncWrites[len(m.asyncWrites)-1]
}

// lastAsyncWritePayload 返回最后一次 AsyncWrite 的 protobuf 载荷（去掉 4 字节帧头）。
func (m *mockGnetConn) lastAsyncWritePayload() []byte {
	data := m.lastAsyncWrite()
	if len(data) < 4 {
		return nil
	}
	return data[4:]
}

func (m *mockGnetConn) asyncWriteCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.asyncWrites)
}

// =============================================================================
// 测试辅助函数
// =============================================================================

func newTestGnetHandler(t *testing.T) (*GnetHandler, *Hub, *jwt.Manager) {
	t.Helper()
	hub := NewHub(100)
	sg, _ := snowflake.New(1)
	jwtMgr := jwt.New("test-secret", time.Hour)
	router := NewRouter(hub, hub, sg, nil, DefaultRouterConfig())

	ctx := context.Background()
	handler := NewGnetHandler(
		ctx, router, hub, jwtMgr,
		256,                     // sendBufSize
		65536,                   // maxMsgSize
		200*time.Millisecond,    // heartbeatTimeout（为测试设短）
		4,                       // workerPoolSize
	)
	return handler, hub, jwtMgr
}

// jwtToken 为测试生成有效的 JWT。
func jwtToken(t *testing.T, mgr *jwt.Manager, uid, username string) string {
	t.Helper()
	token, err := mgr.Generate(uid, username, "user")
	if err != nil {
		t.Fatalf("generate token: %v", err)
	}
	return token
}

// loginFrame 创建编码为 protobuf 的 CmdLogin 帧，JWT 放在 Content 中。
func loginFrame(t *testing.T, mgr *jwt.Manager, uid, username string) []byte {
	t.Helper()
	token := jwtToken(t, mgr, uid, username)
	msg := &proto.Message{Cmd: proto.CmdLogin, Content: token}
	frame, err := pb.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal login: %v", err)
	}
	return frame
}

// =============================================================================
// WorkerPool 测试
// =============================================================================

func TestWorkerPoolSubmit(t *testing.T) {
	wp := NewWorkerPool(2)

	done := make(chan int, 1)
	wp.Submit(func() {
		done <- 42
	})

	select {
	case v := <-done:
		if v != 42 {
			t.Errorf("expected 42, got %d", v)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for task execution")
	}
}

func TestWorkerPoolMultipleTasks(t *testing.T) {
	wp := NewWorkerPool(4)

	var mu sync.Mutex
	var results []int
	done := make(chan struct{})

	for i := 0; i < 10; i++ {
		n := i
		wp.Submit(func() {
			mu.Lock()
			results = append(results, n)
			mu.Unlock()
			if len(results) == 10 {
				close(done)
			}
		})
	}

	select {
	case <-done:
		if len(results) != 10 {
			t.Errorf("expected 10 results, got %d", len(results))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for all tasks")
	}
}

func TestWorkerPoolDefaultSize(t *testing.T) {
	wp := NewWorkerPool(0) // 应默认为 4
	if wp == nil {
		t.Fatal("NewWorkerPool(0) returned nil")
	}
	// 提交一个任务以验证其正常工作。
	done := make(chan struct{})
	wp.Submit(func() { close(done) })
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for task")
	}
}

func TestWorkerPoolQueueFull(t *testing.T) {
	// 用实际缓冲区重新创建，测试非阻塞发送。
	wp2 := NewWorkerPool(1) // 任务缓冲区 = 1*256 = 256

	// 通过提交一个阻塞任务来填满队列。
	blocker := make(chan struct{})
	wp2.Submit(func() { <-blocker }) // 第一个任务阻塞 worker

	// 再提交 300 个任务 —— 最后几个应被丢弃（缓冲区约 256）。
	dropped := false
	for i := 0; i < 300; i++ {
		select {
		case wp2.tasks <- func() {}:
			// 已加入队列
		default:
			dropped = true
		}
	}
	if !dropped {
		t.Log("queue didn't overflow with 300 tasks (buffer may be larger than expected)")
	}
	close(blocker) // 解除 worker 阻塞
}

// =============================================================================
// OnOpen 测试
// =============================================================================

func TestGnetOnOpen(t *testing.T) {
	handler, _, _ := newTestGnetHandler(t)
	c := newMockGnetConn(1)

	out, action := handler.OnOpen(c)
	if action != gnet.None {
		t.Errorf("expected gnet.None, got %v", action)
	}
	if out != nil {
		t.Errorf("expected nil out, got %v", out)
	}
	if c.Context() != "pending" {
		t.Errorf("expected context 'pending', got '%v'", c.Context())
	}
}

// =============================================================================
// OnClose 测试
// =============================================================================

func TestGnetOnClosePending(t *testing.T) {
	handler, hub, _ := newTestGnetHandler(t)
	c := newMockGnetConn(1)
	c.SetContext("pending")

	action := handler.OnClose(c, nil)
	if action != gnet.None {
		t.Errorf("expected gnet.None, got %v", action)
	}
	// 挂起状态的连接不应在 hub 中。
	if hub.Count(context.Background()) != 0 {
		t.Errorf("expected 0 clients in hub, got %d", hub.Count(context.Background()))
	}
}

func TestGnetOnCloseAuthenticated(t *testing.T) {
	handler, hub, _ := newTestGnetHandler(t)
	c := newMockGnetConn(1)

	// 创建一个已认证的客户端并将其设置为 context。
	client := newTestClient(t, "alice", "Alice")
	client.transport = newGnetTransport(c)
	c.SetContext(client)
	hub.Register(context.Background(), client)
	handler.connMap.Store(c.Fd(), client)

	if hub.Count(context.Background()) != 1 {
		t.Fatalf("expected 1 client in hub, got %d", hub.Count(context.Background()))
	}

	action := handler.OnClose(c, nil)
	if action != gnet.None {
		t.Errorf("expected gnet.None, got %v", action)
	}

	// 客户端应被注销。
	if hub.Count(context.Background()) != 0 {
		t.Errorf("expected 0 clients after OnClose, got %d", hub.Count(context.Background()))
	}
	// connMap 应被清理。
	if _, ok := handler.connMap.Load(c.Fd()); ok {
		t.Error("expected connMap entry to be deleted")
	}
}

func TestGnetOnCloseNilContext(t *testing.T) {
	handler, _, _ := newTestGnetHandler(t)
	c := newMockGnetConn(1)
	// 不设置任何 context。

	action := handler.OnClose(c, nil)
	if action != gnet.None {
		t.Errorf("expected gnet.None, got %v", action)
	}
	// 不应 panic —— nil context 会被优雅处理。
}

// =============================================================================
// handleLogin 测试
// =============================================================================

func TestHandleLoginSuccess(t *testing.T) {
	handler, hub, jwtMgr := newTestGnetHandler(t)
	c := newMockGnetConn(1)
	frame := loginFrame(t, jwtMgr, "alice", "Alice")

	client, err := handler.handleLogin(frame, c)
	if err != nil {
		t.Fatalf("handleLogin failed: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
	if client.UID != "alice" {
		t.Errorf("expected UID=alice, got %s", client.UID)
	}
	if client.Username != "Alice" {
		t.Errorf("expected Username=Alice, got %s", client.Username)
	}

	// 客户端应已注册到 hub。
	if !hub.IsOnline(context.Background(), "alice") {
		t.Error("expected alice to be online")
	}

	// connMap 应存储该客户端。
	stored, ok := handler.connMap.Load(c.Fd())
	if !ok {
		t.Error("expected connMap entry")
	}
	if stored != client {
		t.Error("connMap stored wrong client")
	}

	// 登录响应应进入发送通道队列（或由 WriteLoop 刷新）。
	// handleLogin 以 goroutine 启动 WriteLoop；稍等片刻让它刷新到 AsyncWrite。
	time.Sleep(10 * time.Millisecond)
	if c.asyncWriteCount() == 0 {
		// WriteLoop 尚未运行；直接检查发送通道。
		raw := readFromChan(t, client.send)
		resp := &proto.Message{}
		if err := pb.Unmarshal(raw, resp); err != nil {
			t.Fatalf("unmarshal login response from send chan: %v", err)
		}
		if resp.Cmd != proto.CmdLoginResp {
			t.Errorf("expected CmdLoginResp, got cmd=%d", resp.Cmd)
		}
		if resp.To != "alice" {
			t.Errorf("expected To=alice, got %s", resp.To)
		}
	} else {
		// WriteLoop 已刷新 —— 检查 AsyncWrite 捕获的数据（去掉 4 字节帧头）。
		respData := c.lastAsyncWritePayload()
		if len(respData) == 0 {
			t.Fatal("AsyncWrite payload empty after stripping frame header")
		}
		resp := &proto.Message{}
		if err := pb.Unmarshal(respData, resp); err != nil {
			t.Fatalf("unmarshal login response from AsyncWrite: %v (raw=%x)", err, respData)
		}
		if resp.Cmd != proto.CmdLoginResp {
			t.Errorf("expected CmdLoginResp, got cmd=%d", resp.Cmd)
		}
		if resp.To != "alice" {
			t.Errorf("expected To=alice, got %s", resp.To)
		}
	}
}

func TestHandleLoginWrongCmd(t *testing.T) {
	handler, _, _ := newTestGnetHandler(t)
	c := newMockGnetConn(1)

	// 发送 CmdChat 而不是 CmdLogin。
	msg := &proto.Message{Cmd: proto.CmdChat, Content: "not a login"}
	frame, _ := pb.Marshal(msg)

	client, err := handler.handleLogin(frame, c)
	if err == nil {
		t.Error("expected error for wrong Cmd")
	}
	if client != nil {
		t.Error("expected nil client on error")
	}
	if !c.closed {
		// handleLogin 不直接关闭连接 —— processFrame 在出错时才会关闭。
		// 但错误信息应指明 Cmd 错误。
		t.Logf("error (expected): %v", err)
	}
}

func TestHandleLoginInvalidJWT(t *testing.T) {
	handler, _, _ := newTestGnetHandler(t)
	c := newMockGnetConn(1)

	// 使用伪造令牌的 CmdLogin。
	msg := &proto.Message{Cmd: proto.CmdLogin, Content: "not.a.valid.token"}
	frame, _ := pb.Marshal(msg)

	client, err := handler.handleLogin(frame, c)
	if err == nil {
		t.Error("expected JWT validation error")
	}
	if client != nil {
		t.Error("expected nil client on JWT error")
	}
}

func TestHandleLoginBadProtobuf(t *testing.T) {
	handler, _, _ := newTestGnetHandler(t)
	c := newMockGnetConn(1)

	// 不是有效 protobuf 的垃圾数据。
	client, err := handler.handleLogin([]byte("this is not protobuf"), c)
	if err == nil {
		t.Error("expected unmarshal error")
	}
	if client != nil {
		t.Error("expected nil client on unmarshal error")
	}
}

func TestHandleLoginEmptyFrame(t *testing.T) {
	handler, _, _ := newTestGnetHandler(t)
	c := newMockGnetConn(1)

	client, err := handler.handleLogin([]byte{}, c)
	if err == nil {
		t.Error("expected unmarshal error for empty frame")
	}
	if client != nil {
		t.Error("expected nil client for empty frame")
	}
}

// =============================================================================
// processFrame 测试
// =============================================================================

func TestProcessFrameNilContext(t *testing.T) {
	handler, _, _ := newTestGnetHandler(t)
	c := newMockGnetConn(1)
	// 不设置 context —— 应立即关闭。

	handler.processFrame([]byte("anything"), c)
	if !c.closed {
		t.Error("expected connection to be closed on nil context")
	}
}

func TestProcessFramePendingBadProtobuf(t *testing.T) {
	handler, _, _ := newTestGnetHandler(t)
	c := newMockGnetConn(1)
	c.SetContext("pending")

	handler.processFrame([]byte("garbage"), c)
	if !c.closed {
		t.Error("expected connection to be closed on bad protobuf during login")
	}
}

func TestProcessFramePendingWrongCmd(t *testing.T) {
	handler, _, _ := newTestGnetHandler(t)
	c := newMockGnetConn(1)
	c.SetContext("pending")

	// 在挂起状态的连接上发送非登录消息。
	msg := &proto.Message{Cmd: proto.CmdHeartbeat}
	frame, _ := pb.Marshal(msg)

	handler.processFrame(frame, c)
	if !c.closed {
		t.Error("expected connection to be closed on wrong Cmd during login")
	}
}

func TestProcessFramePendingInvalidJWT(t *testing.T) {
	handler, _, _ := newTestGnetHandler(t)
	c := newMockGnetConn(1)
	c.SetContext("pending")

	msg := &proto.Message{Cmd: proto.CmdLogin, Content: "bad-token"}
	frame, _ := pb.Marshal(msg)

	handler.processFrame(frame, c)
	if !c.closed {
		t.Error("expected connection to be closed on invalid JWT")
	}
}

func TestProcessFramePendingSuccess(t *testing.T) {
	handler, hub, jwtMgr := newTestGnetHandler(t)
	c := newMockGnetConn(1)
	c.SetContext("pending")

	frame := loginFrame(t, jwtMgr, "bob", "Bob")
	handler.processFrame(frame, c)

	// 连接应仍然打开。
	if c.closed {
		t.Error("connection should not be closed after successful login")
	}

	// context 现在应为 *Client，而不是 "pending"。
	client, ok := c.Context().(*Client)
	if !ok {
		t.Fatalf("expected *Client context, got %T", c.Context())
	}
	if client.UID != "bob" {
		t.Errorf("expected UID=bob, got %s", client.UID)
	}

	// 客户端应已注册。
	if !hub.IsOnline(context.Background(), "bob") {
		t.Error("bob should be online after login")
	}
}

func TestProcessFrameAuthenticatedValidMessage(t *testing.T) {
	handler, hub, _ := newTestGnetHandler(t)
	c := newMockGnetConn(2)

	// 注册一个已认证的客户端。
	client := newTestClient(t, "alice", "Alice")
	client.transport = newGnetTransport(c)
	c.SetContext(client)
	hub.Register(context.Background(), client)
	handler.connMap.Store(c.Fd(), client)

	// 发送一条心跳消息。
	msg := &proto.Message{Cmd: proto.CmdHeartbeat}
	frame, _ := pb.Marshal(msg)

	handler.processFrame(frame, c)

	// 心跳响应应到达客户端的发送通道
	// （通过 worker pool → router 分发）。
	raw := readFromChan(t, client.send)
	resp := &proto.Message{}
	if err := pb.Unmarshal(raw, resp); err != nil {
		t.Fatalf("unmarshal heartbeat response: %v", err)
	}
	if resp.Cmd != proto.CmdHeartbeat {
		t.Errorf("expected CmdHeartbeat response, got cmd=%d", resp.Cmd)
	}
}

func TestProcessFrameAuthenticatedBadProtobuf(t *testing.T) {
	handler, hub, _ := newTestGnetHandler(t)
	c := newMockGnetConn(2)

	client := newTestClient(t, "alice", "Alice")
	client.transport = newGnetTransport(c)
	c.SetContext(client)
	hub.Register(context.Background(), client)

	// 发送垃圾数据 —— 不应关闭连接，只记录日志。
	handler.processFrame([]byte("not valid protobuf"), c)
	if c.closed {
		t.Error("connection should NOT be closed on bad protobuf from authenticated client")
	}
}

func TestProcessFrameAuthenticatedOverwritesFrom(t *testing.T) {
	handler, hub, _ := newTestGnetHandler(t)
	c := newMockGnetConn(3)

	client := newTestClient(t, "alice", "Alice")
	client.transport = newGnetTransport(c)
	c.SetContext(client)
	hub.Register(context.Background(), client)

	// 在 hub 中注册目标客户端以实现在线投递。
	target := newTestClient(t, "bob", "Bob")
	hub.Register(context.Background(), target)

	// alice 发送一条自称是 "eve" 的聊天消息（伪造尝试）。
	msg := &proto.Message{
		Cmd:      proto.CmdChat,
		From:     "eve", // 伪造的发送者 —— 应被覆盖
		To:       "bob",
		ChatType: proto.ChatTypeSingle,
		MsgType:  proto.MsgTypeText,
		Content:  "I am eve",
	}
	frame, _ := pb.Marshal(msg)

	handler.processFrame(frame, c)

	// 目标收到消息 —— From 应为 "alice"，而不是 "eve"。
	raw := readFromChan(t, target.send)
	delivered := &proto.Message{}
	if err := pb.Unmarshal(raw, delivered); err != nil {
		t.Fatalf("unmarshal delivered: %v", err)
	}
	if delivered.From != "alice" {
		t.Errorf("From should be 'alice' (server-set), got '%s'", delivered.From)
	}
	if delivered.Content != "I am eve" {
		t.Errorf("Content: expected 'I am eve', got '%s'", delivered.Content)
	}
}

func TestProcessFrameInvalidContextType(t *testing.T) {
	handler, _, _ := newTestGnetHandler(t)
	c := newMockGnetConn(4)
	// 将 context 设置为无效类型（不是 "pending" 也不是 *Client）。
	c.SetContext(42)

	handler.processFrame([]byte("test"), c)
	if !c.closed {
		t.Error("expected connection to be closed on invalid context type")
	}
}

// =============================================================================
// OnTraffic 测试（帧解码）
// =============================================================================

func TestGnetOnTrafficCompleteFrame(t *testing.T) {
	handler, hub, jwtMgr := newTestGnetHandler(t)
	c := newMockGnetConn(5)
	c.SetContext("pending")

	// 在 mock 的缓冲区中设置一个完整的登录帧。
	frame := loginFrame(t, jwtMgr, "carol", "Carol")
	c.setFrame(frame)

	// OnTraffic 应解码帧并调用 processFrame。
	action := handler.OnTraffic(c)
	if action != gnet.None {
		t.Errorf("expected gnet.None, got %v", action)
	}
	// 连接不应被关闭。
	if c.closed {
		t.Error("connection should not be closed")
	}
	// carol 应已注册。
	if !hub.IsOnline(context.Background(), "carol") {
		t.Error("carol should be online after login via OnTraffic")
	}
}

func TestGnetOnTrafficFrameTooLarge(t *testing.T) {
	handler, _, _ := newTestGnetHandler(t)
	c := newMockGnetConn(6)

	// 设置一个长度 > maxFrameSize（65536）的帧。
	payload := make([]byte, handler.maxFrameSize+1)
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(payload)))
	c.frameBuf = append(header, payload...)

	action := handler.OnTraffic(c)
	if action != gnet.Close {
		t.Errorf("expected gnet.Close for oversized frame, got %v", action)
	}
}

func TestGnetOnTrafficIncompleteHeader(t *testing.T) {
	handler, _, _ := newTestGnetHandler(t)
	c := newMockGnetConn(7)

	// 少于 4 字节 —— 不完整的头。
	c.frameBuf = []byte{0x00, 0x01}

	action := handler.OnTraffic(c)
	if action != gnet.None {
		t.Errorf("expected gnet.None for incomplete header, got %v", action)
	}
}

func TestGnetOnTrafficIncompletePayload(t *testing.T) {
	handler, _, _ := newTestGnetHandler(t)
	c := newMockGnetConn(8)

	// 头部声明 100 字节，但后面只有 10 字节。
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, 100)
	c.frameBuf = append(header, make([]byte, 10)...)

	action := handler.OnTraffic(c)
	// 应等待更多数据（载荷不完整）。
	if action != gnet.None {
		t.Errorf("expected gnet.None for incomplete payload, got %v", action)
	}
}

func TestGnetOnTrafficMultipleFrames(t *testing.T) {
	handler, hub, jwtMgr := newTestGnetHandler(t)
	c := newMockGnetConn(9)
	c.SetContext("pending")

	// 连续设置两个登录帧。第一个完成登录，第二个被忽略
	// （已经认证，但在已认证的连接上收到 CmdLogin ——
	// 由于 ctx 是 *Client 而不是 "pending"，processFrame 不会走登录流程）。
	// 只测试第一个帧被正确消费。
	frame := loginFrame(t, jwtMgr, "dave", "Dave")
	// 在缓冲区中放入两个帧。
	c.frameBuf = append(frameBytes(frame), frameBytes(frame)...)

	action := handler.OnTraffic(c)
	if action != gnet.None {
		t.Errorf("expected gnet.None, got %v", action)
	}
	if !hub.IsOnline(context.Background(), "dave") {
		t.Error("dave should be online")
	}
}

// =============================================================================
// 心跳检查器测试
// =============================================================================

func TestHeartbeatCheckerKicksStaleConnections(t *testing.T) {
	hub := NewHub(100)
	sg, _ := snowflake.New(1)
	jwtMgr := jwt.New("test-secret", time.Hour)
	router := NewRouter(hub, hub, sg, nil, DefaultRouterConfig())

	ctx := context.Background()
	// 创建心跳超时极短的 handler。
	handler := NewGnetHandler(
		ctx, router, hub, jwtMgr,
		256,                  // sendBufSize
		65536,                // maxMsgSize
		100*time.Millisecond, // heartbeatTimeout —— 超过此时长的连接会被踢下线
		4,                    // workerPoolSize
	)

	c := newMockGnetConn(10)
	client := newTestClient(t, "eve", "Eve")
	client.transport = newGnetTransport(c)
	c.SetContext(client)
	hub.Register(context.Background(), client)
	handler.connMap.Store(c.Fd(), client)

	// 将心跳时间设置为很久以前。
	client.SetHeartbeat(time.Now().Add(-time.Hour))

	// 检查器以 heartbeatTimeout/2 = 50ms 的间隔运行。
	// 等待检查器检测到过期的连接。
	time.Sleep(300 * time.Millisecond)

	// 客户端应被注销。
	if hub.IsOnline(context.Background(), "eve") {
		t.Error("eve should have been kicked by heartbeat checker")
	}
	// connMap 应被清理。
	if _, ok := handler.connMap.Load(c.Fd()); ok {
		t.Error("connMap entry should be deleted after heartbeat timeout")
	}
}

func TestHeartbeatCheckerKeepsActiveConnections(t *testing.T) {
	hub := NewHub(100)
	sg, _ := snowflake.New(1)
	jwtMgr := jwt.New("test-secret", time.Hour)
	router := NewRouter(hub, hub, sg, nil, DefaultRouterConfig())

	ctx := context.Background()
	handler := NewGnetHandler(
		ctx, router, hub, jwtMgr,
		256,                  // sendBufSize
		65536,                // maxMsgSize
		10*time.Second,       // heartbeatTimeout —— 很长，不会触发
		4,                    // workerPoolSize
	)

	c := newMockGnetConn(11)
	client := newTestClient(t, "frank", "Frank")
	client.transport = newGnetTransport(c)
	c.SetContext(client)
	hub.Register(context.Background(), client)
	handler.connMap.Store(c.Fd(), client)

	// 心跳是当前的（由 NewClient 设置）。
	time.Sleep(100 * time.Millisecond)

	// 客户端应仍然在线。
	if !hub.IsOnline(context.Background(), "frank") {
		t.Error("frank should still be online (heartbeat is recent)")
	}
	// connMap 条目应仍然存在。
	if _, ok := handler.connMap.Load(c.Fd()); !ok {
		t.Error("connMap entry should still exist")
	}
}

// =============================================================================
// Transport 测试
// =============================================================================

func TestGnetTransportClose(t *testing.T) {
	c := newMockGnetConn(1)
	tr := newGnetTransport(c)

	if err := tr.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if !c.closed {
		t.Error("mock conn should be closed")
	}
}

func TestGnetTransportWrite(t *testing.T) {
	c := newMockGnetConn(1)
	tr := newGnetTransport(c)

	data := []byte("hello gnet")
	if err := tr.Write(data); err != nil {
		t.Errorf("Write: %v", err)
	}
	if c.asyncWriteCount() != 1 {
		t.Fatalf("expected 1 AsyncWrite, got %d", c.asyncWriteCount())
	}
}

// =============================================================================
// 编译期接口检查
// =============================================================================

var _ gnet.Conn = (*mockGnetConn)(nil)
