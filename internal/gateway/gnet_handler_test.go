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
// mockGnetConn — implements gnet.Conn for unit testing GnetHandler methods.
// =============================================================================

type mockGnetConn struct {
	ctx     any
	safeCtx any
	fd      int
	closed  bool
	mu      sync.Mutex

	// Frame decoding (for OnTraffic simulation)
	frameBuf []byte // complete frame: [4-byte len][payload]

	// Captured writes (synchronous Write + AsyncWrite)
	writes      [][]byte
	asyncWrites [][]byte
	asyncCbs    []gnet.AsyncCallback
}

func newMockGnetConn(fd int) *mockGnetConn {
	return &mockGnetConn{fd: fd}
}

// frameBytes returns a complete gnet frame from the given payload.
func frameBytes(payload []byte) []byte {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(payload)))
	return append(header, payload...)
}

// setFrame sets the frame data that Peek/Discard/Next will consume.
func (m *mockGnetConn) setFrame(payload []byte) {
	m.frameBuf = frameBytes(payload)
}

// ---- Reader interface (io.Reader, io.WriterTo, Next, Peek, Discard, InboundBuffered) ----

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
	// Return a copy (matches gnet semantics: Peek data is valid until Discard)
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

// ---- Writer interface (io.Writer, io.ReaderFrom, SendTo, Writev, Flush, OutboundBuffered, AsyncWrite, AsyncWritev) ----

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

// ---- Socket interface (Fd, Dup, SetReadBuffer, SetWriteBuffer, SetLinger, SetKeepAlivePeriod, SetKeepAlive, SetNoDelay) ----

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

// ---- Connection methods ----

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

// EventLoop returns nil — not called by GnetHandler code paths.
func (m *mockGnetConn) EventLoop() gnet.EventLoop { return nil }

// ---- Helper: capture AsyncWrite payloads as proto messages ----

func (m *mockGnetConn) lastAsyncWrite() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.asyncWrites) == 0 {
		return nil
	}
	return m.asyncWrites[len(m.asyncWrites)-1]
}

// lastAsyncWritePayload returns the last AsyncWrite's protobuf payload (strips 4-byte frame header).
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
// Test helpers
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
		200*time.Millisecond,    // heartbeatTimeout (short for tests)
		4,                       // workerPoolSize
	)
	return handler, hub, jwtMgr
}

// jwtToken generates a valid JWT for testing.
func jwtToken(t *testing.T, mgr *jwt.Manager, uid, username string) string {
	t.Helper()
	token, err := mgr.Generate(uid, username, "user")
	if err != nil {
		t.Fatalf("generate token: %v", err)
	}
	return token
}

// loginFrame creates a protobuf-encoded CmdLogin frame with the JWT in Content.
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
// WorkerPool tests
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
	wp := NewWorkerPool(0) // should default to 4
	if wp == nil {
		t.Fatal("NewWorkerPool(0) returned nil")
	}
	// Submit a task to verify it works.
	done := make(chan struct{})
	wp.Submit(func() { close(done) })
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for task")
	}
}

func TestWorkerPoolQueueFull(t *testing.T) {
	// Recreate with actual buffer to test the non-blocking send.
	wp2 := NewWorkerPool(1) // tasks buffer = 1*256 = 256

	// Fill the queue by submitting a blocking task.
	blocker := make(chan struct{})
	wp2.Submit(func() { <-blocker }) // first task blocks the worker

	// Submit 300 more tasks — the last few should be dropped (buffer ~256).
	dropped := false
	for i := 0; i < 300; i++ {
		select {
		case wp2.tasks <- func() {}:
			// added to queue
		default:
			dropped = true
		}
	}
	if !dropped {
		t.Log("queue didn't overflow with 300 tasks (buffer may be larger than expected)")
	}
	close(blocker) // unblock the worker
}

// =============================================================================
// OnOpen tests
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
// OnClose tests
// =============================================================================

func TestGnetOnClosePending(t *testing.T) {
	handler, hub, _ := newTestGnetHandler(t)
	c := newMockGnetConn(1)
	c.SetContext("pending")

	action := handler.OnClose(c, nil)
	if action != gnet.None {
		t.Errorf("expected gnet.None, got %v", action)
	}
	// Pending connection should NOT be in hub.
	if hub.Count(context.Background()) != 0 {
		t.Errorf("expected 0 clients in hub, got %d", hub.Count(context.Background()))
	}
}

func TestGnetOnCloseAuthenticated(t *testing.T) {
	handler, hub, _ := newTestGnetHandler(t)
	c := newMockGnetConn(1)

	// Create an authenticated client and set it as context.
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

	// Client should be unregistered.
	if hub.Count(context.Background()) != 0 {
		t.Errorf("expected 0 clients after OnClose, got %d", hub.Count(context.Background()))
	}
	// connMap should be cleaned.
	if _, ok := handler.connMap.Load(c.Fd()); ok {
		t.Error("expected connMap entry to be deleted")
	}
}

func TestGnetOnCloseNilContext(t *testing.T) {
	handler, _, _ := newTestGnetHandler(t)
	c := newMockGnetConn(1)
	// Don't set any context.

	action := handler.OnClose(c, nil)
	if action != gnet.None {
		t.Errorf("expected gnet.None, got %v", action)
	}
	// Should not panic — nil context is handled gracefully.
}

// =============================================================================
// handleLogin tests
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

	// Client should be registered in hub.
	if !hub.IsOnline(context.Background(), "alice") {
		t.Error("expected alice to be online")
	}

	// connMap should store the client.
	stored, ok := handler.connMap.Load(c.Fd())
	if !ok {
		t.Error("expected connMap entry")
	}
	if stored != client {
		t.Error("connMap stored wrong client")
	}

	// Login response should be queued on the send channel (or flushed via WriteLoop).
	// handleLogin starts WriteLoop as a goroutine; give it a moment to flush to AsyncWrite.
	time.Sleep(10 * time.Millisecond)
	if c.asyncWriteCount() == 0 {
		// WriteLoop hasn't run yet; check the send channel directly.
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
		// WriteLoop flushed it — check AsyncWrite captured data (strip 4-byte frame header).
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

	// Send CmdChat instead of CmdLogin.
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
		// handleLogin doesn't close the conn directly — processFrame does on error.
		// But the error message should indicate wrong Cmd.
		t.Logf("error (expected): %v", err)
	}
}

func TestHandleLoginInvalidJWT(t *testing.T) {
	handler, _, _ := newTestGnetHandler(t)
	c := newMockGnetConn(1)

	// CmdLogin with a fake token.
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

	// Garbage data that is not valid protobuf.
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
// processFrame tests
// =============================================================================

func TestProcessFrameNilContext(t *testing.T) {
	handler, _, _ := newTestGnetHandler(t)
	c := newMockGnetConn(1)
	// Don't set context — should close immediately.

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

	// Send a non-login message on a pending connection.
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

	// Connection should still be open.
	if c.closed {
		t.Error("connection should not be closed after successful login")
	}

	// Context should now be a *Client, not "pending".
	client, ok := c.Context().(*Client)
	if !ok {
		t.Fatalf("expected *Client context, got %T", c.Context())
	}
	if client.UID != "bob" {
		t.Errorf("expected UID=bob, got %s", client.UID)
	}

	// Client should be registered.
	if !hub.IsOnline(context.Background(), "bob") {
		t.Error("bob should be online after login")
	}
}

func TestProcessFrameAuthenticatedValidMessage(t *testing.T) {
	handler, hub, _ := newTestGnetHandler(t)
	c := newMockGnetConn(2)

	// Register an already-authenticated client.
	client := newTestClient(t, "alice", "Alice")
	client.transport = newGnetTransport(c)
	c.SetContext(client)
	hub.Register(context.Background(), client)
	handler.connMap.Store(c.Fd(), client)

	// Send a heartbeat message.
	msg := &proto.Message{Cmd: proto.CmdHeartbeat}
	frame, _ := pb.Marshal(msg)

	handler.processFrame(frame, c)

	// Heartbeat response should arrive on the client's send channel
	// (dispatched via worker pool → router).
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

	// Send garbage — should not close connection, just log.
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

	// Register target in hub for online delivery.
	target := newTestClient(t, "bob", "Bob")
	hub.Register(context.Background(), target)

	// alice sends a chat claiming to be "eve" (spoof attempt).
	msg := &proto.Message{
		Cmd:      proto.CmdChat,
		From:     "eve", // spoofed sender — should be overwritten
		To:       "bob",
		ChatType: proto.ChatTypeSingle,
		MsgType:  proto.MsgTypeText,
		Content:  "I am eve",
	}
	frame, _ := pb.Marshal(msg)

	handler.processFrame(frame, c)

	// Target receives the message — From should be "alice", not "eve".
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
	// Set context to an invalid type (not "pending" and not *Client).
	c.SetContext(42)

	handler.processFrame([]byte("test"), c)
	if !c.closed {
		t.Error("expected connection to be closed on invalid context type")
	}
}

// =============================================================================
// OnTraffic tests (frame decoding)
// =============================================================================

func TestGnetOnTrafficCompleteFrame(t *testing.T) {
	handler, hub, jwtMgr := newTestGnetHandler(t)
	c := newMockGnetConn(5)
	c.SetContext("pending")

	// Set up a complete login frame in the mock's buffer.
	frame := loginFrame(t, jwtMgr, "carol", "Carol")
	c.setFrame(frame)

	// OnTraffic should decode the frame and call processFrame.
	action := handler.OnTraffic(c)
	if action != gnet.None {
		t.Errorf("expected gnet.None, got %v", action)
	}
	// Connection should not be closed.
	if c.closed {
		t.Error("connection should not be closed")
	}
	// Carol should be registered.
	if !hub.IsOnline(context.Background(), "carol") {
		t.Error("carol should be online after login via OnTraffic")
	}
}

func TestGnetOnTrafficFrameTooLarge(t *testing.T) {
	handler, _, _ := newTestGnetHandler(t)
	c := newMockGnetConn(6)

	// Set up a frame with a length > maxFrameSize (65536).
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

	// Less than 4 bytes — incomplete header.
	c.frameBuf = []byte{0x00, 0x01}

	action := handler.OnTraffic(c)
	if action != gnet.None {
		t.Errorf("expected gnet.None for incomplete header, got %v", action)
	}
}

func TestGnetOnTrafficIncompletePayload(t *testing.T) {
	handler, _, _ := newTestGnetHandler(t)
	c := newMockGnetConn(8)

	// Header says 100 bytes, but only 10 bytes follow.
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, 100)
	c.frameBuf = append(header, make([]byte, 10)...)

	action := handler.OnTraffic(c)
	// Should wait for more data (incomplete payload).
	if action != gnet.None {
		t.Errorf("expected gnet.None for incomplete payload, got %v", action)
	}
}

func TestGnetOnTrafficMultipleFrames(t *testing.T) {
	handler, hub, jwtMgr := newTestGnetHandler(t)
	c := newMockGnetConn(9)
	c.SetContext("pending")

	// Set up two login frames back-to-back. First logs in, second is ignored
	// (already authenticated, but it's a CmdLogin on an authenticated conn —
	// processFrame won't go through the login path since ctx is *Client, not "pending").
	// Just test that the first frame is properly consumed.
	frame := loginFrame(t, jwtMgr, "dave", "Dave")
	// Put two frames in buffer.
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
// Heartbeat checker tests
// =============================================================================

func TestHeartbeatCheckerKicksStaleConnections(t *testing.T) {
	hub := NewHub(100)
	sg, _ := snowflake.New(1)
	jwtMgr := jwt.New("test-secret", time.Hour)
	router := NewRouter(hub, hub, sg, nil, DefaultRouterConfig())

	ctx := context.Background()
	// Create handler with very short heartbeat timeout.
	handler := NewGnetHandler(
		ctx, router, hub, jwtMgr,
		256,                  // sendBufSize
		65536,                // maxMsgSize
		100*time.Millisecond, // heartbeatTimeout — connections older than this get kicked
		4,                    // workerPoolSize
	)

	c := newMockGnetConn(10)
	client := newTestClient(t, "eve", "Eve")
	client.transport = newGnetTransport(c)
	c.SetContext(client)
	hub.Register(context.Background(), client)
	handler.connMap.Store(c.Fd(), client)

	// Set heartbeat to a time far in the past.
	client.SetHeartbeat(time.Now().Add(-time.Hour))

	// The checker runs at heartbeatTimeout/2 = 50ms intervals.
	// Wait for the checker to detect the stale connection.
	time.Sleep(300 * time.Millisecond)

	// Client should be unregistered.
	if hub.IsOnline(context.Background(), "eve") {
		t.Error("eve should have been kicked by heartbeat checker")
	}
	// connMap should be cleaned.
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
		10*time.Second,       // heartbeatTimeout — very long, won't trigger
		4,                    // workerPoolSize
	)

	c := newMockGnetConn(11)
	client := newTestClient(t, "frank", "Frank")
	client.transport = newGnetTransport(c)
	c.SetContext(client)
	hub.Register(context.Background(), client)
	handler.connMap.Store(c.Fd(), client)

	// Heartbeat is current (set by NewClient).
	time.Sleep(100 * time.Millisecond)

	// Client should still be online.
	if !hub.IsOnline(context.Background(), "frank") {
		t.Error("frank should still be online (heartbeat is recent)")
	}
	// connMap entry should still exist.
	if _, ok := handler.connMap.Load(c.Fd()); !ok {
		t.Error("connMap entry should still exist")
	}
}

// =============================================================================
// Transport tests
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
// Compile-time interface check
// =============================================================================

var _ gnet.Conn = (*mockGnetConn)(nil)
