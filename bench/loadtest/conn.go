package main

import (
	"fmt"
	"time"

	"github.com/im/api/proto"
	"github.com/im/bench/kit"
)

// benchConn 抽象 WebSocket 与 gnet TCP 两种传输,供各场景共用。
// 每次调用都设置读写超时(gorilla/gnet 均要求调用方设置 deadline)。
type benchConn interface {
	ReadMsg(timeout time.Duration) (*proto.Message, error)
	WriteMsg(msg *proto.Message, timeout time.Duration) error
	Close() error
}

type wsConn struct{ c *kit.WSClient }

func (w *wsConn) ReadMsg(d time.Duration) (*proto.Message, error) { return w.c.ReadMessage(d) }
func (w *wsConn) WriteMsg(m *proto.Message, d time.Duration) error { return w.c.WriteMessage(m, d) }
func (w *wsConn) Close() error                                     { return w.c.Close() }

type tcpConn struct{ c *kit.TCPClient }

func (t *tcpConn) ReadMsg(d time.Duration) (*proto.Message, error) { return t.c.ReadFrame(d) }
func (t *tcpConn) WriteMsg(m *proto.Message, d time.Duration) error { return t.c.WriteFrame(m, d) }
func (t *tcpConn) Close() error                                     { return t.c.Close() }

// dial 建立连接并按传输类型完成登录握手,返回可用的 benchConn。
// loginResp 在 ws 传输下是 CmdLoginResp,在 tcp 传输下由首帧 CmdLogin 换取。
func dial(transport, wsURL, tcpAddr, token string) (benchConn, error) {
	if transport == "tcp" {
		tc, err := kit.ConnectTCP(tcpAddr)
		if err != nil {
			return nil, fmt.Errorf("tcp dial: %w", err)
		}
		conn := &tcpConn{tc}
		if _, err := tc.Login(token, 10*time.Second); err != nil {
			conn.Close()
			return nil, fmt.Errorf("tcp login: %w", err)
		}
		return conn, nil
	}

	wc, err := kit.ConnectWS(wsURL, token)
	if err != nil {
		return nil, fmt.Errorf("ws connect: %w", err)
	}
	conn := &wsConn{wc}
	if _, err := wc.DrainLoginResp(10 * time.Second); err != nil {
		conn.Close()
		return nil, fmt.Errorf("ws login: %w", err)
	}
	return conn, nil
}

// loginAll 并发登录 users 个用户,返回 uid→token 映射。
// 用户名与 uid 相同,UID 形如 "user-000000"(位数对齐便于排序)。
func loginAll(httpBase string, users, workers int) (map[string]string, error) {
	// 生成 UID 列表。
	uids := make([]string, users)
	for i := 0; i < users; i++ {
		uids[i] = fmt.Sprintf("user-%06d", i)
	}

	type job struct {
		idx   int
		token string
		err   error
	}
	work := make(chan int)
	results := make(chan job, users)

	// 启动 worker 池。
	for w := 0; w < workers; w++ {
		go func() {
			for idx := range work {
				uid := uids[idx]
				token, _, err := kit.LoginDev(httpBase+"/login", uid, uid)
				results <- job{idx: idx, token: token, err: err}
			}
		}()
	}
	go func() {
		for i := 0; i < users; i++ {
			work <- i
		}
		close(work)
	}()

	tokens := make(map[string]string, users)
	for i := 0; i < users; i++ {
		j := <-results
		if j.err != nil {
			return nil, fmt.Errorf("login %s: %w", uids[j.idx], j.err)
		}
		tokens[uids[j.idx]] = j.token
	}
	return tokens, nil
}
