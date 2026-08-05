package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/im/api/proto"
	"github.com/im/bench/kit"
)

// ---------- 通用基础设施 ----------

// readLoop 在连接上持续读消息,直到 ctx 结束或出现非超时错误。
// fn 每收到一条消息调用一次。超时(空闲)不是错误,继续读。
// 返回是否因异常(非超时错误/断连)退出。
func readLoop(ctx context.Context, conn benchConn, timeout time.Duration, fn func(*proto.Message)) bool {
	for {
		msg, err := conn.ReadMsg(timeout)
		if err != nil {
			select {
			case <-ctx.Done():
				return false
			default:
			}
			if isTimeout(err) {
				continue
			}
			return true // 非超时错误/断连
		}
		if fn != nil {
			fn(msg)
		}
	}
}

// isTimeout 判断读取错误是否为超时(空闲)而非连接异常。
func isTimeout(err error) bool {
	ne, ok := err.(net.Error)
	return ok && ne.Timeout()
}

// buildResult 将 stats 组装为 Result,并生成服务端采样汇总。
func buildResult(scenario string, opts *options, st *stats, stopSampler func() *ServiceSummary) *Result {
	r := &Result{
		Scenario:   scenario,
		DurationS:  opts.duration.Seconds(),
		Sent:       st.sent.Load(),
		Delivered:  st.delivered.Load(),
		Acked:      st.acked.Load(),
		Failed:     st.failed.Load(),
		Drops:      st.drops.Load(),
		Connects:   st.connects.Load(),
		ConnectOK:  st.connectOK.Load(),
		SearchOK:   st.searchOK.Load(),
		SearchFail: st.searchFail.Load(),
		HistoryOK:  st.historyOK.Load(),
		HistoryMsg: st.historyMsg.Load(),
		HbOK:       st.hbOK.Load(),
		HbFail:     st.hbFail.Load(),
		Delivery:   st.delivery.percentiles(opts.pct),
		AckLat:     st.ack.percentiles(opts.pct),
		Fanout:     st.fanout.percentiles(opts.pct),
		Request:    st.request.percentiles(opts.pct),
		Hist:       st.hist.percentiles(opts.pct),
	}
	if stopSampler != nil {
		r.Service = stopSampler()
	}
	return r
}

// seqClock 记录 seq→发送时刻,用于计算 ACK 往返延迟。
type seqClock struct {
	mu sync.Mutex
	m  map[int64]time.Time
}

func newSeqClock() *seqClock { return &seqClock{m: map[int64]time.Time{}} }

func (s *seqClock) record(seq int64, t time.Time) {
	s.mu.Lock()
	s.m[seq] = t
	s.mu.Unlock()
}

func (s *seqClock) take(seq int64) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.m[seq]
	if ok {
		delete(s.m, seq)
	}
	return t, ok
}

// ---------- S1 连接抖动 ----------

func runChurn(opts *options, httpBase, wsURL string) (*Result, error) {
	st := &stats{}
	ctx, cancel := context.WithTimeout(context.Background(), opts.duration)
	defer cancel()

	done := make(chan struct{})
	defer close(done)
	stopSampler := startServiceSampler(httpBase+"/health", 10*time.Second, done)

	// 全局令牌:限制连接速率。每 1/connRate 秒放行一个连接。
	rate := opts.connRate
	if rate <= 0 {
		rate = 50
	}
	interval := time.Duration(float64(time.Second) / rate)
	ticks := time.NewTicker(interval)
	defer ticks.Stop()

	var wg sync.WaitGroup
	for w := 0; w < opts.workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			seq := 0
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticks.C:
				}

				uid := fmt.Sprintf("churn-%03d-%06d", worker, seq)
				seq++
				st.connects.Add(1)

				token, _, err := kit.LoginDev(httpBase+"/login", uid, uid)
				if err != nil {
					st.failed.Add(1)
					continue
				}
				conn, err := dial(opts.transport, wsURL, opts.tcpAddr, token)
				if err != nil {
					st.failed.Add(1)
					continue
				}
				st.connectOK.Add(1)

				// 心跳一次。
				if err := conn.WriteMsg(&proto.Message{Cmd: proto.CmdHeartbeat}, 5*time.Second); err == nil {
					if _, err := conn.ReadMsg(5 * time.Second); err == nil {
						st.hbOK.Add(1)
					}
				}
				conn.Close()
			}
		}(w)
	}
	wg.Wait()

	return buildResult("churn", opts, st, stopSampler), nil
}

// ---------- S2 单聊吞吐 ----------

// chatConn 把一个已连接的 benchConn 与它的发送时钟绑定。
type chatConn struct {
	conn benchConn
	uid  string
	clock *seqClock
}

func runChat(opts *options, httpBase, wsURL string) (*Result, error) {
	st := &stats{}
	ctx, cancel := context.WithTimeout(context.Background(), opts.duration)
	defer cancel()

	done := make(chan struct{})
	defer close(done)
	stopSampler := startServiceSampler(httpBase+"/health", 10*time.Second, done)

	// 拨号并发上限:与 churn 的连接速率一致,默认 50。
	connRate := opts.connRate
	if connRate <= 0 {
		connRate = 50
	}

	tokens, err := loginAll(httpBase, opts.users, opts.workers)
	if err != nil {
		return nil, err
	}

	// 建立全部连接。用信号量限制并发拨号数,避免一次性建立大量
	// TCP 连接导致服务端 accept 队列溢出(Windows 上表现为 refused)。
	conns := make([]*chatConn, opts.users)
	var connWG sync.WaitGroup
	sem := make(chan struct{}, int(connRate))
	for i := 0; i < opts.users; i++ {
		connWG.Add(1)
		go func(i int) {
			defer connWG.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			uid := fmt.Sprintf("user-%06d", i)
			conn, err := dial(opts.transport, wsURL, opts.tcpAddr, tokens[uid])
			if err != nil {
				st.failed.Add(1)
				log.Printf("[chat] connect %s failed: %v", uid, err)
				return
			}
			conns[i] = &chatConn{conn: conn, uid: uid, clock: newSeqClock()}
		}(i)
	}
	connWG.Wait()
	if st.failed.Load() > 0 {
		return nil, fmt.Errorf("%d/%d connections failed", st.failed.Load(), opts.users)
	}

	// 每个连接一个 reader:统计投递延迟与 ACK 往返。
	var readerWG sync.WaitGroup
	for i := 0; i < opts.users; i++ {
		cc := conns[i]
		readerWG.Add(1)
		go func(cc *chatConn) {
			defer readerWG.Done()
			readLoop(ctx, cc.conn, time.Second, func(msg *proto.Message) {
				switch msg.Cmd {
				case proto.CmdChat:
					st.delivered.Add(1)
					// msg.Timestamp 为服务端处理时间,衡量服务端→客户端投递延迟。
					st.delivery.add(float64(time.Now().UnixMilli() - msg.Timestamp))
				case proto.CmdAck:
					st.acked.Add(1)
					if t, ok := cc.clock.take(msg.Seq); ok {
						st.ack.add(float64(time.Since(t).Microseconds()) / 1000.0)
					}
				}
			})
		}(cc)
	}

	// 每个连接一个 sender,按 rate 向其配对用户发送。
	// 配对:user-0↔user-1, user-2↔user-3, ... (i ^ 1)
	// 每个连接的 seq 从独立区间起点递增,避免与历史 dedup 记录冲突。
	var senderWG sync.WaitGroup
	for i := 0; i < opts.users; i++ {
		cc := conns[i]
		partner := fmt.Sprintf("user-%06d", i^1)
		senderWG.Add(1)
		go func(i int, cc *chatConn) {
			defer senderWG.Done()
			seq := opts.seqBase + int64(i)*1_000_000
			sendEvery := time.Duration(float64(time.Second) / opts.rate)
			ticker := time.NewTicker(sendEvery)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
				now := time.Now()
				cc.clock.record(seq, now)
				st.sent.Add(1)
				msg := &proto.Message{
					Cmd:      proto.CmdChat,
					To:       partner,
					ChatType: proto.ChatTypeSingle,
					MsgType:  proto.MsgTypeText,
					Content:  "bench-chat-0000000000",
					NeedAck:  true,
					Seq:      seq,
				}
				seq++
				if err := cc.conn.WriteMsg(msg, 2*time.Second); err != nil {
					st.failed.Add(1)
					select {
					case <-ctx.Done():
						return
					default:
					}
				}
			}
		}(i, cc)
	}

	senderWG.Wait()
	// 等待 ACK 落定(短暂 grace)。
	time.Sleep(2 * time.Second)
	readerWG.Wait()

	return buildResult("chat", opts, st, stopSampler), nil
}

// ---------- S3 群聊扇出 ----------

func runGroup(opts *options, httpBase, wsURL string) (*Result, error) {
	st := &stats{}
	ctx, cancel := context.WithTimeout(context.Background(), opts.duration)
	defer cancel()

	done := make(chan struct{})
	defer close(done)
	stopSampler := startServiceSampler(httpBase+"/health", 10*time.Second, done)

	users := opts.groups * opts.groupSize
	tokens, err := loginAll(httpBase, users, opts.workers)
	if err != nil {
		return nil, err
	}

	// 建立全部成员连接。与 chat 一样用信号量限制并发拨号数。
	connRate := opts.connRate
	if connRate <= 0 {
		connRate = 50
	}
	conns := make([]benchConn, users)
	var connWG sync.WaitGroup
	sem := make(chan struct{}, int(connRate))
	for i := 0; i < users; i++ {
		connWG.Add(1)
		go func(i int) {
			defer connWG.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			uid := fmt.Sprintf("user-%06d", i)
			conn, err := dial(opts.transport, wsURL, opts.tcpAddr, tokens[uid])
			if err != nil {
				st.failed.Add(1)
				log.Printf("[group] connect %s failed: %v", uid, err)
				return
			}
			conns[i] = conn
		}(i)
	}
	connWG.Wait()
	if st.failed.Load() > 0 {
		return nil, fmt.Errorf("%d/%d connections failed", st.failed.Load(), users)
	}

	// 创建群:owner 通过 CmdGroupCreate 携带成员列表建群。
	type groupInfo struct {
		id      string
		owner   int
		members []int
	}
	groups := make([]groupInfo, opts.groups)

	readGroupCreate := func(ownerConn benchConn) (string, error) {
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			msg, err := ownerConn.ReadMsg(15 * time.Second)
			if err != nil {
				return "", fmt.Errorf("read group create resp: %w", err)
			}
			if msg.Cmd == proto.CmdGroupCreate {
				var resp struct {
					ID string `json:"id"`
				}
				if err := json.Unmarshal([]byte(msg.Content), &resp); err != nil {
					return "", fmt.Errorf("parse group create resp: %w", err)
				}
				if resp.ID != "" {
					return resp.ID, nil
				}
			}
			// 群通知等其他消息忽略。
		}
		return "", fmt.Errorf("timeout waiting group create response")
	}

	for g := 0; g < opts.groups; g++ {
		ownerIdx := g * opts.groupSize
		memberIdxs := make([]int, 0, opts.groupSize-1)
		memberUIDs := make([]string, 0, opts.groupSize-1)
		for m := ownerIdx + 1; m < ownerIdx+opts.groupSize; m++ {
			memberIdxs = append(memberIdxs, m)
			memberUIDs = append(memberUIDs, fmt.Sprintf("user-%06d", m))
		}
		req := map[string]interface{}{
			"name":    fmt.Sprintf("bench-group-%d", g),
			"members": memberUIDs,
		}
		content, _ := json.Marshal(req)

		ownerConn := conns[ownerIdx]
		if err := ownerConn.WriteMsg(&proto.Message{Cmd: proto.CmdGroupCreate, Content: string(content)}, 5*time.Second); err != nil {
			return nil, fmt.Errorf("group create write: %w", err)
		}
		gid, err := readGroupCreate(ownerConn)
		if err != nil {
			return nil, fmt.Errorf("group %d create: %w", g, err)
		}
		groups[g] = groupInfo{id: gid, owner: ownerIdx, members: memberIdxs}
		log.Printf("[group] created %s with %d members", gid, len(memberIdxs)+1)
	}

	// 每个成员的 reader:统计投递,并按 seq 记录"本成员收到该条消息的延迟"。
	// 结束后对每个 seq 取跨成员最大值 = 该条消息的扇出完成时间。
	var fanMu sync.Mutex
	fanBySeq := map[int64]float64{}

	var readerWG sync.WaitGroup
	for i := 0; i < users; i++ {
		conn := conns[i]
		readerWG.Add(1)
		go func(c benchConn) {
			defer readerWG.Done()
			readLoop(ctx, c, time.Second, func(msg *proto.Message) {
				if msg.Cmd != proto.CmdChat || msg.Seq <= 0 {
					return
				}
				st.delivered.Add(1)
				lat := float64(time.Now().UnixMilli() - msg.Timestamp)
				fanMu.Lock()
				if v, ok := fanBySeq[msg.Seq]; !ok || lat > v {
					fanBySeq[msg.Seq] = lat
				}
				fanMu.Unlock()
			})
		}(conn)
	}

	// owner 发送 msgs 条消息,间隔 = duration/msgs。
	sendEvery := time.Duration(float64(opts.duration) / float64(opts.msgs))
	if sendEvery <= 0 {
		sendEvery = 100 * time.Millisecond
	}
	var ownerWG sync.WaitGroup
	for g := 0; g < opts.groups; g++ {
		grp := groups[g]
		ownerConn := conns[grp.owner]
		ownerWG.Add(1)
		go func(gid string, c benchConn) {
			defer ownerWG.Done()
			seq := opts.seqBase + int64(g*100) + 50 // 每个群一段独立 seq 空间
			ticker := time.NewTicker(sendEvery)
			defer ticker.Stop()
			for i := 0; i < opts.msgs; i++ {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
				st.sent.Add(1)
				msg := &proto.Message{
					Cmd:      proto.CmdChat,
					To:       gid,
					ChatType: proto.ChatTypeGroup,
					MsgType:  proto.MsgTypeText,
					Content:  "bench-group-message",
					NeedAck:  true,
					Seq:      seq,
				}
				seq++
				if err := c.WriteMsg(msg, 2*time.Second); err != nil {
					st.failed.Add(1)
				}
			}
		}(grp.id, ownerConn)
	}
	ownerWG.Wait()

	// 等扇出落定,再关闭 reader。
	time.Sleep(3 * time.Second)
	cancel()
	readerWG.Wait()

	// 聚合跨成员最差延迟。
	fanMu.Lock()
	for _, v := range fanBySeq {
		st.fanout.add(v)
	}
	fanMu.Unlock()

	return buildResult("group", opts, st, stopSampler), nil
}

// ---------- S4 历史/搜索 ----------

func runSearch(opts *options, httpBase, wsURL string) (*Result, error) {
	st := &stats{}
	ctx, cancel := context.WithTimeout(context.Background(), opts.duration)
	defer cancel()

	done := make(chan struct{})
	defer close(done)
	stopSampler := startServiceSampler(httpBase+"/health", 10*time.Second, done)

	// HTTP /search 并发。
	var searchWG sync.WaitGroup
	for w := 0; w < opts.workers; w++ {
		searchWG.Add(1)
		go func(w int) {
			defer searchWG.Done()
			uid := fmt.Sprintf("user-%06d", w*2) // 指向 chat 场景种子的用户,确保有会话数据可搜
			token, _, err := kit.LoginDev(httpBase+"/login", uid, uid)
			if err != nil {
				st.failed.Add(1)
				return
			}
			params := url.Values{}
			params.Set("uid", uid)
			params.Set("token", token)
			params.Set("q", opts.query)
			params.Set("limit", "20")
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				begin := time.Now()
				_, err := kit.HTTPGetJSON(httpBase, "/search", params)
				if err != nil {
					st.searchFail.Add(1)
				} else {
					st.searchOK.Add(1)
					st.request.add(float64(time.Since(begin).Microseconds()) / 1000.0)
				}
			}
		}(w)
	}

	// WS CmdHistory 并发:每 worker 一个连接,反复翻页直到 duration。
	var histWG sync.WaitGroup
	for w := 0; w < opts.historyCon; w++ {
		histWG.Add(1)
		go func(w int) {
			defer histWG.Done()
			uid := fmt.Sprintf("user-%06d", w*2)          // 查自己与配对用户的历史
			partner := fmt.Sprintf("user-%06d", w*2+1)    // 指向 chat 场景种子的用户对
			token, _, err := kit.LoginDev(httpBase+"/login", uid, uid)
			if err != nil {
				st.failed.Add(1)
				return
			}
			conn, err := dial(opts.transport, wsURL, opts.tcpAddr, token)
			if err != nil {
				st.failed.Add(1)
				return
			}
			defer conn.Close()

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				req := &proto.Message{
					Cmd:      proto.CmdHistory,
					To:       partner,
					ChatType: proto.ChatTypeSingle,
					Seq:      30, // 每页 30 条
					Timestamp: time.Now().UnixMilli(),
				}
				begin := time.Now()
				if err := conn.WriteMsg(req, 5*time.Second); err != nil {
					st.failed.Add(1)
					return
				}
				// 读取直到完成信号(CmdHistory,Seq = 已投递条数)。
				count := int64(0)
				done2 := false
				for !done2 {
					msg, err := conn.ReadMsg(15 * time.Second)
					if err != nil {
						st.failed.Add(1)
						return
					}
					if msg.Cmd == proto.CmdHistory {
						count = msg.Seq
						done2 = true
					}
				}
				st.historyOK.Add(1)
				st.historyMsg.Add(count)
				st.hist.add(float64(time.Since(begin).Microseconds()) / 1000.0)
			}
		}(w)
	}

	searchWG.Wait()
	histWG.Wait()

	return buildResult("search", opts, st, stopSampler), nil
}

// ---------- S5 心跳浸泡 ----------

// hbConn 包装一个心跳连接及其上次发送时间。
// lastSent 由 sender 写、reader 读,用互斥锁保护。
type hbConn struct {
	mu       sync.Mutex
	conn     benchConn
	lastSent time.Time
}

func (h *hbConn) markSent(t time.Time) {
	h.mu.Lock()
	h.lastSent = t
	h.mu.Unlock()
}

func (h *hbConn) takeSent() time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lastSent
}

func runHeartbeat(opts *options, httpBase, wsURL string) (*Result, error) {
	st := &stats{}
	ctx, cancel := context.WithTimeout(context.Background(), opts.duration)
	defer cancel()

	done := make(chan struct{})
	defer close(done)
	stopSampler := startServiceSampler(httpBase+"/health", 10*time.Second, done)

	// 顺序建连,每个连接拨号成功立即可启动 reader。
	// 实测发现两种写法会导致连接悄然丢失:
	//   - 并发拨号(信号量):本地回环上瞬间建立上千连接,连接大量被 RST。
	//   - "全部拨号完成后再启动 reader":连接建立后有一段无读取的空窗,
	//     同样触发连接被断开。
	// 边拨号边启动 reader 的写法在 500/2000 连接下全部稳定保持。
	// 顺序拨号每个连接 ~毫秒级,千级连接在十几秒内完成,对浸泡场景足够。
	var hbWG sync.WaitGroup
	var readerWG sync.WaitGroup
	for i := 0; i < opts.conns; i++ {
		uid := fmt.Sprintf("hb-%05d", i)
		token, _, err := kit.LoginDev(httpBase+"/login", uid, uid)
		if err != nil {
			st.failed.Add(1)
			continue
		}
		conn, err := dial(opts.transport, wsURL, opts.tcpAddr, token)
		if err != nil {
			st.failed.Add(1)
			continue
		}
		st.connects.Add(1)
		st.connectOK.Add(1)

		hc := &hbConn{conn: conn}

		// 常驻 reader:持续 ReadMessage,处理服务端 WS ping/pong 保持连接
		// (gorilla 的 pong 应答发生在 ReadMessage 内),并匹配心跳响应。
		// 读超时必须小于心跳间隔,否则 reader 长时间阻塞在超时等待上,
		// 心跳响应无法被及时消费,连接会被服务端按 pongWait 误判死亡。
		readTimeout := 30 * time.Second
		if opts.interval > 0 && opts.interval < readTimeout {
			readTimeout = opts.interval / 2
		}
		if readTimeout < time.Second {
			readTimeout = time.Second
		}
		readerWG.Add(1)
		go func(hc *hbConn) {
			defer readerWG.Done()
			for {
				msg, err := hc.conn.ReadMsg(readTimeout)
				if err != nil {
					select {
					case <-ctx.Done():
						return
					default:
					}
					if isTimeout(err) {
						continue
					}
					st.hbFail.Add(1)
					return
				}
				if msg.Cmd == proto.CmdHeartbeat {
					st.hbOK.Add(1)
					if t := hc.takeSent(); !t.IsZero() {
						st.ack.add(float64(time.Since(t).Microseconds()) / 1000.0)
					}
				}
			}
		}(hc)

		// 心跳 sender:按 interval 周期发送 CmdHeartbeat。
		hbWG.Add(1)
		go func(hc *hbConn) {
			defer hbWG.Done()
			ticker := time.NewTicker(opts.interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
				hc.markSent(time.Now())
				if err := hc.conn.WriteMsg(&proto.Message{Cmd: proto.CmdHeartbeat}, 10*time.Second); err != nil {
					st.hbFail.Add(1)
				}
			}
		}(hc)
	}
	if st.failed.Load() > 0 {
		log.Printf("[heartbeat] %d connections failed, continuing with %d", st.failed.Load(), st.connectOK.Load())
	}

	hbWG.Wait()
	readerWG.Wait()

	return buildResult("heartbeat", opts, st, stopSampler), nil
}
