package gateway

import (
	"context"
	"testing"

	"github.com/im/api/proto"
	"github.com/im/internal/observability"
	"github.com/im/internal/pkg/snowflake"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// histogramCount 读取某直方图族的累计样本数。
func histogramCount(t *testing.T, m *observability.Metrics, name string) uint64 {
	t.Helper()
	fams, err := m.CollectFamilies()
	if err != nil {
		t.Fatalf("CollectFamilies: %v", err)
	}
	for _, f := range fams {
		if f.GetName() != name {
			continue
		}
		var c uint64
		for _, mm := range f.GetMetric() {
			c += mm.GetHistogram().GetSampleCount()
		}
		return c
	}
	return 0
}

// TestMetricsIncrementedOnChat 验证路由一条在线聊天消息后各指标正确递增。
func TestMetricsIncrementedOnChat(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	m := observability.NewMetrics(observability.Options{})
	r.SetMetrics(m)

	target := newTestClient(t, "bob", "Bob")
	h.Register(context.Background(), target)
	sender := newTestClient(t, "alice", "Alice")

	r.Route(context.Background(), sender, &proto.Message{
		Cmd:      proto.CmdChat,
		From:     "alice",
		To:       "bob",
		ChatType: proto.ChatTypeSingle,
		MsgType:  proto.MsgTypeText,
		Content:  "Hello Bob!",
		NeedAck:  true,
		Seq:      1,
	})

	// 消费目标消息与发送者 ACK,避免阻塞。
	readMessageFromChan(t, target.send)
	readMessageFromChan(t, sender.send)

	if got := testutil.ToFloat64(m.Commands.WithLabelValues("CmdChat")); got != 1 {
		t.Errorf("im_commands_total{CmdChat}: expected 1, got %v", got)
	}
	if got := testutil.ToFloat64(m.MessagesReceived.WithLabelValues("single")); got != 1 {
		t.Errorf("im_messages_received_total{single}: expected 1, got %v", got)
	}
	if got := testutil.ToFloat64(m.MessagesDelivered.WithLabelValues("single")); got != 1 {
		t.Errorf("im_messages_delivered_total{single}: expected 1, got %v", got)
	}
	if got := testutil.ToFloat64(m.DeliveryFailures); got != 0 {
		t.Errorf("im_delivery_failures_total: expected 0, got %v", got)
	}
	if got := histogramCount(t, m, "im_message_delivery_duration_seconds"); got != 1 {
		t.Errorf("delivery duration histogram count: expected 1, got %d", got)
	}
}

// TestMetricsDuplicate 验证同一 Seq 重发时重复计数递增。
func TestMetricsDuplicate(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())
	m := observability.NewMetrics(observability.Options{})
	r.SetMetrics(m)

	target := newTestClient(t, "bob", "Bob")
	h.Register(context.Background(), target)
	sender := newTestClient(t, "alice", "Alice")

	msg := &proto.Message{
		Cmd:      proto.CmdChat,
		From:     "alice",
		To:       "bob",
		ChatType: proto.ChatTypeSingle,
		MsgType:  proto.MsgTypeText,
		Content:  "Hello Bob!",
		NeedAck:  true,
		Seq:      1,
	}
	r.Route(context.Background(), sender, msg)
	readMessageFromChan(t, target.send)
	readMessageFromChan(t, sender.send)

	// 同一 Seq 重发 → 命中去重,重放 ACK。
	r.Route(context.Background(), sender, msg)
	readMessageFromChan(t, sender.send) // 重放的 ACK

	if got := testutil.ToFloat64(m.DuplicateDropped); got != 1 {
		t.Errorf("im_duplicate_dropped_total: expected 1, got %v", got)
	}
	// 重复消息不再次计入已投递。
	if got := testutil.ToFloat64(m.MessagesDelivered.WithLabelValues("single")); got != 1 {
		t.Errorf("im_messages_delivered_total{single}: expected 1, got %v", got)
	}
}

// TestRateLimitStats 验证限流器的累计放行/拒绝计数可读。
func TestRateLimitStats(t *testing.T) {
	h := NewHub(100)
	sg, _ := snowflake.New(1)
	r := NewRouter(h, h, sg, nil, DefaultRouterConfig())

	// 1 msg/s,突发 1:同一发送者连发两条,第二条被拒。
	r.SetRateLimit(1, 1)
	sender := newTestClient(t, "alice", "Alice")

	msg := func(seq int64) *proto.Message {
		return &proto.Message{
			Cmd:      proto.CmdChat,
			From:     "alice",
			To:       "nobody",
			ChatType: proto.ChatTypeSingle,
			MsgType:  proto.MsgTypeText,
			Content:  "x",
			Seq:      seq,
		}
	}
	r.Route(context.Background(), sender, msg(1))
	r.Route(context.Background(), sender, msg(2))

	allowed, rejected := r.RateLimitStats()
	if allowed != 1 {
		t.Errorf("RateLimitStats allowed: expected 1, got %d", allowed)
	}
	if rejected != 1 {
		t.Errorf("RateLimitStats rejected: expected 1, got %d", rejected)
	}
}
