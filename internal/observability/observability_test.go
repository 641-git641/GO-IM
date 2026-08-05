package observability

import (
	"math"
	"testing"
	"time"

	"github.com/im/api/proto"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// allFamilies 是 NewMetrics(空 Options)应注册的全部指标族。
var allFamilies = []string{
	"im_online_connections",
	"im_commands_total",
	"im_messages_received_total",
	"im_messages_delivered_total",
	"im_delivery_failures_total",
	"im_rate_limit_allowed_total",
	"im_rate_limit_rejected_total",
	"im_duplicate_dropped_total",
	"im_dedup_marks_total",
	"im_group_fanout_sends_total",
	"im_persist_success_total",
	"im_persist_fail_total",
	"im_persist_queue_drop_total",
	"im_message_delivery_duration_seconds",
	"im_gnet_pool_drop_total",
	"go_goroutines",
}

// registeredNames 返回 registry 中全部已注册指标族名。
func registeredNames(t *testing.T, m *Metrics) map[string]bool {
	t.Helper()
	ms, err := m.registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	seen := make(map[string]bool, len(ms))
	for _, mf := range ms {
		seen[mf.GetName()] = true
	}
	return seen
}

func TestNewMetricsRegistersFamilies(t *testing.T) {
	m := NewMetrics(Options{})
	// CounterVec/HistogramVec 在首次记录前不会物化,先各记录一次使其出现在输出中。
	m.RecordCommand(proto.CmdChat)
	m.RecordMessagesReceived("single")
	m.RecordMessageDelivered("single")
	m.RecordDeliveryFailure()
	m.RecordDuplicateDropped()
	m.RecordGroupFanoutSend()
	m.RecordPersistSuccess()
	m.RecordPersistFail()
	m.RecordPersistQueueDrop()
	m.RecordDeliveredDuration("single", time.Millisecond)
	seen := registeredNames(t, m)
	for _, f := range allFamilies {
		if !seen[f] {
			t.Errorf("metric family %q not registered", f)
		}
	}
}

func TestRecordMethodsIncrementCounters(t *testing.T) {
	m := NewMetrics(Options{})

	m.RecordCommand(proto.CmdChat)
	m.RecordCommand(proto.CmdChat)
	m.RecordCommand(proto.CmdRecall)
	m.RecordMessagesReceived("single")
	m.RecordMessageDelivered("group")
	m.RecordDeliveryFailure()
	m.RecordDuplicateDropped()
	m.RecordGroupFanoutSend()
	m.RecordPersistSuccess()
	m.RecordPersistFail()
	m.RecordPersistQueueDrop()

	if got := testutil.ToFloat64(m.Commands.WithLabelValues("CmdChat")); got != 2 {
		t.Errorf("im_commands_total{CmdChat}: expected 2, got %v", got)
	}
	if got := testutil.ToFloat64(m.Commands.WithLabelValues("CmdRecall")); got != 1 {
		t.Errorf("im_commands_total{CmdRecall}: expected 1, got %v", got)
	}
	if got := testutil.ToFloat64(m.MessagesReceived.WithLabelValues("single")); got != 1 {
		t.Errorf("im_messages_received_total{single}: expected 1, got %v", got)
	}
	if got := testutil.ToFloat64(m.MessagesDelivered.WithLabelValues("group")); got != 1 {
		t.Errorf("im_messages_delivered_total{group}: expected 1, got %v", got)
	}
	for name, c := range map[string]func() float64{
		"im_delivery_failures_total":  func() float64 { return testutil.ToFloat64(m.DeliveryFailures) },
		"im_duplicate_dropped_total":  func() float64 { return testutil.ToFloat64(m.DuplicateDropped) },
		"im_group_fanout_sends_total": func() float64 { return testutil.ToFloat64(m.GroupFanoutSends) },
		"im_persist_success_total":    func() float64 { return testutil.ToFloat64(m.PersistSuccess) },
		"im_persist_fail_total":       func() float64 { return testutil.ToFloat64(m.PersistFail) },
		"im_persist_queue_drop_total": func() float64 { return testutil.ToFloat64(m.PersistQueueDrop) },
	} {
		if got := c(); got != 1 {
			t.Errorf("%s: expected 1, got %v", name, got)
		}
	}
}

func TestRecordDeliveredDurationHistogram(t *testing.T) {
	m := NewMetrics(Options{})
	m.RecordDeliveredDuration("single", 5*time.Millisecond)
	m.RecordDeliveredDuration("single", 15*time.Millisecond)

	// 经 Gather 读取直方图的 sum 与 count(Observer 不能直接喂给 ToFloat64)。
	ms, err := m.registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	var sum float64
	var count uint64
	for _, mf := range ms {
		if mf.GetName() != "im_message_delivery_duration_seconds" {
			continue
		}
		for _, mm := range mf.GetMetric() {
			h := mm.GetHistogram()
			sum += h.GetSampleSum()
			count += h.GetSampleCount()
		}
	}
	if math.Abs(sum-0.020) > 1e-9 {
		t.Errorf("histogram sum: expected 0.020, got %v", sum)
	}
	if count != 2 {
		t.Errorf("histogram count: expected 2, got %d", count)
	}
}

func TestGaugeFuncsReflectOptions(t *testing.T) {
	// 验证实时状态 gauge 读取 Options 回调(惰性求值)。
	m := NewMetrics(Options{
		OnlineConnections: func() int { return 7 },
		RateLimitStats:    func() (int64, int64) { return 10, 3 },
		DedupMarks:        func() int64 { return 42 },
		GnetPoolDrops:     func() int64 { return 1 },
	})
	ms, err := m.registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	vals := map[string]float64{}
	for _, mf := range ms {
		for _, mm := range mf.GetMetric() {
			vals[mf.GetName()] = mm.GetGauge().GetValue()
		}
	}
	want := map[string]float64{
		"im_online_connections":        7,
		"im_rate_limit_allowed_total":  10,
		"im_rate_limit_rejected_total": 3,
		"im_dedup_marks_total":         42,
		"im_gnet_pool_drop_total":      1,
	}
	for name, w := range want {
		if got, ok := vals[name]; !ok || got != w {
			t.Errorf("gauge %q: expected %v, got %v (ok=%v)", name, w, got, ok)
		}
	}
}

func TestNilSafety(t *testing.T) {
	// nil 接收者调用全部记录方法,不应 panic。
	var m *Metrics
	m.RecordCommand(proto.CmdChat)
	m.RecordMessagesReceived("single")
	m.RecordMessageDelivered("group")
	m.RecordDeliveryFailure()
	m.RecordDuplicateDropped()
	m.RecordGroupFanoutSend()
	m.RecordPersistSuccess()
	m.RecordPersistFail()
	m.RecordPersistQueueDrop()
	m.RecordDeliveredDuration("single", time.Millisecond)
}

func TestCmdName(t *testing.T) {
	if got := cmdName(proto.CmdChat); got != "CmdChat" {
		t.Errorf("cmdName(CmdChat): expected CmdChat, got %q", got)
	}
	if got := cmdName(999); got != "999" {
		t.Errorf("cmdName(999): expected 999, got %q", got)
	}
}

func TestChatTypeStr(t *testing.T) {
	if got := ChatTypeStr(&proto.Message{ChatType: proto.ChatTypeGroup}); got != "group" {
		t.Errorf("ChatTypeStr(group): expected group, got %q", got)
	}
	if got := ChatTypeStr(&proto.Message{ChatType: proto.ChatTypeSingle}); got != "single" {
		t.Errorf("ChatTypeStr(single): expected single, got %q", got)
	}
	if got := ChatTypeStr(&proto.Message{}); got != "single" {
		t.Errorf("ChatTypeStr(zero): expected single, got %q", got)
	}
}
