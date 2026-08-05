// Package observability 提供 Prometheus 指标采集能力。
//
// 核心是 Metrics 类型:聚合 IM 的业务计数器/直方图,并经 /metrics 端点暴露。
// 所有 Record* 方法均 nil-safe —— 未配置指标(指针为 nil)的调用方可直接调用,
// 不会 panic。这保证了现有测试与未接线指标的路径保持原样。
package observability

import (
	"net/http"
	"strconv"
	"time"

	"github.com/im/api/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
)

// Options 是 NewMetrics 的配置项。
// 各回调在 /metrics 被抓取时惰性求值,用于暴露实时内部状态。
type Options struct {
	// OnlineConnections 返回当前在线连接数(WebSocket + gnet TCP,经 Hub.Count)。
	OnlineConnections func() int
	// RateLimitStats 返回限流器累计的放行/拒绝消息数。
	RateLimitStats func() (allowed, rejected int64)
	// DedupMarks 返回去重缓存累计标记的消息数。
	DedupMarks func() int64
	// GnetPoolDrops 返回 gnet worker 池满丢弃的消息数。
	GnetPoolDrops func() int64
}

// Metrics 聚合全部业务指标。Prometheus collector 自身线程安全,可并发读写。
// 字段为底层 collector(供测试直接断言),Record* 方法为 nil-safe 记录接口。
type Metrics struct {
	registry *prometheus.Registry

	Commands          *prometheus.CounterVec   // im_commands_total{cmd}
	MessagesReceived  *prometheus.CounterVec   // im_messages_received_total{chat_type}
	MessagesDelivered *prometheus.CounterVec   // im_messages_delivered_total{chat_type}
	DeliveryFailures  prometheus.Counter       // im_delivery_failures_total
	DuplicateDropped  prometheus.Counter       // im_duplicate_dropped_total
	GroupFanoutSends  prometheus.Counter       // im_group_fanout_sends_total
	PersistSuccess    prometheus.Counter       // im_persist_success_total
	PersistFail       prometheus.Counter       // im_persist_fail_total
	PersistQueueDrop  prometheus.Counter       // im_persist_queue_drop_total
	DeliveryDuration  *prometheus.HistogramVec // im_message_delivery_duration_seconds{chat_type}
}

// 投递延迟直方图的桶边界(秒)。IM 投递延迟多在毫秒级,低端密集覆盖。
var deliveryBuckets = []float64{0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1}

// NewMetrics 构建并注册全部指标族。
// 实时状态 gauge(在线连接、限流、去重、gnet 丢弃)无条件注册,回调为空时返回 0,
// 保证 /metrics 输出的指标族恒定存在(仪表盘可稳定引用)。
func NewMetrics(opts Options) *Metrics {
	reg := prometheus.NewRegistry()

	// 默认 runtime + 进程指标(go_* / process_*)。
	reg.MustRegister(prometheus.NewGoCollector())
	reg.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	m := &Metrics{registry: reg}

	m.Commands = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "im_commands_total",
		Help: "处理的命令总数,按命令类型区分。",
	}, []string{"cmd"})
	m.MessagesReceived = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "im_messages_received_total",
		Help: "网关收到的聊天消息数,按聊天类型区分。",
	}, []string{"chat_type"})
	m.MessagesDelivered = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "im_messages_delivered_total",
		Help: "成功投递给在线接收方的消息数,按聊天类型区分。",
	}, []string{"chat_type"})
	m.DeliveryFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "im_delivery_failures_total",
		Help: "在线投递失败并转存离线的消息数(发送缓冲满/连接关闭)。",
	})
	m.DuplicateDropped = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "im_duplicate_dropped_total",
		Help: "被去重缓存识别为重复并丢弃(重放 ACK)的消息数。",
	})
	m.GroupFanoutSends = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "im_group_fanout_sends_total",
		Help: "群聊扇出中逐成员发送尝试的次数。",
	})
	m.PersistSuccess = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "im_persist_success_total",
		Help: "异步持久化(MySQL Save)成功的消息数。",
	})
	m.PersistFail = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "im_persist_fail_total",
		Help: "异步持久化(MySQL Save)失败的消息数。",
	})
	m.PersistQueueDrop = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "im_persist_queue_drop_total",
		Help: "持久化队列满被丢弃的消息数(背压信号)。",
	})
	m.DeliveryDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "im_message_delivery_duration_seconds",
		Help:    "从网关收到消息到向发送方回写 ACK 的耗时(仅 NeedAck 消息)。",
		Buckets: deliveryBuckets,
	}, []string{"chat_type"})

	reg.MustRegister(
		m.Commands, m.MessagesReceived, m.MessagesDelivered,
		m.DeliveryFailures, m.DuplicateDropped, m.GroupFanoutSends,
		m.PersistSuccess, m.PersistFail, m.PersistQueueDrop,
		m.DeliveryDuration,
	)

	// 实时状态 gauge(抓取时惰性求值,回调为空时返回 0)。
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "im_online_connections",
		Help: "当前在线连接数(WebSocket + gnet TCP)。",
	}, func() float64 {
		if opts.OnlineConnections != nil {
			return float64(opts.OnlineConnections())
		}
		return 0
	}))
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "im_rate_limit_allowed_total",
		Help: "限流器累计放行的消息数。",
	}, func() float64 {
		if opts.RateLimitStats == nil {
			return 0
		}
		allowed, _ := opts.RateLimitStats()
		return float64(allowed)
	}))
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "im_rate_limit_rejected_total",
		Help: "限流器累计拒绝的消息数。",
	}, func() float64 {
		if opts.RateLimitStats == nil {
			return 0
		}
		_, rejected := opts.RateLimitStats()
		return float64(rejected)
	}))
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "im_dedup_marks_total",
		Help: "去重缓存累计标记的消息数。",
	}, func() float64 {
		if opts.DedupMarks != nil {
			return float64(opts.DedupMarks())
		}
		return 0
	}))
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "im_gnet_pool_drop_total",
		Help: "gnet worker 池满丢弃的消息数。",
	}, func() float64 {
		if opts.GnetPoolDrops != nil {
			return float64(opts.GnetPoolDrops())
		}
		return 0
	}))

	return m
}

// Handler 返回 /metrics 的 HTTP 处理器。
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// CollectFamilies 返回当前全部指标族(供测试与调试读取结构化值)。
func (m *Metrics) CollectFamilies() ([]*dto.MetricFamily, error) {
	return m.registry.Gather()
}

// ---------- 以下为 nil-safe 的记录方法 ----------

// RecordCommand 记录一条已处理的命令。
func (m *Metrics) RecordCommand(cmd int32) {
	if m == nil {
		return
	}
	m.Commands.WithLabelValues(cmdName(cmd)).Inc()
}

// RecordMessagesReceived 记录一条网关收到的聊天消息。
func (m *Metrics) RecordMessagesReceived(chatType string) {
	if m == nil {
		return
	}
	m.MessagesReceived.WithLabelValues(chatType).Inc()
}

// RecordMessageDelivered 记录一条成功投递给在线接收方的消息。
func (m *Metrics) RecordMessageDelivered(chatType string) {
	if m == nil {
		return
	}
	m.MessagesDelivered.WithLabelValues(chatType).Inc()
}

// RecordDeliveryFailure 记录一次投递失败(转存离线)。
func (m *Metrics) RecordDeliveryFailure() {
	if m == nil {
		return
	}
	m.DeliveryFailures.Inc()
}

// RecordDuplicateDropped 记录一条被去重丢弃的消息。
func (m *Metrics) RecordDuplicateDropped() {
	if m == nil {
		return
	}
	m.DuplicateDropped.Inc()
}

// RecordGroupFanoutSend 记录一次群聊扇出中的逐成员发送尝试。
func (m *Metrics) RecordGroupFanoutSend() {
	if m == nil {
		return
	}
	m.GroupFanoutSends.Inc()
}

// RecordPersistSuccess 记录一次异步持久化成功。
func (m *Metrics) RecordPersistSuccess() {
	if m == nil {
		return
	}
	m.PersistSuccess.Inc()
}

// RecordPersistFail 记录一次异步持久化失败。
func (m *Metrics) RecordPersistFail() {
	if m == nil {
		return
	}
	m.PersistFail.Inc()
}

// RecordPersistQueueDrop 记录一次持久化队列满丢弃。
func (m *Metrics) RecordPersistQueueDrop() {
	if m == nil {
		return
	}
	m.PersistQueueDrop.Inc()
}

// RecordDeliveredDuration 记录一次消息投递耗时(收到 → ACK 回写)。
func (m *Metrics) RecordDeliveredDuration(chatType string, d time.Duration) {
	if m == nil {
		return
	}
	m.DeliveryDuration.WithLabelValues(chatType).Observe(d.Seconds())
}

// ---------- 辅助 ----------

// ChatTypeStr 返回消息聊天类型的指标标签值("group" / "single")。
func ChatTypeStr(msg *proto.Message) string {
	if msg.ChatType == proto.ChatTypeGroup {
		return "group"
	}
	return "single"
}

// cmdNames 映射命令常量到人类可读名称(proto 中为 int32 常量,无生成枚举表)。
var cmdNames = map[int32]string{
	proto.CmdNone:              "CmdNone",
	proto.CmdChat:              "CmdChat",
	proto.CmdAck:               "CmdAck",
	proto.CmdLogin:             "CmdLogin",
	proto.CmdLoginResp:         "CmdLoginResp",
	proto.CmdOffline:           "CmdOffline",
	proto.CmdHeartbeat:         "CmdHeartbeat",
	proto.CmdKick:              "CmdKick",
	proto.CmdHistory:           "CmdHistory",
	proto.CmdReadReceipt:       "CmdReadReceipt",
	proto.CmdUnreadCount:       "CmdUnreadCount",
	proto.CmdSearch:            "CmdSearch",
	proto.CmdGroupCreate:       "CmdGroupCreate",
	proto.CmdGroupJoin:         "CmdGroupJoin",
	proto.CmdGroupLeave:        "CmdGroupLeave",
	proto.CmdGroupInfo:         "CmdGroupInfo",
	proto.CmdGroupList:         "CmdGroupList",
	proto.CmdFile:              "CmdFile",
	proto.CmdGroupInviteMember: "CmdGroupInviteMember",
	proto.CmdRecall:            "CmdRecall",
	proto.CmdFriendRequest:     "CmdFriendRequest",
	proto.CmdFriendResponse:    "CmdFriendResponse",
	proto.CmdTyping:            "CmdTyping",
	proto.CmdForward:           "CmdForward",
	proto.CmdEdit:              "CmdEdit",
}

// cmdName 将命令常量转为指标标签值,未知命令回退为数字字符串。
func cmdName(cmd int32) string {
	if n, ok := cmdNames[cmd]; ok {
		return n
	}
	return strconv.FormatInt(int64(cmd), 10)
}
