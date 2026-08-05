package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/im/bench/kit"
)

// series 是一组延迟样本(毫秒),支持并发追加与百分位计算。
type series struct {
	mu     sync.Mutex
	values []float64
}

func (s *series) add(ms float64) {
	if ms < 0 {
		ms = 0
	}
	s.mu.Lock()
	s.values = append(s.values, ms)
	s.mu.Unlock()
}

// percentiles 计算给定样本序列的延迟百分位,返回 pct→毫秒 映射。
func (s *series) percentiles(pcts []float64) map[float64]float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.values) == 0 {
		return nil
	}
	sorted := make([]float64, len(s.values))
	copy(sorted, s.values)
	sort.Float64s(sorted)

	out := make(map[float64]float64, len(pcts))
	for _, p := range pcts {
		idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(sorted) {
			idx = len(sorted) - 1
		}
		out[p] = sorted[idx]
	}
	return out
}

func (s *series) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.values)
}

// stats 收集压测过程中的计数与各类延迟分布。
type stats struct {
	delivery series // 端到端投递延迟(服务端→客户端)
	ack      series // ACK 往返延迟(客户端→服务端→客户端)
	fanout   series // 群聊单条消息跨成员完成延迟
	request  series // HTTP /search 等请求延迟
	hist     series // WS CmdHistory 翻页延迟

	sent       atomic.Int64
	delivered  atomic.Int64
	acked      atomic.Int64
	failed     atomic.Int64
	connects   atomic.Int64
	connectOK  atomic.Int64
	drops      atomic.Int64
	searchOK   atomic.Int64
	searchFail atomic.Int64
	historyOK  atomic.Int64
	historyMsg atomic.Int64
	hbOK       atomic.Int64
	hbFail     atomic.Int64
}

// sample 记录一次对 /health 的采样。
type sample struct {
	Connections int
	AllocMB     int
	Goroutines  int
}

// ServiceSummary 汇总压测期间的服务端采样。
type ServiceSummary struct {
	MinConnections  int `json:"min_connections"`
	MaxConnections  int `json:"max_connections"`
	LastConnections int `json:"last_connections"`
	MinAllocMB      int `json:"min_alloc_mb"`
	MaxAllocMB      int `json:"max_alloc_mb"`
	LastAllocMB     int `json:"last_alloc_mb"`
	MinGoroutines   int `json:"min_goroutines"`
	MaxGoroutines   int `json:"max_goroutines"`
	LastGoroutines  int `json:"last_goroutines"`
	Samples         int `json:"samples"`
}

// Result 是压测的运行结果,可输出为表格或 JSON。
type Result struct {
	Scenario  string            `json:"scenario"`
	DurationS float64           `json:"duration_s"`
	Sent      int64             `json:"sent"`
	Delivered int64             `json:"delivered"`
	Acked     int64             `json:"acked"`
	Failed    int64             `json:"failed"`
	Drops     int64             `json:"drops"`
	Connects  int64             `json:"connects"`
	ConnectOK int64             `json:"connect_ok"`
	SearchOK  int64             `json:"search_ok"`
	SearchFail int64            `json:"search_fail"`
	HistoryOK int64             `json:"history_ok"`
	HistoryMsg int64            `json:"history_messages"`
	HbOK      int64             `json:"heartbeat_ok"`
	HbFail    int64             `json:"heartbeat_fail"`
	Delivery  map[float64]float64 `json:"delivery_latency_ms,omitempty"`
	AckLat    map[float64]float64 `json:"ack_latency_ms,omitempty"`
	Fanout    map[float64]float64 `json:"fanout_latency_ms,omitempty"`
	Request   map[float64]float64 `json:"request_latency_ms,omitempty"`
	Hist      map[float64]float64 `json:"history_latency_ms,omitempty"`
	Service   *ServiceSummary     `json:"service,omitempty"`
	Extra     map[string]string   `json:"extra,omitempty"`
}

func (r *Result) writeTable(w io.Writer) {
	fmt.Fprintf(w, "\n=== 压测结果: %s ===\n", r.Scenario)
	fmt.Fprintf(w, "时长: %.1fs\n", r.DurationS)
	if r.Sent > 0 {
		fmt.Fprintf(w, "发送: %d (%.1f msg/s)\n", r.Sent, float64(r.Sent)/r.DurationS)
	}
	if r.Delivered > 0 {
		fmt.Fprintf(w, "投递: %d (%.1f msg/s)\n", r.Delivered, float64(r.Delivered)/r.DurationS)
	}
	if r.Acked > 0 {
		miss := r.Sent - r.Acked
		if miss < 0 {
			miss = 0
		}
		fmt.Fprintf(w, "ACK:  %d (%.1f msg/s) 未确认 %d\n", r.Acked, float64(r.Acked)/r.DurationS, miss)
	}
	if r.Connects > 0 {
		rate := float64(r.ConnectOK) / float64(r.Connects) * 100
		fmt.Fprintf(w, "连接: %d 成功 %d (%.1f%%)\n", r.Connects, r.ConnectOK, rate)
	}
	if r.SearchOK+r.SearchFail > 0 {
		fmt.Fprintf(w, "搜索: %d 成功 / %d 失败 (%.1f rps)\n", r.SearchOK, r.SearchFail, float64(r.SearchOK)/r.DurationS)
	}
	if r.HistoryOK > 0 {
		fmt.Fprintf(w, "历史查询: %d 次 / %d 条消息 (%.1f rps)\n", r.HistoryOK, r.HistoryMsg, float64(r.HistoryOK)/r.DurationS)
	}
	if r.HbOK+r.HbFail > 0 {
		fmt.Fprintf(w, "心跳: %d 成功 / %d 失败\n", r.HbOK, r.HbFail)
	}
	printSeries(w, "投递延迟 (ms)", r.Delivery)
	printSeries(w, "ACK延迟 (ms)", r.AckLat)
	printSeries(w, "扇出逐条最差 (ms)", r.Fanout)
	printSeries(w, "请求延迟 (ms)", r.Request)
	printSeries(w, "历史翻页延迟 (ms)", r.Hist)
	if r.Failed > 0 {
		fmt.Fprintf(w, "错误: %d\n", r.Failed)
	}
	if r.Service != nil {
		fmt.Fprintf(w, "服务端: 连接 min/max/last = %d/%d/%d 内存(MB) %d/%d/%d goroutines %d/%d/%d 采样 %d\n",
			r.Service.MinConnections, r.Service.MaxConnections, r.Service.LastConnections,
			r.Service.MinAllocMB, r.Service.MaxAllocMB, r.Service.LastAllocMB,
			r.Service.MinGoroutines, r.Service.MaxGoroutines, r.Service.LastGoroutines,
			r.Service.Samples)
	}
}

func printSeries(w io.Writer, label string, m map[float64]float64) {
	if len(m) == 0 {
		return
	}
	fmt.Fprintf(w, "%s:", label)
	for _, p := range sortedPcts(m) {
		fmt.Fprintf(w, " P%v=%v", p, m[p])
	}
	fmt.Fprintln(w)
}

func (r *Result) writeJSON(w io.Writer) {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(r)
}

func sortedPcts(m map[float64]float64) []float64 {
	keys := make([]float64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Float64s(keys)
	return keys
}

// startServiceSampler 在后台周期性采样 /health,返回停止函数(场景结束后调用拿汇总)。
func startServiceSampler(healthURL string, interval time.Duration, done <-chan struct{}) func() *ServiceSummary {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	s := &serviceSampler{healthURL: healthURL, interval: interval}

	ticker := time.NewTicker(interval)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if h, err := kit.GetHealth(healthURL); err == nil {
					s.record(&sample{
						Connections: h.Connections,
						AllocMB:     h.Memory.AllocMB,
						Goroutines:  h.Memory.Goroutines,
					})
				}
			}
		}
	}()

	return func() *ServiceSummary {
		ticker.Stop()
		select {
		case <-stopped:
		case <-time.After(interval + time.Second):
		}
		return s.summary()
	}
}

type serviceSampler struct {
	healthURL string
	interval  time.Duration
	mu        sync.Mutex
	min       *sample
	max       *sample
	last      *sample
	count     int
}

func (s *serviceSampler) record(p *sample) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.count++
	s.last = p
	if s.min == nil {
		s.min = p
		s.max = p
		return
	}
	if p.Connections < s.min.Connections {
		s.min.Connections = p.Connections
	}
	if p.Connections > s.max.Connections {
		s.max.Connections = p.Connections
	}
	if p.AllocMB < s.min.AllocMB {
		s.min.AllocMB = p.AllocMB
	}
	if p.AllocMB > s.max.AllocMB {
		s.max.AllocMB = p.AllocMB
	}
	if p.Goroutines < s.min.Goroutines {
		s.min.Goroutines = p.Goroutines
	}
	if p.Goroutines > s.max.Goroutines {
		s.max.Goroutines = p.Goroutines
	}
}

func (s *serviceSampler) summary() *ServiceSummary {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.count == 0 {
		return nil
	}
	return &ServiceSummary{
		MinConnections:  s.min.Connections,
		MaxConnections:  s.max.Connections,
		LastConnections: s.last.Connections,
		MinAllocMB:      s.min.AllocMB,
		MaxAllocMB:      s.max.AllocMB,
		LastAllocMB:     s.last.AllocMB,
		MinGoroutines:   s.min.Goroutines,
		MaxGoroutines:   s.max.Goroutines,
		LastGoroutines:  s.last.Goroutines,
		Samples:         s.count,
	}
}
