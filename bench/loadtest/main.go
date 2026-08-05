// Command loadtest 是 IM 网关的压测工具。
//
// 覆盖场景:
//
//	churn      连接抖动(连接-登录-心跳-断开循环)
//	chat       单聊吞吐(配对互发,测 ACK 延迟 + 端到端投递延迟)
//	group      群聊扇出(创建大群,测单条消息扇出最差延迟)
//	search     历史/搜索读路径(HTTP /search + WS CmdHistory 并发)
//	heartbeat  心跳浸泡(大量空闲连接长时间保持)
//
// 依赖 docker-compose 全栈 + configs/config.bench.json(关限流、开 pprof),
// Gateway 与 Logic 以 go run 原生运行,压测客户端通过 127.0.0.1 访问。
//
// 用法示例:
//
//	go run ./bench/loadtest -scenario chat -users 1000 -rate 20 -duration 60s
//	go run ./bench/loadtest -scenario churn -connections 1000 -conn-rate 50 -duration 60s
//	go run ./bench/loadtest -scenario group -users 500 -group-size 500 -duration 30s
//	go run ./bench/loadtest -scenario search -workers 50 -duration 60s -query bench
//	go run ./bench/loadtest -scenario heartbeat -connections 2000 -duration 10m
package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/im/bench/kit"
)

type options struct {
	scenario  string
	addr      string
	tcpAddr   string
	transport string

	users   int // chat 用户数(配对)
	conns   int // churn / heartbeat 连接数
	rate    float64
	duration time.Duration
	workers int
	connRate float64 // churn 连接速率 conn/s

	groupSize int
	groups    int
	msgs      int // group 场景发送消息数

	query      string // search 关键字
	historyCon int    // search 场景的 WS CmdHistory 并发数

	interval time.Duration // heartbeat 心跳间隔

	seqBase int64 // 本次运行的 seq 起点(避免与历史 dedup 记录冲突)

	pct    []float64
	jsonOut bool
	verbose bool
}

func main() {
	var opts options
	flag.StringVar(&opts.scenario, "scenario", "", "churn|chat|group|search|heartbeat (required)")
	flag.StringVar(&opts.addr, "addr", "localhost:8080", "gateway HTTP/WS base address")
	flag.StringVar(&opts.tcpAddr, "tcp-addr", "localhost:18083", "gateway gnet TCP address")
	flag.StringVar(&opts.transport, "transport", "ws", "ws|tcp")

	flag.IntVar(&opts.users, "users", 100, "chat: number of users (must be even)")
	flag.IntVar(&opts.conns, "connections", 100, "churn/heartbeat: number of connections")
	flag.Float64Var(&opts.rate, "rate", 1, "chat: messages/sec per user")
	flag.DurationVar(&opts.duration, "duration", 60*time.Second, "test duration")
	flag.IntVar(&opts.workers, "workers", 10, "concurrency for login/connect/search")
	flag.Float64Var(&opts.connRate, "conn-rate", 50, "churn: connection rate (conn/s)")

	flag.IntVar(&opts.groupSize, "group-size", 100, "group: members per group")
	flag.IntVar(&opts.groups, "groups", 1, "group: number of groups")
	flag.IntVar(&opts.msgs, "msgs", 100, "group: messages to send")

	flag.StringVar(&opts.query, "query", "bench", "search: fulltext keyword")
	flag.IntVar(&opts.historyCon, "history-workers", 10, "search: concurrent WS CmdHistory workers")

	flag.DurationVar(&opts.interval, "interval", 30*time.Second, "heartbeat: interval between heartbeats")

	flag.Var(&pctFlag{&opts.pct}, "pct", "latency percentiles to report, comma-separated (default 50,95,99)")
	flag.BoolVar(&opts.jsonOut, "json", false, "output results as JSON")
	flag.BoolVar(&opts.verbose, "verbose", false, "verbose per-scenario logging")

	flag.Parse()

	if opts.pct == nil {
		opts.pct = []float64{50, 95, 99}
	}

	if opts.scenario == "" {
		usageAndExit("scenario is required")
	}
	if opts.users%2 != 0 {
		usageAndExit("users must be even (pairs)")
	}

	// seq 起点取运行时的随机值,保证每次运行唯一:网关的 dedup 按
	// (fromUID, seq) 去重,若重复运行仍从 seq=1 开始,会把历史记录判为
	// 重复(只回 ACK 不投递),导致投递量统计失真。
	opts.seqBase = rand.Int63n(1<<52) << 8 // 每个 sender 最多占用 255 个 seq

	httpBase := "http://" + opts.addr
	wsURL := "ws://" + opts.addr + "/ws"

	// 检查网关可达。
	if err := kit.WaitHealthy(httpBase+"/health", 5*time.Second); err != nil {
		log.Fatalf("gateway not reachable at %s: %v", httpBase, err)
	}

	log.Printf("loadtest: scenario=%s addr=%s transport=%s duration=%s",
		opts.scenario, opts.addr, opts.transport, opts.duration)

	var (
		result *Result
		err    error
	)
	switch opts.scenario {
	case "churn":
		result, err = runChurn(&opts, httpBase, wsURL)
	case "chat":
		result, err = runChat(&opts, httpBase, wsURL)
	case "group":
		result, err = runGroup(&opts, httpBase, wsURL)
	case "search":
		result, err = runSearch(&opts, httpBase, wsURL)
	case "heartbeat":
		result, err = runHeartbeat(&opts, httpBase, wsURL)
	default:
		usageAndExit("unknown scenario: " + opts.scenario)
	}
	if err != nil {
		log.Fatalf("scenario %s failed: %v", opts.scenario, err)
	}

	out := os.Stdout
	if opts.jsonOut {
		result.writeJSON(out)
	} else {
		result.writeTable(out)
	}
}

func usageAndExit(msg string) {
	fmt.Fprintln(os.Stderr, "error:", msg)
	fmt.Fprintln(os.Stderr, "run 'go run ./bench/loadtest -h' for usage")
	os.Exit(2)
}

// pctFlag 解析逗号分隔的百分位列表。
type pctFlag struct{ dst *[]float64 }

func (p pctFlag) String() string { return "" }
func (p pctFlag) Set(v string) error {
	parts := strings.Split(v, ",")
	var out []float64
	for _, s := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
		if err != nil {
			return fmt.Errorf("invalid pct %q", s)
		}
		out = append(out, f)
	}
	*p.dst = out
	return nil
}
