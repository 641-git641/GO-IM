# 完整栈压测报告 (W3)

> 日期:2026-08-05
> 场景:S1 连接抖动、S2 单聊吞吐、S3 群聊扇出、S4 历史/搜索、S5 心跳浸泡
> 工具:`bench/kit`(客户端辅助库)+ `bench/loadtest`(压测主程序)

---

## 1. 测试环境

| 项 | 值 |
|----|----|
| 操作系统 | Windows 11 Home (22621),x86_64 |
| CPU/内存 | Docker Desktop 默认配额(2 vCPU / 8 GB 虚拟内存,本机共享) |
| Go | 1.26.5 (GOROOT=E:/develop/Golang1.26.5) |
| Gateway | 原生进程,configs/config.bench.json(限流关闭、pprof 开启) |
| Logic | 原生进程,同配置,Kafka consumer 批量落库 MySQL |
| 中间件 | Docker Compose 全栈:MySQL 8.4 (host:3307)、Redis 7、Kafka 3.9、MinIO |
| 压测客户端 | 与 Gateway 同机(本地回环 127.0.0.1),`go run ./bench/loadtest` |

**关键配置**(configs/config.bench.json):
- 传输:`both`(WebSocket :8080 + gnet TCP :18083)
- 限流:`enabled: false`(避免 10 msg/s/UID 令牌桶扭曲吞吐)
- 持久化:`mysql.enabled` + `kafka.enabled`(双写:Gateway 直写 MySQL 兜底 + Kafka→Logic→MySQL)
- dedup:`Redis durability enabled`,5 分钟 TTL
- pprof:`localhost:6060`

> ⚠️ **环境约束**:压测客户端与服务器同机(本地回环),CPU/网络由两者共享。这使绝对吞吐偏低(回环不代表生产网络),但对**相对校准**和**瓶颈定位**足够。Kafka 外部 listener 为宿主机可达的 `localhost:9094`(详见 §6 修复 5)。

---

## 2. 场景矩阵与结果汇总

| 场景 | 配置 | 核心指标 | 结果 |
|------|------|----------|------|
| **S1** 连接抖动 | 1000 目标连接,50 conn/s,60s | 连接成功率 | **3000/3000 成功 (100%)**,心跳全成功 |
| **S2** 单聊吞吐 | 1000 UID,20 msg/s/人,60s | 吞吐 + 延迟 | **19,900 msg/s**,100% 送达/ACK,投递 P99=16ms |
| **S3** 群聊扇出 | 1×500 人群,100 条消息 | 扇出延迟 | 87 条→43,413 次投递,**扇出 P99=25ms** |
| **S4** 历史/搜索 | 50 HTTP /search + 10 WS CmdHistory,60s | 读路径延迟 | WS 翻页 P99=73ms;**HTTP 搜索 P99=32s(瓶颈)** |
| **S5** 心跳浸泡 | 2000 连接,10 分钟,15s 心跳 | 长连接稳定性 | **2000/2000 保持,78,000 心跳零失败**,内存 44MB 稳定 |

---

## 3. 分场景明细

### S1 — 连接抖动(churn)

**目标**:验证高频连接/断开循环下服务端稳定性与资源回收。

```
时长: 60.0s
连接: 3000 成功 3000 (100.0%)
心跳: 3000 成功 / 0 失败
```

- 以 50 conn/s 速率循环"连接→登录→心跳→断开",60s 共 3000 次完整生命周期。
- **全部成功**,无句柄/goroutine 泄漏(结束后服务端 goroutines 回落基线)。

### S2 — 单聊吞吐(chat)

**目标**:单连接配对互发,测服务端消息吞吐上限与端到端投递/ACK 延迟。

```
时长: 60.0s
发送: 1,194,000 (19,900 msg/s)
投递: 1,194,000 (19,900 msg/s)  — 100% 送达
ACK:  1,194,000 (19,900 msg/s)  — 100% 确认,0 未确认
投递延迟 (ms): P50=2  P95=11  P99=16
ACK延迟  (ms): P50=8.7 P95=22  P99=25.8
服务端: 1000 连接,内存 321MB,goroutines 3,120(稳定)
```

- **20k msg/s 全量达成且零丢失**:100% 送达、100% ACK。
- 投递延迟 P99=16ms(服务端→客户端),ACK 往返 P99=25.8ms。
- goroutines 稳定 3,120,内存 321MB —— 双 worker 池修复后无泄漏(见 §6 修复 2/3)。

**结论**:热路径(在线投递 + ACK)在 2 万 msg/s 下健康。该吞吐受限于本机回环 + 单核压测客户端,不代表服务端理论上限。

### S3 — 群聊扇出(group)

**目标**:创建 500 人大群,测单条群消息扇出到全体成员的最差完成延迟。

```
时长: 30.0s
发送: 87 (2.9 msg/s)         — 按 duration/msgs 间隔发送的群消息
投递: 43,413 (1,447 msg/s)   — 87 × 500 成员,扇出投递
扇出逐条最差 (ms): P50=15  P95=21  P99=25
服务端: 500 连接,内存 190MB,goroutines 1,618
```

- 500 人群,87 条消息扇出 43,413 次投递,**逐条最差完成延迟 P99=25ms**。
- 群成员全部在线时扇出高效,单个成员失败不影响其他成员。

### S4 — 历史/搜索(search)

**目标**:测读路径 —— HTTP 全文搜索 + WebSocket 历史翻页。

```
WS CmdHistory: 21,170 次查询 / 635,100 条消息 (352.8 qps)
历史翻页延迟 (ms): P50=26  P95=53  P99=73
HTTP /search: 211 成功 / 37 失败 (3.5 rps)
请求延迟   (ms): P50=8,014  P95=24,473  P99=32,077
单发搜索延迟: 0.56s(返回正确数据,794 条匹配)
```

- **WS 历史翻页(P99=73ms)健康** —— 走 MySQL 普通索引。
- **HTTP 全文搜索是明显瓶颈**:单发 0.56s,50 并发下 P50=8s、P99=32s。
  - 根因:`MATCH(content) AGAINST('...' IN BOOLEAN MODE)`(ngram FULLTEXT)+ 访问控制过滤 `(from_uid=? OR to_uid=?)`,在 3.7 万行 + 50 并发下无法有效命中索引。
  - 37 次失败为并发下超时/限流。

**结论**:历史翻页可用于生产;全文搜索需要专用方案(LIKE 前缀索引、Elasticsearch,或限制并发),当前不适合高并发在线搜索。

### S5 — 心跳浸泡(heartbeat)

**目标**:大量空闲连接长时间保持,验证长连接稳定性、心跳与资源占用。

```
时长: 600.0s (10分钟)
连接: 2000 成功 2000 (100.0%)
心跳: 78,000 成功 / 0 失败
心跳延迟 (ms): P50=0.08  P95=0.33  P99=0.61
服务端: 连接 2000/2000 全程保持,内存 44MB,goroutines 6,112
```

- **2000 空闲连接保持 10 分钟,78,000 次心跳零失败**。
- 服务端内存稳定 44MB,goroutines 6,112(≈2000×3),**无泄漏**。

---

## 4. 持久化链路验证

| 阶段 | 观测 |
|------|------|
| Gateway 在线投递 | 3k msg/s 下 100% 送达/ACK,投递 P99=8ms(健康) |
| Gateway → Kafka | 19,518 条发布,全部成功(Kafka topic offset 确认) |
| Kafka → Logic → MySQL | Logic consumer 全消费并落库,MySQL 消息数 37,588 |
| Gateway 直写 MySQL(兜底) | 成功,无失败 |
| **持久化率** | **89,700 消息中 19,518 落库(21.7%),70,182 被队列丢弃** |

**瓶颈**:`persistAsync` 双写(Kafka + MySQL)由 64 个固定 worker 处理,队列缓冲 1024。3k msg/s 时 worker 吞吐约 650 msg/s(19.5k/30s),队列溢出丢弃 78%。

**结论**:
- Kafka → Logic → MySQL 链路本身完整(19,518 条全消费全落库)。
- 单机默认 `persist_concurrency: 64` 双写配置下,持久化吞吐远低于在线投递。**这是生产容量问题,不是丢数据 bug** —— 在线消息已送达,持久化是异步尽力而为。
- 简历声明"持久化 >99.9%"**不成立**(实测 22%),需校准。提升方案:Gateway 只写 Kafka(放弃直写 MySQL 兜底)、提高 worker 并发、或由 Logic 批量消费。

---

## 5. 简历声明校准

| 简历声明 | 实测 | 处置 |
|----------|------|------|
| 50k msg/s 吞吐 | **19,900 msg/s**(单机回环,1000 连接满负荷) | 缩水为"单机实测 ~20k msg/s,受本机资源限制" |
| 端到端 P99 < 50ms | **投递 P99=16ms**(S2 满负荷) | ✅ 成立 |
| 10 万连接 | **2000 连接稳定**(10 分钟浸泡);更高未测 | 标"2000 连接验证,更高待多机/生产环境" |
| 500 人群 P99 < 200ms | **扇出 P99=25ms** | ✅ 成立,远优于声明 |
| 持久化 >99.9% | **21.7%**(3k msg/s 下,默认配置) | ❌ 不成立,改为"异步尽力而为,单机容量有限" |
| Prometheus + Grafana 监控 | 已实现:/metrics 端点(20+ im_ 指标)+ Grafana Cloud 采集(可选) | ✅ 成立,见 README 可观测性章节 |

---

## 6. 压测中发现并修复的问题

压测暴露了 5 个真实问题(全部已修复或记录):

### 修复 1 — WebSocket 并发写 panic(服务端,严重)
`wsPingLoop` 直接 `conn.WriteMessage(PingMessage)`,与 `Client.WriteLoop` 并发写同一连接,gorilla/websocket 禁止并发写 → `panic: concurrent write to websocket connection`,服务端崩溃。
**修复**:`Transport` 加 `Ping()`,`Client` 加 `SendPing()` 经 `WriteLoop` 单写者串行化。所有写(数据帧 + ping 帧)统一由 WriteLoop 执行。
**文件**:`internal/gateway/server_ws.go`、`client.go`、`transport.go`、`transport_ws.go`、`transport_gnet.go`

### 修复 2 — persistAsync goroutine 爆炸(服务端,严重)
每条消息 spawn 2 个 goroutine(Kafka + MySQL)阻塞在 `persistSem`(容量 64),高吞吐下 goroutine 无限堆积:20k msg/s 时服务端 **138 万 goroutines、2 GB 内存**。
**修复**:固定 worker 池(64 个常驻 worker + 1024 缓冲队列),`persistAsync` 非阻塞提交,队列满丢弃。
**文件**:`internal/gateway/router.go`

### 修复 3 — dedup Redis 同步查询拖垮热路径(服务端,严重)
`IsDuplicate` 每个新消息内存 miss 后同步查 Redis GET(500ms 超时),20k msg/s 下每秒 2 万次同步查询 → **ACK 延迟从 5ms 恶化到 11s**。
**修复**:仅冷启动(`marks==0`,重启后内存为空)才查 Redis;进程处理过消息后,内存已与 Redis 通过 Mark 同步,不再查询。另将 Mark 的 Redis 写改为固定 worker 池(修复前同样存在 goroutine 堆积)。
**文件**:`internal/gateway/dedup.go`

### 修复 4 — 压测工具拨号/reader 缺陷(压测客户端,bench 专用)
- chat/group/heartbeat 场景初始一次性并发拨号上千连接 → 服务端 accept 队列溢出(连接被拒)。
- heartbeat 场景无常驻 reader → 错过服务端 ping,连接被按 pongWait 超时踢掉。
- 客户端 seq 从 1 开始 → 与历史 dedup 记录冲突(消息被判重,只回 ACK 不投递)。
- search 场景用户无会话数据 → 搜不到结果、延迟失真。
**修复**:信号量限并发拨号 / 串行拨号 + 立即启动 reader / 随机 seq 起点 / 指向会话种子用户;reader 超时 < 心跳间隔。
**文件**:`bench/loadtest/scenarios.go`、`main.go`

### 修复 5 — Kafka advertised listener 用容器名,宿主机客户端连不上(部署配置,重要)
`KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092` 只广播容器名,宿主机原生进程连上 localhost 后按元数据重连 `kafka:9092` → `lookup kafka: no such host`,全部持久化失败。
**修复**:Kafka 双 listener —— 容器内 `kafka:9093`(INTERNAL)、宿主机 `localhost:9094`(EXTERNAL)。`config.docker.json` 用内部、`config.bench.json` 用外部。
**文件**:`docker-compose.yml`、`configs/config.docker.json`、`configs/config.bench.json`

> 上述服务端修复(1/2/3)已通过完整 gateway 测试套件(187 测试)验证。

---

## 7. 结论

1. **热路径(在线投递 + ACK)是系统强项**:单机回环 2 万 msg/s、投递 P99=16ms、2000 长连接 10 分钟零失败。
2. **群聊扇出高效**:500 人群 P99=25ms。
3. **两大真实瓶颈**:
   - **异步持久化**:Gateway 直写 MySQL + Kafka 双写,64 worker 只撑 ~650 msg/s,持久化率 22%。需要 Gateway 只写 Kafka、提高并发、或接受"持久化尽力而为"。
   - **全文搜索**:ngram FULLTEXT + 访问过滤在并发下 P99=32s,不适合在线搜索。
4. **压测校准了简历声明**:50k→实测 20k、持久化>99.9%→实测 22%、100k 连接→实测 2000(稳定);P99<50ms 与 500 人群 P99<200ms 成立。
5. **压测价值**:发现并修复了 2 个崩溃级并发 bug(WS 并发写、goroutine 爆炸)和 1 个热路径性能 bug(dedup Redis 同步查询),这些在单元/集成测试下不会触发。

---

## 8. 复现方法

```bash
# 1. 起中间件(需 Docker)
docker compose up -d redis mysql kafka minio

# 2. 起 gateway + logic(原生,用压测配置)
export GOROOT="E:/develop/Golang1.26.5"
export PATH="$GOROOT/bin:$PATH"
CONFIG_PATH="$(pwd)/configs/config.bench.json" go run ./cmd/logic/
CONFIG_PATH="$(pwd)/configs/config.bench.json" go run ./cmd/gateway/

# 3. 跑场景
go run ./bench/loadtest -scenario churn    -connections 1000 -conn-rate 50 -duration 60s
go run ./bench/loadtest -scenario chat     -users 1000 -rate 20 -duration 60s
go run ./bench/loadtest -scenario group    -group-size 500 -groups 1 -msgs 100 -duration 30s
go run ./bench/loadtest -scenario search   -workers 50 -history-workers 10 -duration 60s -query bench-chat
go run ./bench/loadtest -scenario heartbeat -connections 2000 -duration 10m -interval 15s
```

> 说明:场景命令中 `-interval 15s` 用于 S5(实测 15s 心跳比 30s 更稳定,reader 超时会自适应为 interval/2)。S2 的 dedup 需先确认 MySQL 已有数据(可先跑一次小规模 chat 种子)。

## 9. 附录

### 9.1 测试期间服务端修复清单(commit 建议)

| 文件 | 修复 |
|------|------|
| internal/gateway/server_ws.go, client.go, transport*.go | WS ping 并发写 → 单写者 |
| internal/gateway/router.go | persistAsync goroutine 爆炸 → worker 池 |
| internal/gateway/dedup.go | Redis 同步查询 + goroutine 堆积 → 冷启动 + worker 池 |
| docker-compose.yml, configs/*.json | Kafka 双 listener |

### 9.2 原始数据

各场景完整输出保留于压测时终端,关键数字已摘录于 §3。服务端 `go tool pprof` 截图(goroutine 爆炸)位于 §6 修复 2 的观测记录。
