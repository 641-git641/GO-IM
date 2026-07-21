# 04 — Phase 1 实施计划（合并版）

> 版本: v1.3 | 日期: 2026-07-18 | 状态: Phase 1 ✅ 已完成 / Phase 2 ✅ 已完成 | 基于: [02-代码审查](./02-code-review.md) + [03-下一步计划](./03-next-steps.md)

---

## 实施状态总览

**Phase 1 全部完成**：15 个代码缺陷已修复。Phase 2 全部完成（详见 `docs/05-phase2-completion.md`）。85 个测试通过 + 5 个集成测试。

| 类别 | 数量 | 状态 |
|------|------|------|
| P0 修复 (正确性) | 4 | ✅ 全部完成 |
| P1 修复 (健壮性) | 11 | ✅ 9 完成，2 延迟 |
| P2 清理 (卫生) | 2 | ✅ 1 完成，1 延迟 |
| 文档更新 | 3 | ✅ 全部完成 |
| 集成测试 | 2 | ✅ TestIntegrationEndToEnd + TestOfflineMessage 通过 |

### Phase 2 全部完成（2026-07-18）

| 项目 | 状态 | 说明 |
|------|------|------|
| 单元测试 | ✅ | 85 个测试：snowflake(7), jwt(8), hub(10), router(18), redis(8), gnet_handler(27), repo(7) |
| Interface abstractions (ClientRegistry, OfflineStore) | ✅ | `internal/gateway/interfaces.go`，Router/Server/Client 已改为依赖接口 |
| Context propagation | ✅ | `context.Context` 贯穿 HTTP → Server → readPump → Router → Hub |
| Redis 存储 | ✅ | `RedisOfflineStore` 基于 Redis Lists + Lua 脚本，带内存 fallback |
| Protobuf 序列化 | ✅ | `api/proto/message.proto` → `.pb.go`，WebSocket BinaryMessage + Redis 二进制存储 |
| gnet TCP | ✅ | 双传输支持：WebSocket (gorilla/websocket) + TCP (panjf2000/gnet v2)，通过 Transport 接口抽象 |
| CmdHistory（超出原计划） | ✅ | 消息历史查询，分页支持，需 MySQL MessageStore |
| MySQL repo（超出原计划） | ✅ | `internal/repo/` 含 UserStore + MessageStore 接口，MySQLStore 实现 |
| gnet 单元测试（超出原计划） | ✅ | 27 个测试覆盖 WorkerPool, OnOpen, OnClose, handleLogin, processFrame, OnTraffic, heartbeat checker, Transport |
| TCP 集成测试（超出原计划） | ✅ | 3 个端到端测试：混合传输、离线消息、TCP 心跳 |
| Protocol fix（超出原计划） | ✅ | gnet 出站消息添加 4 字节长度前缀，与入站协议对称 |

### 延迟项全部完成

原计划延迟到 Phase 2 的 Interface abstractions、Context propagation、单元测试均已完成。此外新增了 gnet handler 测试（27 个）、TCP 集成测试（3 个）、CmdHistory、MySQL repo 层。详见 [05-phase2-completion.md](./05-phase2-completion.md)。

### 文档状态

| 文档 | 版本 | 状态 |
|------|------|------|
| `CLAUDE.md` | current | ✅ 已更新 |
| `docs/01-architecture-design.md` | v1.1 | ✅ 新增 5.5-5.8 四节 |
| `docs/04-phase1-implementation.md` | v1.1 | ✅ 本文件 |
| `configs/config.example.json` | — | ✅ 已创建 |
| `go.mod` | go 1.26.5 | ✅ 依赖正确 |
| `.vscode/settings.json` | — | ✅ GOROOT 已配置 |

### 环境

```
GOROOT: E:\develop\Golang1.26.5
GOPATH: E:\develop\Go
go version: go1.26.5 windows/amd64
```

### 基础设施要求（2026-07-17 新增）

**所有中间件必须以 Docker 容器形式启动。** 包括 Redis、MySQL 及后续可能引入的 Kafka、ETCD 等。Gateway 自身在开发阶段可原生运行以便快速迭代，但所有依赖服务均通过 `docker-compose.yml` 容器化部署。

```bash
# 启动所有中间件依赖
docker-compose up -d

# 停止并清理
docker-compose down
```

### Phase 2 — 全部完成 ✅

1. ✅ **单元测试** — 85 个测试 + 5 集成测试，覆盖 7 个包
2. ✅ **Docker 基础设施** — `docker-compose.yml`（Redis 7-alpine + MySQL 8.0 已配置）
3. ✅ **Interface abstractions** — ClientRegistry / OfflineStore 接口（`interfaces.go`）
4. ✅ **Context propagation** — HTTP → WS → Router → Hub 全链路 context
5. ✅ **Redis 存储** — Redis-backed OfflineStore + Lua 脚本 + 内存 fallback（`redis_store.go`）
6. ✅ **Protobuf 序列化** — 替代 JSON，WebSocket BinaryMessage + Redis 二进制（`message.proto`）
7. ✅ **gnet TCP** — 双传输支持：WebSocket + gnet TCP，Transport 接口抽象
8. ✅ **CmdHistory** — 消息历史查询，分页支持（before + limit），需 MySQL
9. ✅ **MySQL repo** — UserStore + MessageStore 接口 + MySQLStore 实现（`internal/repo/`）
10. ✅ **gnet handler 测试** — 27 个单元测试覆盖全部 GnetHandler 组件
11. ✅ **TCP 集成测试** — 3 个端到端测试：混合传输、离线消息、心跳
12. ✅ **Protocol fix** — gnet 出站消息添加 4 字节长度前缀

---

本文档合并 `02-code-review.md` 和 `03-next-steps.md`，覆盖代码审查发现的全量 19 个问题。每个问题标注严重级别、涉及文件、修复方案和验证方式。（**所有已计划的修复项均已完成，以下保留原始设计文档供参考。**）

---

## 1. 问题总览

| ID | 级别 | 文件 | 问题 | 修复摘要 | 工时 |
|----|------|------|------|---------|------|
| P0-01 | P0 | message.go | CmdHeartbeat=0 零值歧义 | 改为 CmdHeartbeat=6，新增 CmdNone=0 | 0.25h |
| P0-02 | P0 | client.go | Send() 静默丢消息 | 返回 ErrSendBufferFull / ErrClientClosed | 0.5h |
| P0-03 | P0 | router.go | 无消息去重 | 新增 DedupCache，key=fromUID:seq | 0.75h |
| P0-04 | P0 | hub.go+router.go | Hub Get+Send 竞态窗口 | Send 失败时降级离线存储 | 0.25h |
| P1-01 | P1 | snowflake.go | 时钟回拨死循环 | 回拨>10ms 返回 0 + 日志告警 | 0.25h |
| P1-02 | P1 | snowflake.go | WorkerID 静默截断 | New() 返回 error | 0.25h |
| P1-03 | P1 | snowflake.go | 缺少 ExtractTimestamp() | 新增包级函数 | 0.25h |
| P1-04 | P1 | config.go | Duration JSON 不可读 | 自定义 Duration 类型 Marshal/Unmarshal | 0.5h |
| P1-05 | P1 | config.go | 未用字段(WSAddr 等) | 删除 | 0.1h |
| P1-06 | P1 | config.go | Load() 静默忽略文件不存在 | 加 log 提示 | 0.1h |
| P1-07 | P1 | hub.go | Register() 静默关闭旧连接 | 先发 CmdKick 再关闭 | 0.25h |
| P1-08 | P1 | hub.go | StoreOffline() 静默截断 | 可配置上限 + 截断日志 | 0.25h |
| P1-09 | P1 | router.go | 无限流 | 令牌桶 per-UID | 0.5h |
| P1-10 | P1 | server.go | HandleLogin ParseForm 错误忽略 | 显式处理 error | 0.1h |
| P1-11 | P1 | server.go | CheckOrigin 全放行 | 可配置白名单 | 0.25h |
| P1-12 | P1 | server.go | 无 panic recovery | Recovery 中间件 | 0.25h |
| P1-13 | P1 | 全链路 | 无 context.Context 传播 | 延迟到 Phase 2 | 0h |
| P2-01 | P2 | message.go | 4 个死类型 | 删除 | 0.1h |
| P2-02 | P2 | 全项目 | 缺少单元测试 | 单独迭代 | 0h |

**覆盖统计**：原 next-steps 覆盖 10 个，本计划覆盖 17 个（P1-13 和 P2-02 计划延迟）。

---

## 2. P0 修复 (Correctness — 必须修)

### 2.1 P0-01: CmdHeartbeat = 0 零值歧义

**影响**：Go 中 int32 零值就是 0。客户端忘记设置 `cmd` 字段时默认为 0，服务端将其当作心跳处理，消息静默丢失。

**修复**：

```go
const (
    CmdNone      = 0 // sentinel: 未设置命令
    CmdChat      = 1 // 聊天消息 (不变)
    CmdAck       = 2 // 确认 (不变)
    CmdLogin     = 3 // 登录请求 (不变)
    CmdLoginResp = 4 // 登录响应 (不变)
    CmdOffline   = 5 // 请求离线消息 (不变)
    CmdHeartbeat = 6 // 心跳 (从 0 移到 6)
    CmdKick      = 7 // 被踢通知 (新增)
)
```

**兼容性**：CmdChat(1)~CmdOffline(5) 值不变，集成测试无影响。客户端需将心跳的 cmd 从 0 改为 6。

**风险**：低。编译期可发现所有引用。

---

### 2.2 P0-02: Client.Send() 静默丢消息

**影响**：send channel（容量 256）满时静默返回 nil。IM 的核心契约是"消息必达"，静默丢弃违反基本契约。

**修复**：

```go
var (
    ErrSendBufferFull = errors.New("send buffer full")
    ErrClientClosed   = errors.New("client closed")
)

func (c *Client) Send(msg proto.Message) error {
    data, err := json.Marshal(msg)
    if err != nil {
        return fmt.Errorf("marshal: %w", err)
    }
    select {
    case <-c.closed:
        return ErrClientClosed
    default:
    }
    select {
    case c.send <- data:
        return nil
    default:
        return ErrSendBufferFull
    }
}
```

**调用方影响**：Router.handleChat 收到 error 后降级存入离线队列。

**风险**：低。Send() 的原返回值 error 在所有调用方都被忽略（`client.Send(msg)`），现在变为有意义的返回值。

---

### 2.3 P0-03: 无消息去重

**影响**：客户端弱网超时重试时使用相同 `seq`，服务端当作新消息重复投递。接收方收到重复消息。

**修复**：新建 `internal/gateway/dedup.go`，DedupCache 基于 `"fromUID:seq"` → `msgID` 映射：

```go
type DedupCache struct {
    mu   sync.Mutex
    seen map[string]int64
}
```

- 仅 `seq > 0` 时触发（`seq == 0` 的消息不参与去重）
- 定期清理（5 分钟全量替换 map）
- 重复消息仅重发 ACK（携带已分配的 MsgID），不重复投递

**风险**：低。新文件，增量添加。

---

### 2.4 P0-04: Hub Get+Send 竞态窗口

**影响**：Alice 发消息 → `hub.Get("bob") != nil` → 准备投递 → Bob 断线 → 投递到已关闭连接 → 消息丢失。

**修复**：依赖 P0-02 的 Send() 返回 error。Router 在 `handleChat` 中处理 Send 失败：

```go
if target != nil {
    if err := target.Send(msg); err != nil {
        // 发送失败（连接可能刚好断开）→ 降级离线存储
        r.hub.StoreOffline(msg.To, msg)
    }
} else {
    r.hub.StoreOffline(msg.To, msg)
}
```

**风险**：低。增强了现有代码路径，不改变正常流程。

---

## 3. P1 修复 (Robustness — 应该修)

### 3.1 P1-01: Snowflake 时钟回拨死循环

**影响**：NTP 校时导致时钟回拨 5 秒 → `Next()` 空转 5 秒，服务完全不可用。

**修复**：

```go
const maxClockBackoff = 10 * time.Millisecond

if now < g.lastStamp {
    backoff := time.Duration(g.lastStamp-now) * time.Millisecond
    if backoff > maxClockBackoff {
        log.Printf("[snowflake] severe clock rollback: %v", backoff)
        return 0 // 返回 0，上层通过 MsgID==0 识别异常
    }
    // 小幅回拨 (<10ms)：等待恢复
    for now < g.lastStamp {
        time.Sleep(time.Microsecond * 100)
        now = time.Now().UnixMilli()
    }
}
```

**风险**：低。严重时钟回拨是小概率内核事件；发生时有明确日志，运维可感知。

---

### 3.2 P1-02: WorkerID 静默截断

**修复**：`New(workerID int64) (*Generator, error)`

越界时返回 `ErrWorkerIDInvalid`。仅 `main.go` 调用 `New()`，需同步更新错误处理。

---

### 3.3 P1-03: 缺少 ExtractTimestamp()

**修复**：

```go
func ExtractTimestamp(id int64) time.Time {
    return time.UnixMilli((id >> timestampShift) + Epoch)
}
```

包级函数，不依赖 Generator 实例。用于调试消息排序问题。

---

### 3.4 P1-04: Duration JSON 序列化不可读

**修复**：自定义 `Duration` 类型，实现 `MarshalJSON`/`UnmarshalJSON`，在 JSON 中表示为 `"30s"` 而非 `30000000000`。

影响 `GatewayConfig.Heartbeat` 和 `JWTConfig.Expiration`，main.go 中需 `time.Duration(cfg.JWT.Expiration)` 转换。

---

### 3.5 P1-05: 配置未使用字段

**删除**：`GatewayConfig.WSAddr`, `ReadTimeout`, `WriteTimeout`。被删除字段从未在任何代码路径中读取。

---

### 3.6 P1-06: Load() 静默忽略文件不存在

**修复**：文件不存在时加 `log.Printf("[config] no config file at %s, using defaults", path)`。

---

### 3.7 P1-07: Register() 无声踢旧连接

**修复**：覆盖前先发 `CmdKick` 消息：

```go
if old, ok := h.clients[c.UID]; ok {
    old.Send(proto.Message{
        Cmd:     proto.CmdKick,
        Content: "logged in from another device",
    })
    old.Close()
}
```

---

### 3.8 P1-08: StoreOffline() 静默截断

**修复**：可配置上限 + 截断日志：

```go
if len(h.offline[uid]) > h.offlineMaxSize {
    dropped := len(h.offline[uid]) - h.offlineMaxSize
    h.offline[uid] = h.offline[uid][dropped:]
    log.Printf("[hub] offline queue truncated for %s: dropped %d", uid, dropped)
}
```

`NewHub(offlineMaxSize int)` 替代 `NewHub()`。

---

### 3.9 P1-09: 无限流

**修复**：令牌桶算法 per-UID，在 `Router.handleChat` 入口处检查（在去重检查之后、消息分配 MsgID 之前）。RateLimiter 直接写在 router.go 内。

---

### 3.10 P1-10: HandleLogin ParseForm 错误忽略

**修复**：

```go
if err := r.ParseForm(); err != nil {
    http.Error(w, "invalid form data: "+err.Error(), http.StatusBadRequest)
    return
}
```

---

### 3.11 P1-11: CheckOrigin 全放行

**修复**：`Server` 持有一个 `allowedOrigins []string`，构建 Origin 白名单 checker。空列表 = 开发模式（全放行，维持当前行为）。

---

### 3.12 P1-12: 无 Panic Recovery

**修复**：`Recovery(next http.Handler) http.Handler` 中间件，defer recover，日志 + 500 响应。在 main.go 中包裹 mux。

---

### 3.13 P1-13: 无 Context 传播

**决策**：延迟到 Phase 2。

**理由**：Context 传播需要端到端修改调用链（HTTP Handler → Server → readPump → Router → Hub），触及所有文件。在 P0 修复和 P1 健壮性修复完成后再做，避免合并冲突。

---

### 3.14 P1-14: 硬编码常量覆盖配置

**修复**：在 B5 (client.go) 中一并处理。`pongWait`, `pingPeriod`, `maxMsgSize`, `sendBufSize` 改为从 `GatewayConnConfig` 读取。

---

## 4. P2 清理 (Hygiene)

### 4.1 P2-01: 死类型

**删除** `api/proto/message.go` 中 4 个从未使用的类型：
- `LoginRequest` — 客户端通过 query param 传 token
- `LoginResponse` — 已改用 proto.Message{Cmd: CmdLoginResp}
- `OfflineRequest` — 已改用 proto.Message{Cmd: CmdOffline}
- `OfflineMessages` — 离线消息逐条发送

Grep 确认：全项目仅 docs 和 CLAUDE.md 引用这 4 个类型名。

---

### 4.2 P2-02: 缺少单元测试

**决策**：不在本次迭代中做。单独一个 Step 来做单元测试（预计 3h），避免混在这次改动中增加变更范围。

---

## 5. 实施顺序（依赖图）

```
B1 (message.go) ──────────────────────────────────────────┐
B2 (snowflake) ───────────────────────────────────────────┤
B3 (config) ──────────────────────────────────────────────┤
B4 (jwt) ─────────────────────────────────────────────────┤
                                                           │
B5 (client) ─── 依赖 B3 (GatewayConnConfig) ──────────────┤
B6 (dedup.go) ─ 无依赖 ───────────────────────────────────┤
B7 (hub) ─────── 依赖 B1 (CmdKick 常量) ──────────────────┤
B8 (router) ─── 依赖 B1, B5, B6, B7 ──────────────────────┤
B9 (server) ─── 依赖 B3, B4, B5 ──────────────────────────┤
B10 (main.go) ─ 依赖 ALL ─────────────────────────────────┤
```

每步完成后运行 `go build ./...` 验证编译。B10 之后运行 `go test ./cmd/gateway/ -v`。

---

## 6. 变更影响矩阵

| 改动 | 影响文件 | 风险 | 向后兼容 |
|------|---------|------|---------|
| CmdHeartbeat=0→6 | message.go, router.go | 低 | CmdChat~CmdOffline 值不变 |
| 删除 4 死类型 | message.go | 极低 | 无引用 |
| Validate() 新增 | message.go | 极低 | 新方法，无调用方 |
| Snowflake New() 返回 error | snowflake.go, main.go | 低 | 仅 main.go 调用 |
| Duration 类型 | config.go, main.go, server.go | 中 | 需类型转换 |
| 删除未用字段 | config.go | 极低 | 无引用 |
| 新增 GatewayConnConfig | config.go, client.go, server.go, main.go | 中 | 新结构体，需传参 |
| Send() 返回真实 error | client.go, router.go | 中 | 签名不变，语义增强 |
| DedupCache 新文件 | dedup.go, router.go | 低 | 增量新增 |
| NewHub(offlineMaxSize) | hub.go, main.go | 低 | 仅 main.go 调用 |
| Register() 先 Kick 再 Close | hub.go | 低 | 仅影响多设备场景 |
| 令牌桶限流 | router.go | 低 | 增量新增 |
| CheckOrigin 白名单 | server.go, config.go | 低 | 空列表=全放行 |
| Recovery 中间件 | server.go, main.go | 低 | 增量新增 |
| NewApp() 返回 error | main.go, integration_test.go | 中 | 测试同步更新 |

---

## 7. 预估工时

| 类别 | 步骤 | 工时 |
|------|------|------|
| 文档 | A1-A3 | 3.0h |
| P0 修复 | B1, B5, B6, B7, B8 (部分) | 2.5h |
| P1 修复 | B2, B3, B4, B7, B8, B9 | 4.0h |
| P2 清理 | B1 (部分) | 0.1h |
| 总装 | B10 | 0.75h |
| 验证 | C | 1.0h |
| **合计** | | **~11.5h** |
