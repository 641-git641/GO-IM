# 02 — 代码审查

> 版本: v1.0 | 日期: 2026-07-15 | 范围: Phase 1 MVP 全部源码

---

## 审查总览

| 维度 | 评分 (1-5) | 说明 |
|------|-----------|------|
| 完整性 | ⭐⭐⭐ | 核心流程覆盖，但边界情况处理不足 |
| 扩展性 | ⭐⭐ | 缺乏接口抽象，组件间紧耦合 |
| 健壮性 | ⭐⭐ | 错误吞没、无超时控制、channel 满时静默丢消息 |
| 编码风格 | ⭐⭐⭐ | 整体清晰，但文档注释和常量组织可改进 |

---

## 1. 逐文件审查

### 1.1 `api/proto/message.go`

**问题 1: `CmdHeartbeat = 0` — 零值歧义**

```go
// ❌ 当前
CmdHeartbeat = 0 // heartbeat

// 在 Go 中，零值 int32 就是 0。客户端忘记设置 Cmd 时默认为 0，
// 服务端会将其当作心跳处理，造成混淆。
```

> **建议**: 将心跳命令改为非零值（如 `CmdHeartbeat = 1`），或引入显式的 `CmdNone = 0`。

**问题 2: 未使用的类型**

```go
// ❌ 以下类型定义后从未被引用
type LoginRequest struct { ... }    // 客户端未实现登录请求协议
type LoginResponse struct { ... }   // 改用 proto.Message{Cmd: CmdLoginResp} 发送
type OfflineRequest struct { ... }  // 客户端直接发 proto.Message{Cmd: CmdOffline}
type OfflineMessages struct { ... } // 离线消息逐条发送，不用此封装
```

> **建议**: 删除死代码，或标记为 `// TODO: Phase 2 使用`。

**问题 3: 缺少消息校验方法**

```go
// ❌ 当前：任何地方都要手动校验
if msg.Cmd == proto.CmdChat && msg.To == "" { ... }

// ✅ 建议
func (m *Message) Validate() error {
    switch m.Cmd {
    case CmdChat:
        if m.To == "" { return errors.New("chat: missing target") }
        if m.MsgType < MsgTypeText || m.MsgType > MsgTypeVideo { return errors.New("chat: invalid msg_type") }
    case CmdOffline:
        // offline requests need no extra fields
    }
    return nil
}
```

---

### 1.2 `internal/pkg/snowflake/snowflake.go`

**问题 1: 时钟回拨死循环**

```go
// ❌ 当前
if now < g.lastStamp {
    for now < g.lastStamp {
        time.Sleep(time.Microsecond * 100)
        now = time.Now().UnixMilli()
    }
}
// 如果时钟回拨 5 秒，这里会空转 5 秒，完全不可用
```

> **建议**:
> ```go
> const maxClockBackoff = 10 * time.Millisecond
> // 小幅度回拨 (< 10ms)：等待恢复
> // 大幅度回拨：直接 panic 或返回 error，触发上层告警
> ```

**问题 2: WorkerID 静默截断**

```go
// ❌ 当前：传入 9999 的 WorkerID 被静默修正，调用方无感知
if workerID < 0 || workerID > workerMax {
    workerID = workerID & workerMax
}
```

> **建议**: 返回 `(*Generator, error)`，对无效 WorkerID 显式报错。

**问题 3: 缺少 ID 反解能力**

```go
// ✅ 建议增加
func (g *Generator) ExtractTimestamp(id int64) time.Time {
    return time.UnixMilli((id >> timestampShift) + Epoch)
}
```

> 这在调试消息顺序问题时非常有用。

**问题 4: 缺少单元测试**

---

### 1.3 `internal/pkg/jwt/jwt.go`

**问题 1: 错误信息区分度不够**

```go
// ❌ 当前：所有错误归为两类
return nil, fmt.Errorf("parse token: %w", err)  // 过期/签名错误都是这个
return nil, fmt.Errorf("invalid token")           // Claims 类型错误

// 调用方无法区分"token 过期"和"token 被伪造"，而这两种情况处理方式不同：
// - 过期：提示用户重新登录
// - 签名错误：记录安全告警
```

> **建议**: 定义 sentinel errors
> ```go
> var (
>     ErrTokenExpired   = errors.New("token expired")
>     ErrTokenInvalid   = errors.New("token invalid")
>     ErrTokenSignature = errors.New("token signature mismatch")
> )
> ```

**问题 2: 缺少单元测试**

---

### 1.4 `configs/config.go`

**问题 1: `time.Duration` 的 JSON 序列化问题**

```go
// ❌ 当前结构体
type GatewayConfig struct {
    Heartbeat     time.Duration `json:"heartbeat"`
    HeartbeatFail int           `json:"heartbeat_fail"`
    // ...
}

// JSON 中的 "heartbeat": 30000000000 不可读
// 期望的是 "heartbeat": "30s"
```

> **建议**: 使用自定义类型或 string 格式
> ```go
> type Duration time.Duration
> func (d Duration) MarshalJSON() ([]byte, error) { ... }
> func (d *Duration) UnmarshalJSON(b []byte) error { ... }
> ```

**问题 2: 配置不使用时仍定义**

```go
// ❌ GatewayConfig.WSAddr、ReadTimeout、WriteTimeout 定义了但从未使用
WSAddr       string        `json:"ws_addr"`       // 未使用
ReadTimeout  time.Duration `json:"read_timeout"`   // 未使用
WriteTimeout time.Duration `json:"write_timeout"`  // 未使用
```

> **建议**: 删除未使用字段，使用时再加。避免给维护者假信号。

**问题 3: `Load()` 静默忽略文件不存在**

```go
// ❌ 如果运维写了配置文件但路径错了，这里静默使用默认值，难以排查
if os.IsNotExist(err) {
    return cfg, nil
}
```

> **建议**: 区分 `--config` 显式指定路径和默认路径；显式指定时文件不存在应报错。

---

### 1.5 `internal/gateway/hub.go`

**问题 1: `Register()` 覆盖旧连接时无通知**

```go
// ❌ 当前：旧连接被无提示关闭
if old, ok := h.clients[c.UID]; ok {
    old.Close()
}
```

> **影响**：用户 A 在手机和电脑同时登录同一账号，先登录的设备会静默断开。这在移动端是常见需求（多端互踢），但应该有明确的 Kick 通知（发送一条 `CmdKick` 消息告知被踢原因）。

**问题 2: `StoreOffline()` 的 FIFO 语义不明确**

```go
// ❌ 当前：超过 1000 条时截断前半部分
if len(h.offline[uid]) > 1000 {
    h.offline[uid] = h.offline[uid][len(h.offline[uid])-1000:]
}
```

> 1000 是硬编码常量，应配置化。且静默丢弃最旧消息无日志 — 运维无法感知队列溢出。

**问题 3: 离线消息和连接注册的竞态条件**

```
时序问题:
  1. Bob 断线 → Hub.Unregister("bob")
  2. Alice 发消息 → Router.handleChat → hub.Get("bob") == nil → StoreOffline("bob", msg)
  3. Bob 重连 → Hub.Register(bob) → Hub.DrainOffline("bob")
  4. Bob 收到消息 ✓ (没问题)

但如果:
  1. Bob 断线 → Hub.Unregister("bob")
  2. Bob 重连 → Hub.Register(bob)
  3. Alice 发消息 → hub.Get("bob") != nil → 直接投递 ✓
  
以及:
  1. Alice 发消息 → hub.Get("bob") != nil → 准备投递
  2. Bob 断线 → Hub.Unregister → Unregister 和 Get 之间有窗口
  3. 投递失败（连接已关闭），消息丢失！
```

> **建议**: 将 Hub 的业务操作放在锁内原子化，或引入消息队列保证 at-least-once。

**问题 4: `OnlineUsers()` 每次分配新切片**

```go
// ❌ 每次调用都分配内存，高并发下 GC 压力大
uids := make([]string, 0, len(h.clients))
```

> 如果只是统计在线人数，新增 `Count()` 方法即可（已有）；`OnlineUsers()` 只在低频的管理接口使用，可以接受。

---

### 1.6 `internal/gateway/client.go`

**问题 1: `Send()` 静默丢消息 — 最严重的问题**

```go
// ❌ 当前：channel 满时悄无声息地丢弃
select {
case c.send <- data:
    return nil
default:
    return nil // 丢了！调用者不知道！
}
```

> **影响**：当接收者消费速度 < 发送速度时，消息被静默丢弃。这对 IM 是不可接受的。
>
> **建议**：
> ```go
> var ErrSendBufferFull = errors.New("send buffer full")
> 
> func (c *Client) Send(msg proto.Message) error {
>     data, err := json.Marshal(msg)
>     if err != nil { return err }
>     select {
>     case c.send <- data:
>         return nil
>     case <-c.closed:
>         return ErrClientClosed
>     default:
>         return ErrSendBufferFull
>     }
> }
> ```

**问题 2: 硬编码的 Ping/Pong 参数覆盖了配置**

```go
// ❌ client.go 硬编码
const pongWait = 60 * time.Second
const pingPeriod = (pongWait * 9) / 10

// ❌ server.go 里的 heartbeat/heartFail 配置从未被使用
type Server struct {
    heartbeat time.Duration  // 有配置但未传到 Client
    heartFail int            // 有配置但未使用
}
```

> **建议**: 将 heartbeat 相关参数传入 NewClient，或使用配置结构体。

**问题 3: `readPump` 中消息发送者不可变是安全特性，但缺乏说明**

```go
// 当前行为: msg.From 被强制覆盖为认证身份
msg.From = c.UID
```

> 这是正确的安全设计（类似 SSH 的 `ForceCommand`），但需要注释说明这**不是 bug 而是 feature**，防止后续开发者"修复"。

**问题 4: `Close()` 不关闭 send channel**

```go
// ❌ 当前：close(c.closed) 后 writePump 退出，但 send channel 未关闭
// 如果有其他 goroutine 向 send 发送，会 panic（虽然目前没有）
```

> 当前实现因为 sync.Once 保护 Close 只执行一次，且 send 的生产者只有 readPump，readPump 退出后不会再写 send。但应加注释说明这个不变量。

---

### 1.7 `internal/gateway/router.go`

**问题 1: 消息无持久化 — 进程重启丢全量**

> 这是 MVP 阶段的设计选择（文档已说明），但需要在日志中明确标记 `[WARN] message not persisted`。

**问题 2: 无消息去重**

```go
// ❌ 当前：客户端重试 seq=1 的消息，会被当作新消息
// 缺少基于 seq+from 的去重缓存
```

> **建议**：在 Router 中维护 `map[string]int64` (key = `from:seq`)，收到重复 seq 仅重发 ACK，不重复投递。

**问题 3: 无发送频率限制**

> 缺少 per-user rate limiting，任何认证用户都可以无上限发送消息。这在恶意场景下可能导致 Hub 内存爆炸（发送 1 万条消息给 1 万不同不存在的用户 → 离线队列堆积）。

---

### 1.8 `internal/gateway/server.go`

**问题 1: `HandleLogin` 中 `ParseForm` 错误被忽略**

```go
// ❌
r.ParseForm()
uid := r.FormValue("uid")
```

> `ParseForm()` 可能返回错误（如 `Content-Type` 不正确或 body 过大），应处理。

**问题 2: `HandleWS` 中的 Upgrader CheckOrigin 过于宽松**

```go
CheckOrigin: func(r *http.Request) bool {
    return true // allow all origins for development
}
```

> 生产环境必须做 Origin 白名单校验，否则可被用于跨站 WebSocket 劫持。

**问题 3: 缺少 Panic Recovery 中间件**

```go
// ❌ 任何 handler 中若发生 panic，整个进程崩溃
// ✅ 建议: HTTP handler 外层加 recover
```

---

### 1.9 `cmd/gateway/main.go`

**问题 1: `/health` 端点中错误被忽略**

```go
data, _ := json.Marshal(...)  // 忽略错误
w.Write(data)                  // 可能写入 nil
```

> `json.Marshal` 对简单类型几乎不会出错，但结构体变更时可能引入问题。建议至少 log 一下。

**问题 2: `App.Run` 不返回 ListenAndServe 的**非** ErrServerClosed 错误**

> 当前代码正确处理了 `http.ErrServerClosed`，但 `ListenAndServe` 可能返回其他错误（如端口被占用）——这些应该被 propagate 出去。

---

## 2. 跨文件架构问题

### 2.1 零接口抽象

当前代码中没有定义任何 interface：

```
所有组件都是具体类型之间的直接依赖:
  Server → Hub (具体类型)
  Server → Router (具体类型)
  Router → Hub (具体类型)
  Client → Hub (具体类型)
```

> **影响**: 单元测试困难 — 测试 Router 必须创建真实的 Hub；无法替换实现（如将内存 Hub 切换为 Redis-backed Hub）。
>
> **建议**:
> ```go
> // internal/gateway/interfaces.go
> type ConnectionRegistry interface {
>     Get(uid string) *Client
>     IsOnline(uid string) bool
>     Register(c *Client)
>     Unregister(uid string)
> }
> 
> type OfflineStore interface {
>     Store(uid string, msg proto.Message)
>     Drain(uid string) []proto.Message
> }
> ```

### 2.2 无 Context 传播

```
调用链中没有任何 context.Context:
  HTTP Handler → Server.HandleWS → readPump → Router.Route → Hub.Get
  任何一步都没有超时控制或取消信号
```

> **影响**: 无法实现请求级别的超时控制；优雅关闭只能强制断开连接，无法等待正在处理的消息完成。
>
> **建议**: 将 `context.Context` 注入到关键路径中。

### 2.3 缺少结构化错误

> 当前使用 `log.Printf` + `fmt.Errorf`，无错误码、无调用栈、无 trace ID。随着系统复杂度增长，这些将严重阻碍问题排查。

---

## 3. 修复优先级矩阵

```
                    高影响              低影响
                ┌─────────────────┬─────────────────┐
  高频率        │ P0: Send() 静默   │ P1: 硬编码常量    │
  (每条消息)    │     丢消息        │     ParseForm 忽略 │
                │ P0: 无消息去重    │                  │
                ├─────────────────┼─────────────────┤
  低频率        │ P1: Hub 竞态条件   │ P2: 死代码清理    │
  (连接/启动)   │ P1: 时钟回拨      │ P2: 缺少注释      │
                │ P1: 无接口抽象    │ P2: Duration JSON │
                └─────────────────┴─────────────────┘
```

---

## 4. 编码风格建议

| 项目 | 当前 | 建议 |
|------|------|------|
| 包名 | `gateway`, `proto`, `jwt` | ✅ 符合 Go 惯例 |
| 导出符号 | `Message`, `Client`, `Hub` | ✅ 合理 |
| 文件组织 | 按功能拆分 | ✅ client.go / hub.go / router.go 职责清晰 |
| 注释 | 部分有 | 导出的 struct 和 method 应全部有 doc comment |
| 错误处理 | `if err != nil { return }` | ✅ 符合 Go 惯例，但部分位置吞没错误 |
| 测试 | 仅有集成测试 | 需要补充单元测试（snowflake、jwt、router） |

---

## 5. 测试覆盖缺口

| 包 | 测试文件 | 状态 |
|------|---------|------|
| `api/proto` | 无 | ❌ 需要：Message.Validate() 单元测试 |
| `internal/pkg/snowflake` | 无 | ❌ 需要：并发正确性、唯一性、单调性 |
| `internal/pkg/jwt` | 无 | ❌ 需要：签发/校验/过期/篡改 |
| `internal/gateway/hub` | 无 | ❌ 需要：注册/注销/离线存取 |
| `internal/gateway/router` | 无 | ❌ 需要：路由分发/ACK/离线处理 |
| `cmd/gateway` | integration_test.go | ✅ 端到端流程覆盖 |
