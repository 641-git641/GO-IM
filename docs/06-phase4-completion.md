# 06 — Phase 4 完成报告

> 版本: v1.0 | 日期: 2026-07-19 | 状态: ✅ 已完成 | 基于: [03-next-steps](./03-next-steps.md) + [05-phase2-completion](./05-phase2-completion.md)

---

Phase 4 在不改变 Phase 1-3 架构骨架的前提下，横向扩展了三个维度：**群聊**、**富媒体消息**（文件/图片）、**多网关水平扩展**。同时补充了未读计数、全文搜索、用户注册、运维稳定性等工程基础设施。

---

## 1. 群聊 (Group Chat)

### 1.1 核心组件

| 组件 | 文件 | 职责 |
|------|------|------|
| `GroupStore` | `internal/gateway/group_store.go` | 群组持久化接口：Create, AddMember, RemoveMember, GetMembers, IsMember, GetUserGroups, Get |
| `InMemoryGroupStore` | 同上 | 内存实现，雪花 ID 生成群 ID（`g_123456`），空群自动删除 |
| `Group` | 同上 | 数据模型：ID, Name, OwnerUID, Members map, CreatedAt |

### 1.2 HTTP API

| 端点 | 方法 | 说明 |
|------|------|------|
| `/group/create` | POST | 创建群组（uid + name），创建者自动成为群主+成员 |
| `/group/join` | POST | 加入群组（uid + group_id），`ErrAlreadyMember` / `ErrGroupNotFound` |
| `/group/leave` | POST | 退出群组（uid + group_id），最后一人退出时自动删群 |
| `/group/members` | GET | 查询群成员列表 |
| `/group/list` | GET | 查询用户所在的所有群（含成员数，不含成员详情） |

### 1.3 消息协议

- `ChatTypeGroup = 2`：protobuf `Message.chat_type` 区分单聊和群聊
- `Router.fanoutGroup()`：群消息扇出——在线成员实时推送，离线成员存离线队列，跨网关转发
- 发送者自己不会收到自己的群消息
- 群消息同样走去重 → 限流 → ACK → 异步持久化完整链路

### 1.4 待完成

- MySQL 持久化（重启后群组数据保留）
- WebSocket/TCP 协议支持群管理（当前仅 HTTP）
- 群消息历史查询
- 群通知消息（成员加入/退出）

---

## 2. 已读/未读追踪 (Read/Unread Tracking)

### 2.1 核心组件

| 组件 | 文件 | 职责 |
|------|------|------|
| `UnreadTracker` | `internal/gateway/unread_tracker.go` | 接口：Increment, MarkRead, GetCounts, GetCount |
| `InMemoryUnreadTracker` | 同上 | 内存实现，uid → {peerUID → count} 二层 map |

### 2.2 消息协议

- `CmdReadReceipt = 9`：客户端发送已读回执 → 清除未读计数 → 转发给原发送方
- `CmdUnreadCount = 10`：客户端请求未读计数 → 返回 `{"uid": "...", "counts": {"alice": 3, "bob": 1}}`

### 2.3 行为

- 消息投递时自动 Increment（单聊：对端+1；群聊：除发送者外所有成员+1）
- 自己给自己发消息不产生未读计数
- 已读回执是投递即忘的：对端不在线则丢弃
- HTTP `/unread?uid=alice` 直接查询

---

## 3. 对象存储与文件消息 (Object Storage & File Messages)

### 3.1 核心组件

| 组件 | 文件 | 职责 |
|------|------|------|
| `ObjectStore` | `internal/gateway/object_store.go` | 接口：Put, Get, Delete |
| `InMemoryObjectStore` | 同上 | 内存实现（开发/测试），数据拷贝隔离 |
| `MinioStore` | 同上 | MinIO/S3 实现，自动建 bucket，支持 Ping 健康检查 |

### 3.2 HTTP API

| 端点 | 方法 | 说明 |
|------|------|------|
| `/upload` | POST | multipart 上传（uid + token + file），自动检测 MIME，生成缩略图 |
| `/file` | GET | 下载（id + uid + token），`?thumb=1` 下载缩略图 |

### 3.3 图片缩略图

| 函数 | 文件 | 说明 |
|------|------|------|
| `Thumbnail()` | `internal/gateway/thumbnail.go` | CatmullRom 缩放，保持宽高比，200px 上限，JPEG 80% 质量输出 |
| `ImageDimensions()` | 同上 | 不解码全图获取宽高 |
| `IsImageMIME()` | 同上 | MIME 类型判断（排除 SVG） |

- 支持格式：JPEG, PNG, GIF, WebP（通过 `golang.org/x/image/webp`）
- 解压炸弹防御：超过 4096px 的图片跳过缩略图生成
- 上传时自动生成缩略图并存入 object store（`{fileID}_thumb` key）

### 3.4 待完成

- `CmdFile` 协议：通过 WebSocket/TCP 发送文件引用（file_id, name, size, mime）
- 文件消息出现在历史记录中
- 图片消息类型使用 `MsgTypeImage`（已定义）

---

## 4. 全文搜索 (Fulltext Search)

### 4.1 实现

- `CmdSearch = 11`：protobuf 协议命令
- `Router.handleSearch()`：JSON 参数解析，调用 `MessageStore.SearchMessages()`
- `repo.SearchParams`：Query, Peer, ChatType, MsgType, Before, After, Cursor, Limit
- `repo.SearchResult`：Messages, Count, NextCursor（游标分页）
- MySQL 实现：`LIKE` 匹配（可后续升级为 FULLTEXT 索引）
- HTTP API：`GET /search?uid=X&token=Y&q=hello&peer=Z&limit=20`

### 4.2 待完成

- Logic gRPC 服务接管搜索（router.go 中有 `// TODO` 标记）
- MySQL FULLTEXT 索引加速

---

## 5. 多网关水平扩展 (Multi-Gateway Horizontal Scaling)

### 5.1 核心组件

| 组件 | 文件 | 职责 |
|------|------|------|
| `HashRing` | `internal/gateway/hashring.go` | 一致性哈希环（CRC32, 150 虚拟节点），Add/Remove/Get |
| `Forwarder` | `internal/gateway/grpc_forwarder.go` | 跨网关转发接口 |
| `GrpcForwarder` | 同上 | gRPC 实现：懒连接，故障自动驱逐，双检锁 |
| `GrpcGatewayServer` | `internal/gateway/grpc_server.go` | 接收对端网关转发过来的消息，本地投递 |

### 5.2 协议

- `gateway.proto`：`ForwardRequest`（message + uid）+ `ForwardResponse`（delivered + error）
- `Gateway` gRPC service：`ForwardMessage` RPC

### 5.3 路由逻辑（`routeOrStoreOffline`）

```
目标不在本地
  ├── 无哈希环 → 存本地离线（单节点模式，向后兼容）
  ├── 有哈希环 + 本节点拥有目标 → 目标确实离线，存本地
  ├── 有哈希环 + 对端拥有目标 → gRPC 转发
  │   ├── 转发成功 delivered=true → 对端在线投递
  │   ├── 转发成功 delivered=false → 对端存离线
  │   └── 转发失败 → 降级本地离线存储
  └── 无 Forwarder → 降级本地离线
```

### 5.4 运维

- 配置：`grpc.addr`（本节点 gRPC 地址）+ `grpc.node_id` + `grpc.peer_addrs`
- 所有字段为空 = 单节点模式（默认）
- `/health` 不直接暴露 gRPC 状态（通过 MinIO/MySQL/Redis 依赖间接反映）

### 5.5 待完成

- 动态节点发现（通过 Redis 注册）
- 节点健康检查与自动摘除
- 哈希环变更时的离线消息迁移

---

## 6. 稳定性与可运维性 (Stability & Operability)

### 6.1 配置

```go
type StabilityConfig struct {
    MaxConnections   int      // 0 = 不限制
    HTTPReadTimeout  Duration // 默认 10s
    HTTPWriteTimeout Duration // 默认 10s
    HTTPIdleTimeout  Duration // 默认 120s
    ShutdownTimeout  Duration // 默认 30s
    PprofEnabled     bool     // 默认 false
    PprofAddr        string   // 默认 "localhost:6060"
}
```

### 6.2 改进

- **HTTP 超时**：`http.Server` 配置 ReadTimeout / WriteTimeout / IdleTimeout
- **优雅关闭**：HTTP → gnet TCP → gRPC forwarder → gRPC server → pprof → Kafka → Logic → Redis → MySQL 逐级关闭
- **pprof**：可选的 `net/http/pprof` 端点（默认关闭）
- **Panic Recovery**：`Recovery` 中间件包装所有 HTTP handler
- **持久化信号量**：`persistSem`（容量 64）限制并发持久化 goroutine，防止消息洪峰时 goroutine 爆炸

### 6.3 健康检查

`GET /health` 返回增强状态：

```json
{
  "status": "ok" | "degraded",
  "connections": 42,
  "dependencies": {
    "redis": "ok",
    "mysql": "ok",
    "kafka": "ok",
    "minio": "ok"
  },
  "memory": {
    "alloc_mb": 15,
    "goroutines": 87
  }
}
```

---

## 7. 用户注册 (User Registration)

- `POST /register`：uid + username + password，bcrypt 哈希存储
- `POST /login`：支持 password 登录路径（`bcrypt.CompareHashAndPassword`）
- `auth.dev_mode: true`（默认）：开发模式下跳过密码验证
- 重复注册检测：MySQL error 1062 → HTTP 409 Conflict
- `repo.UserStore` 接口：Create, GetByUID

---

## 8. 配置新增

### 8.1 新增配置块

| 配置块 | 关键字段 | 默认值 |
|--------|---------|--------|
| `stability` | max_connections, http_*_timeout, shutdown_timeout, pprof_* | 空/0 = 安全默认值 |
| `object_storage` | enabled, endpoint, access_key, secret_key, bucket, use_ssl, max_upload | enabled=false, bucket="im-files", max_upload=10MB |
| `grpc` | addr, node_id, peer_addrs | 全部为空 = 单节点模式 |
| `auth` | dev_mode | true（开发模式） |

---

## 9. 测试覆盖

**总计：183 个测试，全部通过，0 失败**

| 包 | 测试数 | 覆盖范围 |
|----|--------|---------|
| `cmd/gateway` | 5 | WebSocket + gnet TCP 集成测试 |
| `internal/gateway` | 137 | hub(9) + router(27) + gnet_handler(27) + redis_store(7) + grpc_client(2) + grpc_gateway(5) + hashring(9) + group_store(12) + unread_tracker(9) + object_store(6) + thumbnail(9) + router_read_receipt(11) + server(3) |
| `internal/logic` | 9 | gRPC server(3) + consumer(6) |
| `internal/mq` | 11 | producer(3) + consumer(8) |
| `internal/pkg/jwt` | 8 | sign/validate, expiry, tampering, malformed |
| `internal/pkg/snowflake` | 7 | uniqueness, monotonic, worker ID |
| `internal/repo` | 7 | MySQL CRUD + history + search（MySQL 不可用时自动跳过） |

### 9.1 Phase 4 新增测试详情

| 组件 | 测试数 | 关键场景 |
|------|--------|---------|
| GroupStore | 12 | 创建、加人（重复/不存在）、退群（非成员/最后一人删群）、查成员（不存在）、用户群列表、并发安全 |
| UnreadTracker | 9 | 增加、标记已读（幂等）、获取计数（空/非空）、自消息过滤、并发安全、清理 |
| ObjectStore | 6 | Put/Get、Get 不存在、Delete、数据隔离、大数据、并发 |
| Thumbnail | 9 | JPEG/PNG、小图无损、正方/竖/宽图、非法数据、尺寸检测、MIME 判断 |
| HashRing | 9 | 一致性、分布均匀、空环、单节点、增删、重复删除、重新加入、默认副本数、并发 |
| gRPC Gateway | 5 | 在线投递、离线存储、nil 消息、空 UID、缓冲区满 |
| Read Receipt (router) | 11 | 清除未读、对端离线、非法对端、跨网关转发、未读计数返回、群聊未读增量等 |

---

## 10. Phase 5 规划

详见 [03-next-steps.md](./03-next-steps.md)（已更新为 Phase 5 计划）。

| 优先级 | 任务 | 预计工时 |
|--------|------|---------|
| P0 | 群聊 WebSocket/TCP 协议 + 群消息历史 | 4h |
| P0 | `CmdFile` 文件消息协议 | 3h |
| P1 | MySQL GroupStore 持久化 | 3h |
| P1 | Logic 服务扩展（Search, Group, Unread gRPC） | 4h |
| P2 | 多网关动态集群（健康检查、服务发现） | 5h |
| P2 | 音视频信令（WebRTC signaling） | 3h |
