# IM 后端接口文档

> 版本：Phase 1-5 Complete | 最后更新：2026-07-20

---

## 目录

1. [概述](#1-概述)
2. [通用协议](#2-通用协议)
3. [HTTP 接口](#3-http-接口)
4. [WebSocket 协议](#4-websocket-协议)
5. [gnet TCP 协议](#5-gnet-tcp-协议)
6. [Protobuf Message 结构](#6-protobuf-message-结构)
7. [命令字参考](#7-命令字参考)
8. [典型交互流程](#8-典型交互流程)
9. [错误处理](#9-错误处理)
10. [附录：数据结构速查](#10-附录数据结构速查)

---

## 1. 概述

### 1.1 系统架构

```
┌─────────────────────────────────────────────────────────┐
│                        Frontend                         │
│  HTTP (login/upload/search)  +  WebSocket / TCP (IM)    │
└─────────────┬───────────────────────────────────────────┘
              │
    ┌─────────▼──────────┐
    │   Gateway :8080     │  HTTP + WebSocket + 业务逻辑
    │   Gateway :8081     │  gnet TCP (裸TCP, 可选)
    │   Gateway :50050    │  gRPC (多网关转发, 可选)
    └────────┬───────────┘
             │ gRPC (可选)
    ┌────────▼───────────┐
    │   Logic :50051      │  历史查询 / 群组管理 / 用户查询
    └────────┬───────────┘
             │
    ┌────────▼───────────┐
    │   MySQL / Kafka     │  消息持久化
    └────────────────────┘
```

### 1.2 端口与传输方式

| 端口 | 协议 | 用途 | 是否必需 |
|------|------|------|----------|
| `:8080` | HTTP | REST API（登录/注册/上传/搜索/群组/健康检查） | ✅ 必需 |
| `:8080` | WebSocket | 实时 IM 通信 | 默认开启 |
| `:8081` | gnet TCP | 裸 TCP 实时 IM 通信 | 可选（配置 `transport: "gnet"` 或 `"both"`） |
| `:50050` | gRPC | 跨网关节点转发 | 可选（多网关集群） |
| `:50051` | gRPC | Gateway → Logic 内部调用 | 可选（启用 MySQL 时推荐） |

### 1.3 开发模式 vs 生产模式

| 项目 | 开发模式 (`dev_mode: true`) | 生产模式 (`dev_mode: false`) |
|------|---------------------------|------------------------------|
| 登录 | `uid` + `username` 即可 | 需要 `uid` + `password`（bcrypt） |
| 注册 | 不需要 | 需要 `uid` + `username` + `password` |
| 消息存储 | 内存（重启丢失） | MySQL + Kafka 持久化 |
| 文件存储 | 内存 | MinIO S3 |

---

## 2. 通用协议

### 2.1 认证方式

所有需要认证的接口使用 **JWT HS256** Token：

```
Authorization: 不通过 HTTP Header，而是通过请求参数或消息体传递 token
```

Token 有效期：**7 天**（可配置）。

### 2.2 时间格式

所有时间戳使用 **Unix 毫秒**（13 位整数）。

### 2.3 ID 格式

| ID 类型 | 格式 | 示例 |
|---------|------|------|
| 用户 ID (UID) | 字符串 | `"alice"`, `"user_123"` |
| 消息 ID (MsgId) | 64 位 Snowflake 整数 | `337501598508912640` |
| 群组 ID | `g_` + Snowflake 整数 | `"g_337501598508912641"` |
| 文件 ID | Snowflake 整数字符串 | `"337501598508912642"` |

### 2.4 Snowflake 消息 ID

- 全局唯一，时间有序
- 可用于排序和分页游标
- 同一毫秒内的消息顺序由序列号保证

---

## 3. HTTP 接口

### 3.1 用户认证

#### POST /login — 登录

获取 JWT Token。

**请求**
```
POST /login
Content-Type: application/x-www-form-urlencoded

uid=alice&username=Alice&password=secret123
```

| 参数 | 必需 | 说明 |
|------|------|------|
| `uid` | 是 | 用户唯一标识 |
| `username` | 否 | 显示名称（dev_mode 时可为空，默认取 uid） |
| `password` | 否 | 密码（dev_mode 时不需要；生产模式必需） |

**成功响应** `200`
```json
{
  "uid": "alice",
  "username": "Alice",
  "token": "eyJhbGciOiJIUzI1NiIs..."
}
```

**错误响应**
| 状态码 | 说明 |
|--------|------|
| 400 | uid 缺失或表单解析失败 |
| 401 | 用户不存在 / 密码错误 |
| 503 | 用户存储不可用（MySQL 未启用） |

---

#### POST /register — 注册

**仅生产模式可用**（需要 MySQL）。dev_mode 下可跳过此步骤直接 login。

**请求**
```
POST /register
Content-Type: application/x-www-form-urlencoded

uid=alice&username=Alice&password=secret123
```

| 参数 | 必需 | 说明 |
|------|------|------|
| `uid` | 是 | 用户唯一标识 |
| `username` | 否 | 显示名称（默认等于 uid） |
| `password` | 是 | 密码（bcrypt 哈希存储） |

**成功响应** `200`
```json
{
  "uid": "alice",
  "username": "Alice",
  "token": "eyJhbGciOiJIUzI1NiIs..."
}
```

**错误响应**
| 状态码 | 说明 |
|--------|------|
| 400 | uid 或 password 缺失 |
| 409 | uid 已被注册 |
| 503 | 用户存储不可用 |

---

### 3.2 用户与状态

#### GET /online — 在线用户

**请求**
```
GET /online
```

**响应** `200`
```json
{
  "count": 3,
  "users": ["alice", "bob", "charlie"]
}
```

---

#### GET /health — 健康检查

**请求**
```
GET /health
```

**响应** `200`
```json
{
  "status": "ok",
  "connections": 42,
  "dependencies": {
    "mysql": "ok",
    "redis": "ok",
    "minio": "ok"
  },
  "memory": {
    "alloc_mb": 16,
    "goroutines": 128
  }
}
```

`status` 可能值：`"ok"` / `"degraded"`（有依赖不健康）。

---

### 3.3 群组管理

#### POST /group/create — 创建群组

**请求**
```
POST /group/create
Content-Type: application/x-www-form-urlencoded

uid=alice&name=Dev%20Team
```

| 参数 | 必需 | 说明 |
|------|------|------|
| `uid` | 是 | 创建者 UID（自动成为群主） |
| `name` | 是 | 群组名称 |

**响应** `200`
```json
{
  "group_id": "g_337501598508912641",
  "name": "Dev Team",
  "owner_uid": "alice",
  "members": ["alice"],
  "created_at": 1752994800000
}
```

---

#### POST /group/join — 加入群组

**请求**
```
POST /group/join
Content-Type: application/x-www-form-urlencoded

uid=bob&group_id=g_337501598508912641
```

| 参数 | 必需 | 说明 |
|------|------|------|
| `uid` | 是 | 加入者 UID |
| `group_id` | 是 | 群组 ID |

**响应** `200`
```json
{ "ok": "true" }
```

**错误**
| 状态码 | 说明 |
|--------|------|
| 404 | 群组不存在 |
| 409 | 已经是成员 |

---

#### POST /group/leave — 退出群组

**请求**
```
POST /group/leave
Content-Type: application/x-www-form-urlencoded

uid=bob&group_id=g_337501598508912641
```

| 参数 | 必需 | 说明 |
|------|------|------|
| `uid` | 是 | 退出者 UID |
| `group_id` | 是 | 群组 ID |

**响应** `200`
```json
{ "ok": "true" }
```

> **注意**：最后一人退出时群组自动删除。

**错误**
| 状态码 | 说明 |
|--------|------|
| 404 | 群组不存在 |
| 409 | 不是成员 |

---

#### GET /group/members — 获取群成员

**请求**
```
GET /group/members?group_id=g_337501598508912641
```

**响应** `200`
```json
{
  "group_id": "g_337501598508912641",
  "members": ["alice", "bob"]
}
```

---

#### GET /group/list — 获取用户的所有群组

**请求**
```
GET /group/list?uid=alice
```

**响应** `200`
```json
{
  "groups": [
    {
      "id": "g_337501598508912641",
      "name": "Dev Team",
      "owner_uid": "alice",
      "member_count": 2,
      "created_at": 1752994800000
    }
  ]
}
```

---

### 3.4 文件上传与下载

#### POST /upload — 上传文件

**请求**
```
POST /upload
Content-Type: multipart/form-data

uid=alice&token=eyJhbG...&file=@photo.jpg
```

| 字段 | 必需 | 说明 |
|------|------|------|
| `uid` | 是 | 上传者 UID |
| `token` | 是 | JWT Token |
| `file` | 是 | 文件数据（最大 10MB 可配置） |

**响应** `200`
```json
{
  "file_id": "337501598508912642",
  "name": "photo.jpg",
  "size": 245760,
  "mime": "image/jpeg",
  "width": 1920,
  "height": 1080,
  "thumb_width": 200,
  "thumb_height": 113
}
```

> **缩略图**：图片自动生成 200px 缩略图。超过 4096px 的图片不生成缩略图（防解压炸弹）。支持 JPEG / PNG / GIF / WebP。

**错误**
| 状态码 | 说明 |
|--------|------|
| 400 | 缺少 uid/token/file，或文件为空 |
| 401 | Token 无效 |
| 403 | uid 与 token 不匹配 |
| 413 | 文件过大 |
| 503 | 对象存储不可用 |

---

#### GET /file — 下载文件/缩略图

**请求**
```
GET /file?id=337501598508912642&uid=alice&token=eyJhbG...
GET /file?id=337501598508912642&thumb=1&uid=alice&token=eyJhbG...
```

| 参数 | 必需 | 说明 |
|------|------|------|
| `id` | 是 | 文件 ID |
| `uid` | 是 | 请求者 UID |
| `token` | 是 | JWT Token |
| `thumb` | 否 | `1` = 下载缩略图（仅图片） |

**响应**：直接返回文件二进制数据，`Content-Type` 为原始 MIME 类型。

**错误**
| 状态码 | 说明 |
|--------|------|
| 400 | 缺少 id/uid/token |
| 401 | Token 无效 |
| 403 | uid 与 token 不匹配 |
| 404 | 文件不存在 |
| 503 | 对象存储不可用 |

---

### 3.5 搜索

#### GET /search — 全文搜索消息

**请求**
```
GET /search?uid=alice&token=eyJhbG...&q=hello&peer=bob&chat_type=1&limit=20
```

| 参数 | 必需 | 说明 |
|------|------|------|
| `uid` | 是 | 搜索者 UID |
| `token` | 是 | JWT Token |
| `q` | 是 | 搜索关键词 |
| `peer` | 否 | 限定与某人的对话 |
| `chat_type` | 否 | `1`=单聊 `2`=群聊 |
| `msg_type` | 否 | `1`=文本 `2`=图片 `3`=语音 `4`=视频 `5`=文件 |
| `before` | 否 | 时间戳上限（毫秒） |
| `after` | 否 | 时间戳下限（毫秒） |
| `cursor` | 否 | 分页游标（上一页的 `next_cursor`） |
| `limit` | 否 | 每页条数（默认 20，最大 50） |

**响应** `200`
```json
{
  "query": "hello",
  "messages": [
    {
      "msg_id": 337501598508912640,
      "cmd": 1,
      "from": "alice",
      "to": "bob",
      "chat_type": 1,
      "msg_type": 1,
      "content": "Hello World!",
      "timestamp": 1752994800000,
      "need_ack": false
    }
  ],
  "total": 1,
  "next_cursor": 0
}
```

> - 仅返回搜索者参与的消息（`from_uid` 或 `to_uid` 匹配）
> - `next_cursor == 0` 表示没有更多结果
> - 游标分页：将 `next_cursor` 作为 `cursor` 参数请求下一页

---

### 3.6 未读计数

#### GET /unread — 获取未读计数

**请求**
```
GET /unread?uid=alice
```

**响应** `200`
```json
{
  "uid": "alice",
  "counts": {
    "bob": 3,
    "charlie": 1
  }
}
```

> Key 为 peer UID（谁发给我的），Value 为未读条数。

---

## 4. WebSocket 协议

### 4.1 建立连接

```
ws://localhost:8080/ws?token=eyJhbGciOiJIUzI1NiIs...
         或
wss://your-domain/ws?token=eyJhbGciOiJIUzI1NiIs...
```

**步骤**
1. 先通过 `POST /login` 获取 `token`
2. 使用 token 发起 WebSocket 升级请求
3. 服务端验证 token，返回 `CmdLoginResp` 消息
4. 连接就绪，可以发送和接收消息

**连接成功后收到的第一条消息**
```json
// Protobuf 解码后:
{
  "cmd": 4,
  "to": "alice",
  "content": "Alice",
  "timestamp": 1752994800000
}
```
其中 `CmdLoginResp = 4`，`content` 为你的用户名。

### 4.2 数据格式

- **编码**：Protobuf 二进制（`proto.Message`）
- **不是** JSON 或文本

详见 [§6 Protobuf Message 结构](#6-protobuf-message-结构)。

### 4.3 心跳

WebSocket 层使用标准 **Ping/Pong** 帧保活：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| Ping 周期 | 54s | 服务端→客户端 |
| Pong 等待 | 60s | 超时断开 |
| 应用层心跳 | 自动处理 | 无需发送 `CmdHeartbeat` |

> WebSocket Ping/Pong 由浏览器/WS 库自动处理，**前端无需额外编码**。

### 4.4 连接保活参数

| 参数 | 默认值 |
|------|--------|
| 消息最大长度 | 65536 字节（64 KiB） |
| 发送缓冲区 | 256 条消息 |
| 写超时 | 10 秒 |

---

## 5. gnet TCP 协议

当配置 `transport: "gnet"` 或 `"both"` 时可用。

### 5.1 连接与帧格式

```
帧格式: [4字节 Big-Endian 长度] + [Protobuf 数据]
```

```
┌──────────────────┬─────────────────────────────┐
│  4 bytes (BE)    │      N bytes                 │
│  Payload Length  │  Protobuf Message            │
└──────────────────┴─────────────────────────────┘
```

### 5.2 登录（首条消息）

TCP 连接建立后，**第一条消息必须是登录**：

```json
// proto.Message (CmdLogin=3)
{
  "cmd": 3,
  "content": "eyJhbGciOiJIUzI1NiIs..."  // JWT Token
}
```

登录成功后，服务端返回 `CmdLoginResp`（cmd=4），之后即可正常通信。

### 5.3 心跳

与 WebSocket 不同，gnet TCP 使用**应用层心跳**：

发送：
```json
{ "cmd": 6 }
```

接收：
```json
{
  "cmd": 6,
  "msg_id": 337501598508912640,
  "timestamp": 1752994800000
}
```

> 心跳间隔默认 30s，连续失败 3 次断开连接。

---

## 6. Protobuf Message 结构

### 6.1 字段定义

所有 WebSocket 和 TCP 通信使用同一 `Message` 结构：

| 字段 | 类型 | 编号 | 说明 |
|------|------|------|------|
| `seq` | int64 | 1 | 客户端消息序列号（用于去重和 ACK 关联） |
| `msg_id` | int64 | 2 | 服务端分配的全局唯一 ID（Snowflake） |
| `cmd` | int32 | 3 | **命令字**，决定消息类型 |
| `from` | string | 4 | **发送者 UID**（服务端覆写，不可伪造） |
| `to` | string | 5 | 接收者 UID 或群组 ID |
| `chat_type` | int32 | 6 | 聊天类型：1=单聊, 2=群聊 |
| `msg_type` | int32 | 7 | 消息类型：1=文本, 2=图片, 3=语音, 4=视频, 5=文件 |
| `content` | string | 8 | 消息内容（文本 / JSON 字符串） |
| `timestamp` | int64 | 9 | Unix 毫秒时间戳 |
| `need_ack` | bool | 10 | 是否需要服务端 ACK 确认 |

### 6.2 字段使用场景表

| 命令 | 客户端设置 | 服务端设置 | 特别说明 |
|------|-----------|-----------|---------|
| CmdChat | `to`, `chat_type`, `msg_type`, `content`, `seq`, `need_ack` | `msg_id`, `from`, `timestamp` | `from` 被服务端覆写 |
| CmdAck | — | `msg_id`, `seq`, `timestamp` | 服务端生成 |
| CmdHeartbeat | — | `msg_id`, `timestamp` | WebSocket用Ping/Pong，TCP才需此命令 |
| CmdHistory | `to`, `seq`(=limit), `timestamp`(=before), `chat_type` | — | 服务端返回多条消息+一条完成信号 |
| CmdOffline | — | — | 拉取离线消息 |
| CmdReadReceipt | `to`(=对方UID) | — | `seq` 可携带已读的最后一条 msg_id |
| CmdUnreadCount | — | — | 服务端返回 JSON |
| CmdSearch | `content`(JSON) | — | 服务端返回多条消息+完成信号 |
| CmdGroupCreate | `content`=`{"name":"..."}` | — | |
| CmdGroupJoin | `to`=group_id | — | |
| CmdGroupLeave | `to`=group_id | — | |
| CmdGroupInfo | `to`=group_id | — | |
| CmdGroupList | — | — | |
| CmdFile | 同 CmdChat，`content` 为文件元数据 JSON | 同 CmdChat | `msg_type` 建议为 2/3/4/5 |
| CmdRecall | `to`, `seq`(=原始MsgId) | `msg_id`, `timestamp` | 撤回通知，`seq` 携带原始消息ID |

### 6.3 多平台注意事项

如果你使用 **JavaScript/TypeScript** 客户端（Web/React Native），需要：

1. 使用 `protobufjs` 或 `@bufbuild/protobuf` 进行 Protobuf 编解码
2. 大整数（`int64`）在 JS 中可能丢失精度——`msg_id` 和 `seq` 建议以字符串形式处理
3. WebSocket 使用 Binary 帧（不是 Text 帧）

---

## 7. 命令字参考

| Cmd | 常量 | 值 | 方向 | 说明 |
|-----|------|---|------|------|
| — | `CmdNone` | 0 | — | 未初始化（不应出现） |
| 🟢 | `CmdChat` | 1 | C→S, S→C | 聊天消息（文本/图片/语音/视频/文件） |
| 🔵 | `CmdAck` | 2 | S→C | 服务端确认（消息已收到） |
| 🟠 | `CmdLogin` | 3 | C→S | TCP 登录（发送 JWT） |
| 🟠 | `CmdLoginResp` | 4 | S→C | 登录成功响应 |
| 🟡 | `CmdOffline` | 5 | C→S | 拉取离线消息 |
| 🟡 | `CmdHeartbeat` | 6 | C→S, S→C | 应用层心跳（仅 TCP 需要） |
| 🔴 | `CmdKick` | 7 | S→C | 被踢下线（多设备互踢） |
| 🟢 | `CmdHistory` | 8 | C→S | 请求聊天记录 |
| 🟢 | `CmdReadReceipt` | 9 | C→S, S→C | 已读回执 |
| 🟢 | `CmdUnreadCount` | 10 | C→S, S→C | 未读计数查询 |
| 🟢 | `CmdSearch` | 11 | C→S, S→C | 全文搜索 |
| 🟣 | `CmdGroupCreate` | 12 | C→S, S→C | 创建群组 |
| 🟣 | `CmdGroupJoin` | 13 | C→S, S→C | 加入群组 |
| 🟣 | `CmdGroupLeave` | 14 | C→S, S→C | 退出群组 |
| 🟣 | `CmdGroupInfo` | 15 | C→S, S→C | 群组信息 |
| 🟣 | `CmdGroupList` | 16 | C→S, S→C | 群组列表 |
| 🟢 | `CmdFile` | 17 | C→S, S→C | 文件消息（经过完整聊天管线） |
| 🔴 | `CmdRecall` | 19 | C→S, S→C | 消息撤回 |

---

## 8. 典型交互流程

### 8.1 登录 + 连接

```
Client                          Server
  │                                │
  │── POST /login ────────────────▶│ 返回 JWT token
  │◀─ {uid, username, token} ─────│
  │                                │
  │── ws://host/ws?token=xxx ─────▶│ WebSocket 升级
  │◀─ CmdLoginResp ───────────────│ 登录确认
  │                                │
```

### 8.2 发送单聊消息（含 ACK）

```
Alice                            Server                            Bob
  │                                │                                │
  │── CmdChat ────────────────────▶│                                │
  │   to="bob", seq=1, need_ack   │                                │
  │                                │── CmdChat ────────────────────▶│
  │◀─ CmdAck ─────────────────────│   (推送给 Bob)                  │
  │   seq=1, msg_id=337501...     │                                │
```

### 8.3 离线消息流程

```
Alice                            Server                            Bob
  │                                │                                │
  │── CmdChat to="bob" ──────────▶│  Bob 不在线                     │
  │◀─ CmdAck ─────────────────────│  存入离线队列                    │
  │                                │                                │
  │                     ... Bob 稍后上线 ...                         │
  │                                │◀── CmdOffline ────────────────│
  │                                │── 推送离线消息 ───────────────▶│
  │                                │── CmdOffline(完成) ──────────▶│
```

### 8.4 群聊消息

```
Alice                            Server                            Bob, Charlie...
  │                                │                                │
  │── CmdChat ────────────────────▶│                                │
  │   to="g_xxx", chat_type=2     │                                │
  │                                │── CmdChat ────────────────────▶│ Bob (在线)
  │                                │── CmdChat ────────────────────▶│ Charlie (在线)
  │                                │── 存入离线队列 ───────────────▶│ Dave (离线)
  │◀─ CmdAck ─────────────────────│                                │
  │                                │                                │
  │◀─ CmdChat (member_joined) ───│  群通知（有人加群/退群时）       │
```

### 8.5 消息历史

```
Client                            Server
  │                                │
  │── CmdHistory ─────────────────▶│
  │   to="bob", seq=30(limit)     │
  │   timestamp=1752994800000      │ (before 此时间)
  │                                │
  │◀─ CmdChat (msg 1) ────────────│  历史消息 1
  │◀─ CmdChat (msg 2) ────────────│  历史消息 2
  │◀─ CmdChat (msg 3) ────────────│  历史消息 3
  │◀─ CmdHistory ─────────────────│  完成信号 (seq=3=已发送数)
```

- `seq` 字段 → `limit`（每页条数，默认 30，最大 100）
- `timestamp` 字段 → `before`（只返回早于此时间的消息）
- 结果按时间倒序（最新在前）
- 返回完所有消息后发一条 `CmdHistory` 消息，其 `seq` = 实际发送条数
- 群聊历史需设置 `chat_type = 2`

### 8.6 已读回执 + 未读计数

```
Bob                              Server                            Alice
  │                                │                                │
  │  Bob 看到 Alice 发来的消息      │                                │
  │── CmdReadReceipt ─────────────▶│                                │
  │   to="alice"                   │                                │
  │                                │── CmdReadReceipt ─────────────▶│
  │                                │   from="bob", to="alice"       │
  │                                │   (通知 Alice: 消息已被 Bob 读了)│
  │                                │                                │
  │── CmdUnreadCount ─────────────▶│                                │
  │◀─ CmdUnreadCount ─────────────│                                │
  │   content={"uid":"bob",       │                                │
  │    "counts":{"alice":3}}       │                                │
```

> 也可通过 HTTP 接口获取未读数：`GET /unread?uid=bob`

### 8.7 消息撤回

```
Alice                            Server                            Bob
  │                                │                                │
  │── CmdRecall ──────────────────▶│                                │
  │   to="bob", seq=337501...     │  (seq=原始消息的 msg_id)        │
  │                                │  检查: sender==原始发送者       │
  │                                │  检查: 2分钟窗口内             │
  │                                │  MySQL标记 recalled=1          │
  │                                │── CmdRecall ──────────────────▶│
  │                                │   seq=337501...               │
  │                                │   content={"recalled":true,   │
  │                                │    "msg_id":337501...}         │
```

**撤回通知格式**：
```json
{
  "cmd": 19,
  "from": "alice",
  "to": "bob",
  "seq": 337501598508912640,
  "msg_id": 337501598508912641,
  "content": "{\"recalled\":true,\"msg_id\":337501598508912640}",
  "timestamp": 1752994800000
}
```

**撤回规则**：
- 仅消息发送者可撤回
- 时间窗口：**2 分钟**（发送后 120 秒内）
- 撤回后，历史查询返回的 `content` 为 `{"recalled":true}`
- 不支持群聊撤回（本轮仅单聊）
- 不生成 ACK，不支持撤回已撤回的消息

**撤回失败时的错误响应**（仅发给撤回者）：
```json
{
  "cmd": 19,
  "to": "alice",
  "content": "{\"error\":\"message not found or not owned by alice\"}",
  "msg_id": 337501598508912642,
  "timestamp": 1752994800000
}
```

### 8.8 群组管理（WebSocket/TCP 方式）

群组管理支持两种方式：HTTP API（见 §3.3）和 WebSocket/TCP 消息（本节）。两种方式功能等价。

#### 创建群组

```
Client → Server   Cmd=12, content={"name":"Dev Team"}
Server → Client   Cmd=12, content={"id":"g_xxx","name":"Dev Team","owner_uid":"alice",...}
```

#### 加入群组

```
Client → Server   Cmd=13, to="g_xxx"
Server → Client   Cmd=13, content={"group_id":"g_xxx","uid":"bob","members":[...]}
Server → 群成员    Cmd=1, chat_type=2, content={"type":"member_joined","group_id":"g_xxx","uid":"bob"}
```

#### 退出群组

```
Client → Server   Cmd=14, to="g_xxx"
Server → Client   Cmd=14, content={"group_id":"g_xxx","uid":"bob","deleted":false}
Server → 群成员    Cmd=1, chat_type=2, content={"type":"member_left","group_id":"g_xxx","uid":"bob"}
```

#### 查看群信息

```
Client → Server   Cmd=15, to="g_xxx"
Server → Client   Cmd=15, content={"id":"g_xxx","name":"Dev Team","owner_uid":"alice","members":[...],"created_at":123}
```

#### 查看我的群组

```
Client → Server   Cmd=16
Server → Client   Cmd=16, content={"uid":"alice","groups":[{"id":"g_xxx","name":"...","owner_uid":"...","member_count":2,"created_at":123}]}
```

---

## 9. 错误处理

### 9.1 HTTP 通用错误码

| 状态码 | 含义 |
|--------|------|
| 200 | 成功 |
| 400 | 请求参数错误 |
| 401 | 未认证（Token 无效或过期） |
| 403 | 无权限（uid 与 token 不匹配） |
| 404 | 资源不存在 |
| 409 | 冲突（重复注册、重复加群等） |
| 413 | 请求体过大 |
| 429 | 请求过于频繁（触发限流） |
| 500 | 服务端内部错误 |
| 503 | 服务不可用（依赖未就绪） |

### 9.2 WebSocket/TCP 错误处理

- **格式错误**（Protobuf 解包失败）：消息被丢弃，连接保持
- **校验失败**（缺少必填字段）：消息被丢弃，无响应
- **限流触发**：消息被丢弃，连接保持
- **连接断开**：客户端应自动重连
  - WebSocket：监听 `onclose` 事件
  - TCP：监听连接断开
- **Kick**（Cmd=7）：被踢下线（多设备登录），收到后应断开连接并不重连

### 9.3 重连建议

```
┌─────────────────────────────────────────────────┐
│  1. 检测断线                                     │
│  2. 等待 1-3 秒                                  │
│  3. 重新 POST /login 获取新 Token（如果旧Token过期）│
│  4. 重新建立 WebSocket/TCP 连接                    │
│  5. 发送 CmdOffline(5) 拉取错过消息                │
│  6. 可选: CmdUnreadCount(10) 或 GET /unread       │
│  7. 可选: CmdHistory(8) 拉取最近历史               │
└─────────────────────────────────────────────────┘
```

---

## 10. 附录：数据结构速查

### 10.1 ChatType（聊天类型）

| 值 | 含义 |
|----|------|
| 1 | 单聊 |
| 2 | 群聊 |

### 10.2 MsgType（消息类型）

| 值 | 含义 | content 格式 |
|----|------|-------------|
| 1 | 文本 | 纯文本字符串 |
| 2 | 图片 | JSON: `{"file_id":"xxx","width":1920,"height":1080,"text":"..."}` |
| 3 | 语音 | JSON: `{"file_id":"xxx","duration":15,"text":"..."}` |
| 4 | 视频 | JSON: `{"file_id":"xxx","width":1920,"height":1080,"duration":30,"text":"..."}` |
| 5 | 文件 | JSON: `{"file_id":"xxx","name":"doc.pdf","size":1024000}` |

### 10.3 文件消息 content JSON

```json
{
  "file_id": "337501598508912642",
  "name": "photo.jpg",
  "size": 245760,
  "mime": "image/jpeg",
  "width": 1920,
  "height": 1080
}
```

> 建议：上传文件后，前端将返回的 JSON 加上 `text`（可选附言）作为 `CmdFile` 的 `content` 发送。

### 10.4 群通知 JSON

```json
// 有人加群
{
  "type": "member_joined",
  "group_id": "g_337501598508912641",
  "uid": "bob"
}

// 有人退群
{
  "type": "member_left",
  "group_id": "g_337501598508912641",
  "uid": "bob"
}
```

> 群通知以 `CmdChat` (cmd=1) + `chat_type=2`（群聊）方式投递，`msg_type=1`（文本）。前端应检查 `content` 是否为 JSON 且含 `type` 字段来判断是否是系统通知。

### 10.5 搜索 JSON（WebSocket 方式）

```
CmdSearch (11) + content={"q":"hello","peer":"bob","limit":20}
```

参数同 HTTP 搜索接口（见 §3.5），但通过 `content` JSON 传递。

### 10.6 已撤回消息

历史查询/搜索返回的已撤回消息：
```json
{
  "cmd": 1,
  "msg_id": 337501598508912640,
  "content": "{\"recalled\":true}"
}
```

---

## 附录 A：快速上手检查清单

- [ ] `POST /login` → 获取 token
- [ ] WebSocket 连接 `/ws?token=xxx`
- [ ] 收到 `CmdLoginResp`(4) → 连接成功
- [ ] 发送 `CmdChat`(1) → 测试消息收发
- [ ] 收到 `CmdAck`(2) → 消息已送达
- [ ] 断线重连 → `CmdOffline`(5) 拉取离线消息
- [ ] `CmdHistory`(8) → 加载聊天记录
- [ ] `POST /upload` → 上传文件 → `CmdFile`(17) 发送文件消息
- [ ] `POST /register` → 注册（生产模式）
- [ ] 群组：`CmdGroupCreate`(12) → `CmdGroupJoin`(13) → 群聊 `CmdChat` chat_type=2
- [ ] 撤回：`CmdRecall`(19) + seq=原始msg_id

---

## 附录 B：配置参考

```json
{
  "gateway": {
    "http_addr": ":8080",
    "tcp_addr": ":8081",
    "transport": "websocket",
    "auth": { "dev_mode": true },
    "rate_limit": { "enabled": true, "rate": 10, "burst": 20 },
    "object_storage": { "enabled": false },
    "mysql": { "enabled": false },
    "kafka": { "enabled": false }
  }
}
```

> 开发阶段用默认配置即可：无需 MySQL/Redis/Kafka/MinIO，消息存内存，免密登录。

---

*文档生成时间：2026-07-20 | 项目版本：Phase 1-5 Complete*
