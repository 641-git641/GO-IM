# IM — Go 即时通讯系统

基于 Go 构建的即时通讯（IM）系统，支持 WebSocket / TCP 双传输协议、单聊 / 群聊、文件上传、全文搜索、多网关集群等功能。前后端分离，Docker 一键部署。

## 架构

```
 Client (React SPA)          ───  nginx (:80)  ───  Gateway (:8080 / :8081)
                                                         │
                                                    gRPC (:50051)
                                                         │
                                                      Logic
                                                         │
                                              ┌──────────┼──────────┐
                                            MySQL     Redis     Kafka    MinIO
```

- **Gateway**：连接层，WebSocket + gnet TCP + HTTP API，消息路由、心跳、离线存储
- **Logic**：业务层，gRPC 服务 + Kafka Consumer，消息持久化、历史查询、群组管理
- **Frontend**：React + TypeScript + Tailwind CSS，响应式单页应用

## 功能

| 模块 | 说明 |
|------|------|
| 实时消息 | 单聊 / 群聊，在线推送 + 离线拉取，消息 ACK 与去重 |
| 双传输协议 | WebSocket（gorilla/websocket）+ 裸 TCP（gnet v2），通过 Transport 接口统一 |
| 用户系统 | 注册 / 登录（JWT + bcrypt）、在线状态、角色管理（admin） |
| 好友系统 | 添加 / 接受 / 拒绝 / 删除，WebSocket 实时通知 |
| 群组管理 | 创建 / 加入 / 离开 / 踢人 / 改名 / 转交，群通知（member_joined / member_left） |
| 未读计数 | 按会话追踪未读消息数，已读回执实时清除 |
| 消息历史 | 分页查询历史消息（MySQL 持久化 + Kafka 异步写入） |
| 全文搜索 | CmdSearch 协议 + REST API，支持按关键词、会话、消息类型过滤 |
| 文件上传 | 图片 / 文件上传到 MinIO（S3），自动生成 200px 缩略图 |
| 消息撤回 | 2 分钟撤回窗口，撤回通知实时推送给对端 |
| 多网关集群 | 一致性哈希路由（CRC32 + 150 虚拟节点），跨节点 gRPC 转发 |
| 动态集群 | gRPC 健康探测 + Redis 服务发现，自动上下线 |
| 速率限制 | 每 UID 令牌桶限流（默认 10 msg/s，突发 20） |
| 管理后台 | 系统统计、用户管理、消息浏览，仅 admin 角色可见 |

## 快速开始

### 前置要求

- [Docker](https://docs.docker.com/get-docker/) + Docker Compose
- Go 1.26+（仅本地开发需要）

### 首次构建与启动

```bash
# 克隆项目
git clone <repo-url> && cd im

# 构建所有镜像（首次或依赖变更时需要）
docker-compose build

# 启动所有服务（后台运行）
docker-compose up -d

# 查看各服务运行状态
docker-compose ps

# 查看所有服务日志
docker-compose logs -f
```

浏览器打开 [http://localhost](http://localhost)，注册账号即可使用。

> **国内网络加速**：如果 `docker-compose build` 拉取依赖缓慢，可通过代理构建：
> ```bash
> set HTTP_PROXY=http://127.0.0.1:<port>      # Windows (cmd)
> # 或
> export HTTP_PROXY=http://127.0.0.1:<port>   # Linux / macOS / Git Bash
>
> docker-compose build --build-arg HTTP_PROXY=$HTTP_PROXY --build-arg HTTPS_PROXY=$HTTP_PROXY
> ```
>
> Go 镜像使用 `GOPROXY=https://goproxy.cn,direct`，无需额外配置。

### 修改代码后重新构建

```bash
# 只改 Go 代码 → 重建 Gateway
docker-compose build gateway && docker-compose up -d gateway

# 只改 Go 代码 → 重建 Logic
docker-compose build logic && docker-compose up -d logic

# 只改前端 → 重建前端
docker-compose build frontend && docker-compose up -d frontend

# 改了 Protobuf / go.mod → 重建所有
docker-compose build --no-cache && docker-compose up -d
```

### 查看日志

```bash
# 实时跟踪所有服务
docker-compose logs -f

# 只看某个服务
docker-compose logs -f gateway
docker-compose logs -f logic
docker-compose logs -f frontend

# 最近 N 行
docker-compose logs --tail=50 gateway
```

### 重启单个服务

```bash
docker-compose restart gateway     # 不重建镜像，仅重启容器
docker-compose restart frontend
```

### 停止与清理

```bash
# 停止所有服务（保留数据卷和镜像）
docker-compose down

# 停止并删除数据卷（MySQL、Redis、Kafka、MinIO 数据清空）
docker-compose down -v

# 同时删除构建的镜像
docker-compose down -v --rmi all
```

### 服务端口

| 端口 | 服务 | 说明 |
|------|------|------|
| `:80` | nginx / 前端 | 浏览器入口 |
| `:8080` | Gateway HTTP + WebSocket | API / 长连接 |
| `:8081` | Gateway gnet TCP | 裸 TCP 长连接 |
| `:50051` | Logic gRPC | 内部服务 |
| `:6379` | Redis | 离线消息 / 会话 |
| `:3307` | MySQL | 用户 / 消息持久化 |
| `:9092` | Kafka | 消息队列 |
| `:9000` | MinIO API | 对象存储 |
| `:9001` | MinIO Console | 管理面板 |

## 项目结构

```
im/
├── cmd/
│   ├── gateway/             # Gateway 入口（连接层）
│   └── logic/               # Logic 入口（业务层）
├── internal/
│   ├── gateway/             # Gateway 核心：Server, Hub, Router, Client, Transport
│   │                       #   GnetHandler, HashRing, Cluster, GroupStore, etc.
│   ├── logic/               # Logic gRPC Server + Kafka Consumer
│   ├── mq/                  # Kafka Producer + Consumer
│   ├── repo/                # MySQL 数据持久层（UserStore, MessageStore）
│   └── pkg/
│       ├── jwt/             # JWT 签发 / 验证
│       └── snowflake/       # 分布式 ID 生成器
├── api/proto/
│   ├── message.proto        # 核心消息体（客户端 ↔ 服务端）
│   ├── logic.proto          # Gateway → Logic gRPC 接口
│   └── gateway.proto        # Gateway → Gateway gRPC 接口
├── configs/
│   ├── config.json          # 本地开发配置
│   ├── config.docker.json   # Docker 部署配置
│   └── config.example.json  # 完整配置示例
├── web/                     # React 前端（TypeScript + Vite + Tailwind）
│   ├── src/
│   │   ├── components/      # UI 组件（chat, contact, group, friend, admin）
│   │   ├── pages/           # 页面（Chat, Contacts, Profile, Admin…）
│   │   ├── stores/          # Zustand 状态管理
│   │   ├── lib/             # API 客户端、WebSocket 管理、认证工具
│   │   └── hooks/           # 自定义 Hooks
│   └── nginx.conf           # 前端 nginx 配置（SPA + API 反向代理）
├── docs/                    # 设计文档、API 参考、实现记录
├── docker-compose.yml       # 一键部署编排
├── Dockerfile.gateway       # Gateway 镜像
├── Dockerfile.logic         # Logic 镜像
└── Dockerfile.frontend      # 前端镜像
```

## 本地开发

### 运行依赖服务

```bash
# 只启动中间件（Redis, MySQL, Kafka, MinIO）
docker-compose up -d redis mysql kafka minio
```

### 启动 Gateway

```bash
# 使用本地配置（configs/config.json）
go run ./cmd/gateway/
```

### 启动 Logic（需要 MySQL）

```bash
go run ./cmd/logic/
```

### 启动前端

```bash
cd web
npm install
npm run dev          # Vite 开发服务器，默认 :5173
```

### 运行测试

```bash
# 全部单元测试（226 个）
go test ./internal/...

# 集成测试（需要本地 Gateway 运行在 :18080）
go test ./cmd/gateway/ -v

# 单个包
go test ./internal/gateway/ -v -run TestRouter
```

## 配置

配置文件通过 `CONFIG_PATH` 环境变量指定（Docker 中默认 `/etc/im/config.docker.json`），不存在时自动使用默认值。

### 核心配置项

```json
{
  "gateway": {
    "http_addr": ":8080",
    "tcp_addr": ":8081",
    "transport": "both",             // "websocket" | "gnet" | "both"
    "mysql":  { "enabled": true, "dsn": "..." },
    "kafka":  { "enabled": true, "brokers": ["kafka:9092"] },
    "redis":  { "addr": "redis:6379" },
    "object_storage": { "enabled": true, "endpoint": "minio:9000" },
    "rate_limit": { "enabled": true, "rate": 10, "burst": 20 },
    "auth": { "dev_mode": false }
  },
  "admin_uids": ["admin"],
  "jwt": { "secret": "change-me", "expiration": "168h" },
  "snowflake": { "worker_id": 1 },
  "stability": { "max_connections": 0, "pprof_enabled": false }
}
```

完整配置说明见 [configs/config.example.json](configs/config.example.json)。

## 通信协议

### 核心消息体

所有消息使用 Protocol Buffers 编码，统一 `Message` 结构：

```protobuf
message Message {
  string cmd      = 1;   // 命令字：chat / heartbeat / history / ...
  string from     = 2;   // 发送者 UID（服务端覆写）
  string to       = 3;   // 接收者 UID / Group ID
  int32  chatType = 4;   // 0=单聊, 1=群聊
  int32  msgType  = 5;   // 0=文本, 1=图片, 2=文件
  bytes  content  = 6;   // 消息内容（文本 / JSON）
  string msgId    = 7;   // Snowflake 消息 ID
  string seq      = 8;   // 客户端序列号
  // ...
}
```

### 传输层编码

| 传输方式 | 编码 |
|----------|------|
| WebSocket | Protobuf 二进制帧 |
| gnet TCP | `[4-byte Big-Endian Length][Protobuf Payload]` |
| gRPC | HTTP/2 + Protobuf |
| Kafka | Protobuf Value + Snowflake MsgID Key |

详细协议规范见 [docs/07-api-reference.md](docs/07-api-reference.md)。

## 技术栈

| 层级 | 技术 |
|------|------|
| 语言 | Go 1.26 + TypeScript 6 |
| 前端框架 | React 19 + React Router 7 + Zustand 5 |
| 前端样式 | Tailwind CSS 3 + Lucide Icons |
| 构建工具 | Vite 8 + Rolldown |
| 长连接 | gorilla/websocket + panjf2000/gnet v2 |
| RPC | gRPC + Protobuf |
| 消息队列 | Apache Kafka (segmentio/kafka-go) |
| 数据库 | MySQL 8.4 (database/sql) |
| 缓存 | Redis 7 (go-redis/v9) |
| 对象存储 | MinIO (S3 兼容) |
| 认证 | JWT HS256 + bcrypt |
| 反向代理 | nginx（SPA 静态服务 + API 代理） |
| 容器化 | Docker + Docker Compose |

## 文档

| 文档 | 说明 |
|------|------|
| [docs/01-architecture-design.md](docs/01-architecture-design.md) | 架构设计与技术选型 |
| [docs/07-api-reference.md](docs/07-api-reference.md) | 完整 HTTP / WS / TCP 接口文档 |
| [docs/04-phase1-implementation.md](docs/04-phase1-implementation.md) | Phase 1 实现记录 |
| [docs/05-phase2-completion.md](docs/05-phase2-completion.md) | Phase 2 完成报告 |
| [docs/06-phase4-completion.md](docs/06-phase4-completion.md) | Phase 4 完成报告 |
| [CLAUDE.md](CLAUDE.md) | AI 辅助开发指南 |

## License

MIT
