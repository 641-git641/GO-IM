# 08 — 架构图集(Mermaid)

> 版本: v1.0 | 日期: 2026-08-04
>
> 本文档收录系统各层面的 Mermaid 架构图,与代码同步演进。GitHub 原生渲染 Mermaid,修改代码后请同步更新对应图示。
>
> 与 [docs/01-architecture-design.md](01-architecture-design.md) 的关系:docs/01 保留设计决策与论证,本文档聚焦"当前代码真实长什么样"的运行视图。

## 目录

| # | 图 | 类型 | 适用场景 |
|---|-----|------|---------|
| 1 | [系统总览](#1-系统总览) | flowchart | README 架构区、新人入门 |
| 2 | [WS 热路径 + 异步持久化](#2-ws-热路径--异步持久化) | sequence | 单聊消息全链路 |
| 3 | [Kafka 持久化链路](#3-kafka-持久化链路) | sequence | 消息落库 / 至少一次语义 |
| 4 | [跨网关转发三层降级](#4-跨网关转发三层降级) | sequence | 多网关消息路由 |
| 5 | [多网关集群拓扑](#5-多网关集群拓扑) | flowchart | 水平扩展 / 服务发现 |
| 6 | [开发部署拓扑](#6-开发部署拓扑) | flowchart | docker-compose.yml |
| 7 | [生产部署拓扑](#7-生产部署拓扑) | flowchart | docker-compose.prod.yml |
| 8 | [Go 模块图](#8-go-模块图) | graph | 代码结构导航 |
| 9 | [登录 / 心跳生命周期](#9-登录--心跳生命周期) | sequence | 连接建立与保活 |
| 10 | [群聊扇出](#10-群聊扇出) | sequence | 群聊投递 |

---

## 1. 系统总览

> 本文档图 1 供 README 架构区复用。Gateway 当前为单体进程,HTTP / WebSocket / gnet TCP 三种入口统一收敛到 `Hub → Router`。

```mermaid
flowchart TD
    Client["Web (React) / PC / Mobile<br/>客户端"]

    subgraph GW["Gateway 接入层 (单体 · :8080 HTTP/WS · :8081 TCP)"]
        HTTP["HTTP API<br/>/login /register /online /health /upload /file /search /unread /group/*"]
        WS["WebSocket 长连接<br/>(gorilla/websocket)"]
        TCP["gnet TCP 长连接<br/>4字节长度前缀 + Protobuf"]
        Hub["Connection Hub<br/>UID → *Client + 离线队列"]
        Router["Router<br/>路由 / ACK / 去重 / 限流 / 群扇出 / 撤回 / 搜索"]
    end

    subgraph LG["Logic 业务层 (gRPC :50051)"]
        GRPC["gRPC Server<br/>QueryHistory / GetUser"]
        CONS["Kafka Consumer<br/>批量落库 MySQL"]
    end

    subgraph DS["存储层"]
        MYSQL["MySQL<br/>users / messages / groups"]
        REDIS["Redis<br/>离线队列 / 集群服务发现"]
        KAFKA["Kafka<br/>im.message.persist"]
        MINIO["MinIO (S3)<br/>im-files 对象存储 + 缩略图"]
    end

    Client -->|HTTP| HTTP
    Client -->|WebSocket| WS
    Client -->|TCP| TCP
    WS --> Hub
    TCP --> Hub
    HTTP --> Hub
    Hub --> Router
    Router -->|异步持久化| KAFKA
    Router -->|gRPC 历史查询| GRPC
    CONS --> MYSQL
    GRPC --> MYSQL
    Hub --> REDIS
    Router --> MINIO
```

---

## 2. WS 热路径 + 异步持久化

> 投递 + ACK 完成后才异步持久化,`persistAsync()` 不阻塞热路径。

```mermaid
sequenceDiagram
    participant A as Alice
    participant GW as Gateway (Router)
    participant B as Bob
    participant K as Kafka
    participant LG as Logic + MySQL

    A->>GW: CmdChat {to: "bob", content: "hi"}
    GW->>GW: 校验 / 去重 / 限流
    GW->>B: 投递消息 (MsgId)
    B-->>GW: ACK
    GW-->>A: ACK (MsgId)
    Note over GW,K: 投递成功后异步持久化,不阻塞热路径
    GW-->>K: persistAsync() → im.message.persist
    K-->>LG: Consumer 批量消费
    LG->>LG: 批量写 MySQL 并提交 offset
```

---

## 3. Kafka 持久化链路

> at-least-once 语义:批量写成功后提交 offset;毒消息用独立 2s 超时提交,不阻塞消费。

```mermaid
sequenceDiagram
    participant GW as Gateway (Producer)
    participant K as Kafka im.message.persist
    participant C as Logic Consumer
    participant DB as MySQL

    GW->>GW: 8 字节 Snowflake MsgID 作 key
    GW->>K: Publish(protobuf value, msgId key)
    Note over GW,K: 即发即忘,失败仅记日志,AllowAutoTopicCreation 自愈建 topic
    loop 批量缓冲 (最多 100 条 或 1s 定时刷新)
        C->>K: FetchMessage
        K-->>C: 消息
        C->>DB: 批量 Save
        DB-->>C: ok
        C->>K: CommitOffsets (at-least-once)
    end
    Note over C,DB: 毒消息: 独立 2s 超时提交,不阻塞消费
```

---

## 4. 跨网关转发三层降级

> 路由链:HashRing 定位 → gRPC Forward → 失败则降级本地离线存储,每层优雅降级。

```mermaid
sequenceDiagram
    participant GWA as Gateway-A (发送方所在节点)
    participant RING as HashRing
    participant GWB as Gateway-B (接收方所在节点)
    participant LOCAL as 本地离线队列

    GWA->>RING: Get(targetUID)
    RING-->>GWA: 归 Gateway-B 所有
    alt Gateway-B 在线可达
        GWA->>GWB: gRPC ForwardMessage(uid, msg)
        GWB-->>GWA: ForwardResponse(delivered=true)
    else 转发失败或节点不可达
        GWA->>LOCAL: 降级 StoreOffline
        Note over LOCAL: 接收方上线后 CmdOffline 拉取
    end
```

---

## 5. 多网关集群拓扑

> ClusterManager 通过 gRPC 健康探测 + Redis `SETEX` 心跳维护 HashRing 成员,摘除/恢复自动同步。

```mermaid
flowchart TD
    LB["负载均衡<br/>nginx / HAProxy"]
    G1["Gateway-1"]
    G2["Gateway-2"]
    G3["Gateway-3"]
    LOGIC["Logic"]
    REDIS["Redis<br/>SETEX 心跳 im:gateway:node:*"]
    MYSQL["MySQL"]
    KAFKA["Kafka"]

    LB --> G1
    LB --> G2
    LB --> G3
    G1 <-->|gRPC Forward| G2
    G2 <-->|gRPC Forward| G3
    G1 <-->|gRPC Forward| G3
    G1 --> LOGIC
    G2 --> LOGIC
    G3 --> LOGIC
    G1 -.->|心跳 / 发现| REDIS
    G2 -.->|心跳 / 发现| REDIS
    G3 -.->|心跳 / 发现| REDIS
    LOGIC --> MYSQL
    LOGIC --> KAFKA
```

---

## 6. 开发部署拓扑

> `docker-compose.yml` 共 7 个服务,依赖链由 healthcheck + `depends_on.condition` 保证顺序。

```mermaid
flowchart LR
    subgraph HOST["宿主机 (docker-compose.yml, 7 个服务)"]
        FE["frontend<br/>nginx :80 (SPA + 反代)"]
        GW["gateway<br/>:8080 HTTP/WS · :8081 TCP"]
        LG["logic<br/>:50051 gRPC"]
        RD["redis<br/>:6379"]
        MY["mysql<br/>:3307 → 容器 3306"]
        KA["kafka<br/>:9093 容器内 · :9094 宿主机"]
        MI["minio<br/>:9000 API · :9001 Console"]

        FE -->|/api /ws 反代| GW
        GW -->|gRPC QueryHistory / GetUser| LG
        GW --> RD
        GW --> MY
        GW --> KA
        GW --> MI
        LG --> MY
        LG --> KA
    end
    BROWSER["浏览器<br/>http://localhost"] --> FE
```

---

## 7. 生产部署拓扑

> 仅 proxy 暴露宿主端口,内部服务走 Docker 内网。证书由 certbot 每 12h 自动续期(首次需 `deploy/init-ssl.sh`)。

```mermaid
flowchart TD
    USER["用户浏览器"]
    PROXY["nginx proxy<br/>:80 / :443 (SSL 终止)<br/>对外唯一入口"]
    FE["frontend (nginx + SPA)"]
    GW["gateway"]
    LG["logic"]
    REDIS["redis"]
    MYSQL["mysql"]
    KAFKA["kafka"]
    MINIO["minio"]
    CERT["certbot<br/>每 12h 自动续期"]

    USER -->|HTTPS :443| PROXY
    PROXY -->|静态资源| FE
    PROXY -->|/api /ws 反代| GW
    PROXY -.->|证书验证| CERT
    GW --> LG
    GW --> REDIS
    GW --> MYSQL
    GW --> KAFKA
    GW --> MINIO
    LG --> MYSQL
    LG --> KAFKA
```

> 内网服务(redis / mysql / kafka / minio / gateway / logic / frontend)不映射宿主端口,仅 Docker 内部网络可达。

> 💡 **2C2G 最小栈形态**(当前 prod compose 默认):省略 Kafka 与 Logic,共 7 个服务——网关直连 MySQL 异步持久化(`router.doPersist` 双路径),历史 / 群聊 / 搜索 / 未读不受影响。换大服务器可恢复上图完整形态。

---

## 8. Go 模块图

> 箭头表示依赖方向(上游依赖下游)。`internal/pkg/*` 为无外部依赖的基础库。

```mermaid
graph LR
    CFG["configs"]
    P["api/proto<br/>(protobuf 定义 + 生成代码)"]
    S["internal/pkg/snowflake"]
    J["internal/pkg/jwt"]
    R["internal/repo<br/>(UserStore / MessageStore)"]
    MQ["internal/mq<br/>(Kafka Producer / Consumer)"]
    G["internal/gateway<br/>(Server / Hub / Router / ...)"]
    L["internal/logic<br/>(gRPC Server + Consumer)"]
    CMDG["cmd/gateway"]
    CMDL["cmd/logic"]

    CFG --> CMDG
    CFG --> CMDL
    S --> G
    J --> G
    P --> G
    P --> MQ
    P --> L
    R --> G
    R --> L
    R --> MQ
    MQ --> G
    MQ --> L
    G --> CMDG
    L --> CMDL
```

---

## 9. 登录 / 心跳生命周期

> WebSocket 用协议层 Ping/Pong 保活,gnet TCP 用应用层 CmdHeartbeat;连续 3 次失败判定离线。

```mermaid
sequenceDiagram
    participant C as 客户端
    participant GW as Gateway

    C->>GW: HTTP POST /login (username + password)
    GW->>GW: bcrypt 校验密码
    GW-->>C: JWT (HS256, 7 天有效期)
    C->>GW: WebSocket 升级 / TCP 首帧携带 JWT
    GW->>GW: 校验 JWT → 提取 UID → 注册 Client
    loop 每 30s 心跳
        C->>GW: Ping (WS) / CmdHeartbeat (TCP)
        GW-->>C: Pong / CmdHeartbeatResp
    end
    Note over C,GW: 连续 3 次失败 → 判定离线,清理连接
```

---

## 10. 群聊扇出

> `CmdChat` 命中群聊后 fan-out 到全部成员(发送者除外),每人独立走"在线投递 / 离线存储 / 跨网关转发",并累计未读数。

```mermaid
sequenceDiagram
    participant A as Alice
    participant GW as Gateway
    participant GS as GroupStore
    participant M as 群成员 Bob / Carol / ...

    A->>GW: CmdChat {chatType=群聊, to=g_xxx, content}
    GW->>GW: 校验 / 去重 / 限流
    GW->>GS: GetMembers(g_xxx)
    GS-->>GW: [bob, carol, ...]
    loop 除发送者外的每个成员
        GW->>M: 在线投递 / 离线存储 / 跨网关转发
        GW->>GW: UnreadTracker.Increment(成员)
    end
    GW-->>A: ACK
```
