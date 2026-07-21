# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test

```bash
# Run the gateway server (reads configs/config.json)
go run ./cmd/gateway/

# Run the logic service (reads configs/config.json, requires MySQL)
go run ./cmd/logic/

# Run integration tests (spins up a real server on :18080)
go test ./cmd/gateway/ -v

# Run a single test
go test ./cmd/gateway/ -run TestIntegrationEndToEnd -v

# Build binaries
go build -o gateway.exe ./cmd/gateway/
go build -o logic.exe ./cmd/logic/
```

Unit tests (183 tests across 9 packages, all passing) are in `internal/pkg/snowflake/`, `internal/pkg/jwt/`, `internal/gateway/` (hub, router, redis_store, gnet_handler, grpc_client, hashring, grpc_gateway, group_store, unread_tracker, object_store, thumbnail, read_receipt, server), `internal/mq/` (producer, consumer), `internal/logic/` (gRPC server + consumer), `internal/repo/` (MySQL, skipped when not running), plus `cmd/gateway/` (5 integration tests: 2 WebSocket + 3 gnet TCP). Use `go test ./internal/...` to run them all, plus `go test ./cmd/gateway/ -v` for integration tests. See `docs/06-phase4-completion.md` for the full test breakdown.

## Architecture

This is a Go IM (instant messaging) system in MVP — a **single monolith** that merges three planned layers (Gateway → Logic → Storage). It uses Protocol Buffers (protobuf) binary encoding over WebSocket (gorilla/websocket) and raw TCP (panjf2000/gnet v2). See `docs/01-architecture-design.md` for the full vision.

### Request flow

```
# WebSocket (default, port :8080)
HTTP /login  → Server.HandleLogin              → JWT issued
HTTP /ws     → Server.HandleWS                 → JWT validated → WebSocket upgrade → Client created
WebSocket    → wsReadPump(ctx, conn, client, router) → Router.Route(ctx, …) → Hub
             → Client.WriteLoop()              → outbound messages (transport-agnostic)
             → wsPingLoop()                    → WebSocket ping/pong keepalive

# gnet TCP (optional, port :8081)
TCP connect  → GnetHandler.OnOpen              → pending state
TCP data     → GnetHandler.OnTraffic           → frame decode (4-byte len prefix + protobuf)
             → GnetHandler.processFrame        → handleLogin (first msg = CmdLogin + JWT)
                                               → Router.Route(ctx, …) via WorkerPool
             → Client.WriteLoop()              → outbound messages → gnet.Conn.AsyncWrite (4-byte len prefix + protobuf)

# Message history (CmdHistory)
CmdHistory    → Router.handleHistory(ctx, …)    → LogicClient.QueryHistory (gRPC) or MessageStore.QueryHistory (local)
             → sender.Send(historyMsg)        → sender.Send(completion) with Seq=delivered count

# Message persistence (Kafka, Phase 3)
CmdChat       → Router.handleChat(ctx, …)     → deliver + ACK (hot path, unchanged)
             → go mq.Producer.Publish(context.WithoutCancel(ctx), msg) → Kafka "im.message.persist" → mq.Consumer → MySQL

# Group chat (Phase 4)
CmdChat (ChatType=Group) → Router.handleChat → fanoutGroup(ctx, sender, msg)
                         → GroupStore.GetMembers → for each member: send or routeOrStoreOffline
                         → UnreadTracker.Increment (all members except sender)

# Read receipt & unread count (Phase 4)
CmdReadReceipt → Router.handleReadReceipt → UnreadTracker.MarkRead(reader, peer)
               → forward receipt to peer (local or cross-gateway via hash ring)
CmdUnreadCount → Router.handleUnreadCount → UnreadTracker.GetCounts(uid) → JSON response

# File upload/download (Phase 4)
HTTP /upload   → Server.HandleUpload      → JWT validate → ObjectStore.Put(fileID, data, mime)
                                          → Thumbnail generation (images, max 4096px, 200px output)
HTTP /file     → Server.HandleDownload    → JWT validate → ObjectStore.Get(fileID or fileID_thumb)

# Fulltext search (Phase 4)
CmdSearch      → Router.handleSearch     → MessageStore.SearchMessages(params) → paginated results + completion signal
HTTP /search   → Server.HandleSearch     → JWT validate → Router.Search → JSON response

# Message recall (Phase 5)
CmdRecall      → Router.handleRecall → validate target + sender + Seq (original MsgID)
               → msgStore.RecallMessage(msgID, fromUID) → MySQL marks recalled=1
               → recall notification to peer (local / cross-gateway / offline)
               → sendRecallError on failure — no ACK, no persistence for recall itself

# Multi-gateway forwarding (Phase 4)
routeOrStoreOffline → HashRing.Get(targetUID) → if peer owns: GrpcForwarder.Forward(ctx, uid, msg)
                    → GrpcGatewayServer.ForwardMessage → local deliver or StoreOffline
                    → fallback: local StoreOffline on forward failure
```

### Component responsibilities

| Component | File | Role |
|-----------|------|------|
| `App` | `cmd/gateway/main.go` | Wires everything together; owns `ClientRegistry`, `Server`, `Config`. Supports dual-transport (WebSocket + gnet TCP) startup via `transport` config. |
| `Server` | `internal/gateway/server.go` | HTTP handlers: `/login` (JWT issue), `/register`, `/online`, `/health`, group CRUD, upload/download, search, unread. Depends on `ClientRegistry` (not concrete `*Hub`). Also owns `StartGNet()` for gnet TCP server. |
| `Hub` | `internal/gateway/hub.go` | Implements `ClientRegistry` + `OfflineStore`. Connection registry (`map[UID]*Client`) + offline message queue (in-memory, max 1000/user). All methods accept `context.Context`. |
| `ClientRegistry` | `internal/gateway/interfaces.go` | Interface for connection management: `Register`, `Unregister`, `Get`, `IsOnline`, `OnlineUsers`, `Count`. Implemented by `*Hub`; Redis impl planned. |
| `OfflineStore` | `internal/gateway/interfaces.go` | Interface for offline message queues: `StoreOffline`, `DrainOffline`. Implemented by `*Hub` (in-memory) and `*RedisOfflineStore` (Redis-backed, with in-memory fallback). |
| `RedisOfflineStore` | `internal/gateway/redis_store.go` | Redis-backed `OfflineStore` using Redis Lists + Lua scripts for atomic push/trim and drain. Falls back to an in-memory `OfflineStore` on Redis failure. |
| `DedupCache` | `internal/gateway/dedup.go` | Message deduplication by `fromUID:seq` key. Prevents duplicate delivery on client retry. |
| `Router` | `internal/gateway/router.go` | Message dispatch: heartbeat, chat delivery (validate → dedup → rate limit → push/store), group fan-out, offline drain, history, read receipts, unread count, search, group CRUD, message recall (CmdRecall). Depends on `ClientRegistry` + `OfflineStore` + `repo.MessageStore` (optional) + `mq.Producer` (optional) + `GroupStore` + `UnreadTracker` + `HashRing` + `Forwarder`. |
| `Client` | `internal/gateway/client.go` | Per-connection wrapper. Uses `Transport` interface instead of concrete `*websocket.Conn`. `WriteLoop()` drains the `send` channel to `transport.Write()`. Read paths differ by transport (wsReadPump for WebSocket, GnetHandler.OnTraffic for TCP). |
| `Transport` | `internal/gateway/transport.go` | Interface: `Close() error` + `Write([]byte) error`. Allow Client to work with any underlying connection type. |
| `wsTransport` | `internal/gateway/transport_ws.go` | WebSocket Transport: wraps `*websocket.Conn`, Write → `WriteMessage(BinaryMessage)`. |
| `gnetTransport` | `internal/gateway/transport_gnet.go` | gnet TCP Transport: wraps `gnet.Conn`, Write → `AsyncWrite`. |
| `GnetHandler` | `internal/gateway/gnet_handler.go` | gnet `EventHandler`: `OnOpen` (pending state), `OnClose` (cleanup), `OnTraffic` (frame decode + dispatch). Also: `WorkerPool`, heartbeat checker, `connMap` (gnet.Conn → *Client). |
| `server_ws.go` | `internal/gateway/server_ws.go` | WebSocket-specific code: `HandleWS`, `wsReadPump`, `wsPingLoop`. Extracted from server.go and client.go. |
| `proto.Message` | `api/proto/message.pb.go` (generated) | Protobuf-generated message struct for all communication — control (heartbeat/ACK/login) and data (chat) share the same shape. `Cmd` field discriminates. Constants and `Validate()` in `api/proto/message.go`. |
| `snowflake.Generator` | `internal/pkg/snowflake/snowflake.go` | Globally unique, time-sortable message IDs |
| `jwt.Manager` | `internal/pkg/jwt/jwt.go` | HS256 token issue/validate for auth |
| `configs.Config` | `configs/config.go` | JSON config loading with `Default()` fallback |
| `repo.UserStore` | `internal/repo/repo.go` | Interface for user persistence: `Create`, `GetByUID`. Implemented by `*MySQLStore`. |
| `repo.MessageStore` | `internal/repo/repo.go` | Interface for message history persistence: `Save`, `QueryHistory`, `SearchMessages`, `RecallMessage`. Implemented by `*MySQLStore`. |
| `MySQLStore` | `internal/repo/mysql.go` | MySQL-backed `UserStore` + `MessageStore` implementation using `database/sql`. Auto-creates `users` and `messages` tables on init. Disabled when `mysql.enabled: false` (default). |
| `mq.Producer` | `internal/mq/producer.go` | Publishes messages to Kafka for async persistence (Phase 3). Fire-and-forget — failures are logged but never block the hot path. Disabled when `kafka.enabled: false` (default). |
| `mq.Consumer` | `internal/mq/consumer.go` | Kafka consumer that reads from `im.message.persist` topic and batch-writes messages to a `repo.MessageStore`. Runs in the Logic service (`cmd/logic/`). |
| `LogicClient` | `internal/gateway/grpc_client.go` | gRPC client for the Logic service. Used for synchronous queries (history, user lookup). Falls back to local `MessageStore` when gRPC is disabled. |
| `Logic Server` | `internal/logic/server.go` | gRPC server implementing `proto.LogicServer`. Provides `QueryHistory` and `GetUser` RPCs backed by MySQL. |
| `RateLimiter` | `internal/gateway/rate_limiter.go` | Per-UID token-bucket rate limiting with background stale-bucket cleanup. |
| `GroupStore` | `internal/gateway/group_store.go` | Interface: Create, AddMember, RemoveMember, GetMembers, IsMember, GetUserGroups, Get. `InMemoryGroupStore` is the default impl with snowflake group IDs; MySQL-backed impl planned for Phase 5. |
| `Group` | `internal/gateway/group_store.go` | Data model: ID (`g_123456`), Name, OwnerUID, Members map, CreatedAt. Empty groups are auto-deleted on last member removal. |
| `UnreadTracker` | `internal/gateway/unread_tracker.go` | Interface: Increment, MarkRead, GetCounts, GetCount. `InMemoryUnreadTracker` uses uid→{peerUID→count} two-level map. Self-messages are silently ignored. |
| `ObjectStore` | `internal/gateway/object_store.go` | Interface: Put, Get, Delete. `InMemoryObjectStore` for dev/test; `MinioStore` for S3-compatible MinIO with auto-bucket creation and Ping health check. |
| `Thumbnail` | `internal/gateway/thumbnail.go` | Image thumbnail generation: CatmullRom scaling, 200px max dimension, JPEG 80% output. Supports JPEG/PNG/GIF/WebP. Decompression bomb defense (max 4096px source). `ImageDimensions()` and `IsImageMIME()` helpers. |
| `HashRing` | `internal/gateway/hashring.go` | Consistent hashing with CRC32 + 150 virtual nodes per physical node. Thread-safe Add/Remove/Get. Used for UID→Gateway node routing in multi-node clusters. |
| `Forwarder` | `internal/gateway/grpc_forwarder.go` | Interface for cross-node message delivery. `GrpcForwarder` implements it via gRPC with lazy connection (double-checked locking), auto-eviction on RPC failure, configurable dial/rpc timeouts. Dynamic peer management: `AddPeer`, `RemovePeer`, `PeerAddrs`. |
| `GrpcGatewayServer` | `internal/gateway/grpc_server.go` | gRPC server implementing `proto.GatewayServer.ForwardMessage`. Receives forwarded messages from peer Gateways, delivers locally or stores offline. |
| `ClusterManager` | `internal/gateway/cluster.go` | Dynamic multi-gateway clustering: health checks peers via gRPC probes, removes/re-adds unhealthy/healthy nodes to the hash ring. Supports Redis service discovery (`SETEX` heartbeat + `KEYS` scan) as an alternative to static `peer_addrs`. Manages `HashRing` + `GrpcForwarder` synchronization. |
| `StabilityConfig` | `configs/config.go` | Operational settings: max_connections, HTTP timeouts (read/write/idle), shutdown_timeout, pprof toggle. |
| `Recovery` | `internal/gateway/server.go` | HTTP middleware: catches panics in handlers, logs stack, returns 500. Prevents single-connection panics from crashing the server. |

### Key design decisions (documented in `docs/01-architecture-design.md`)

- **`msg.From` is server-set**: The client's claimed sender is ignored; `readPump` overwrites it with the authenticated UID. This is a security feature, not a bug.
- **Online push, offline pull**: Online users receive messages in real-time via WebSocket; offline users get messages delivered when they reconnect and send `CmdOffline`.
- **Snowflake IDs**: 10-bit worker + 12-bit sequence, epoch 2024-01-01. WorkerID is currently hardcoded to 1.
- **No persistence by default**: Everything is in-memory — restart loses all state. MySQL persistence is available via `mysql.enabled: true` in config (see `internal/repo/`). Message history query (CmdHistory) requires MySQL.

## Phase history

### Phase 2 — complete (2026-07-18)

Phase 1 (2026-07-17) resolved 15 of 19 code-review issues. Phase 2 extended the system across 3 workstreams. See `docs/04-phase1-implementation.md` and `docs/05-phase2-completion.md`.

| # | Task | Status |
|---|------|--------|
| 1 | **Unit tests** — 85 tests across 7 packages + 5 integration tests | ✅ |
| 2 | **Interface abstractions** — `ClientRegistry` and `OfflineStore` interfaces extracted | ✅ |
| 3 | **Context propagation** — `context.Context` threaded through HTTP → Server → readPump → Router → Hub | ✅ |
| 4 | **Redis storage** — Redis-backed `OfflineStore` with Lua scripts + in-memory fallback | ✅ |
| 5 | **Protobuf** — Replace JSON with protobuf binary serialization | ✅ |
| 6 | **gnet TCP** — Dual-transport: WebSocket + raw TCP via `Transport` interface | ✅ |
| 7 | **CmdHistory** — Message history via `MessageStore` interface, paginated | ✅ |
| 8 | **MySQL repo** — `UserStore` + `MessageStore` interfaces + `MySQLStore` with `database/sql` | ✅ |
| 9 | **gnet handler tests** — 27 tests: WorkerPool, OnOpen, OnClose, handleLogin, processFrame, OnTraffic | ✅ |
| 10 | **TCP integration tests** — 3 e2e tests: mixed transport, offline delivery, heartbeat | ✅ |
| 11 | **Protocol fix** — gnet outbound messages: 4-byte big-endian length prefix | ✅ |

### Phase 3 — gRPC + Kafka (2026-07-18)

| # | Task | Status |
|---|------|--------|
| 1 | **Kafka infrastructure** — Apache Kafka 3.9 in KRaft mode, `segmentio/kafka-go` client | ✅ |
| 2 | **gRPC proto definitions** — `logic.proto` (QueryHistory, GetUser) + `gateway.proto` (ForwardMessage) | ✅ |
| 3 | **KafkaProducer** — Fire-and-forget publish to `im.message.persist`, never blocks hot path | ✅ |
| 4 | **Logic service** (`cmd/logic/`) — standalone gRPC server + Kafka consumer, batch-writes to MySQL | ✅ |
| 5 | **gRPC client** — Gateway calls Logic for `QueryHistory`, falls back to local `MessageStore` | ✅ |
| 6 | **Tests** — 7 new tests: Kafka producer (4), gRPC client (2), Logic server + consumer (5) | ✅ |

Phase 3 refinements:
- **`context.WithoutCancel`**: Prevents shutdown cancellation from dropping in-flight Kafka/MySQL persistence.
- **Malformed message commit**: Independent 2s timeout context for committing poison messages.
- **Config separation**: `logic.listen_addr` independent of `grpc.addr`.

### Phase 4 — Group chat + Rich media + Multi-gateway (2026-07-19)

See `docs/06-phase4-completion.md` for the full report.

| # | Task | Status |
|---|------|--------|
| 1 | **Group chat** — `GroupStore` + `InMemoryGroupStore`, HTTP CRUD APIs, fan-out delivery with dedup/ACK/persist | ✅ |
| 2 | **Read/unread tracking** — `UnreadTracker` + `InMemoryUnreadTracker`, CmdReadReceipt + CmdUnreadCount protocol | ✅ |
| 3 | **Object storage** — `ObjectStore` interface, `InMemoryObjectStore` + `MinioStore` (S3), HTTP upload/download with JWT | ✅ |
| 4 | **Image thumbnails** — CatmullRom scaling, JPEG/PNG/GIF/WebP, 200px, decompression bomb defense (max 4096px) | ✅ |
| 5 | **Fulltext search** — CmdSearch protocol, `repo.SearchMessages`, cursor pagination, HTTP API | ✅ |
| 6 | **Multi-gateway** — `HashRing` (CRC32, 150 replicas), `GrpcForwarder` (lazy dial, auto-eviction), `GrpcGatewayServer` | ✅ |
| 7 | **Stability** — HTTP timeouts, graceful shutdown, pprof, panic recovery, persist semaphore (max 64) | ✅ |
| 8 | **User registration** — bcrypt password hashing, duplicate detection, dev_mode toggle | ✅ |
| 9 | **Tests** — 183 total (all passing), +91 Phase 4 tests across 12 test files | ✅ |

Phase 4 refinements:
- **`persistSem` (capacity 64)**: Bounds concurrent async persistence goroutines; prevents goroutine explosion under high throughput.
- **Dedup Mark AFTER delivery**: `dedup.Mark` called only after successful online delivery — buffer-full failures leave the client free to retry.
- **`routeOrStoreOffline` fallback chain**: Hash ring → gRPC forward → local offline store. Each layer degrades gracefully.
- **Thumbnail decompression bomb defense**: Images > 4096px skip thumbnail generation.
- **`SetRateLimit` goroutine leak fix**: Calls `rl.Stop()` on the old limiter before creating a new one.

### Phase 5 — Group chat protocol + Dynamic clustering (2026-07-20)

| # | Task | Status |
|---|------|--------|
| 1 | **Group WS/TCP protocol** — CmdGroupCreate/Join/Leave/Info/List (12-16), router handlers | ✅ |
| 2 | **MySQL group persistence** — `groups` + `group_members` tables, `MySQLGroupStore` in gateway + logic | ✅ |
| 3 | **Group notifications** — `sendGroupNotification` for member_joined/member_left | ✅ |
| 4 | **Dynamic clustering** — `ClusterManager` with gRPC health probes + Redis service discovery (`SETEX` heartbeat, `KEYS` scan, peer reconciliation) | ✅ |
| 6 | **Message recall** — `CmdRecall` (19), 2-min window, `RecallMessage` MySQL store (recalled column + migration), `handleRecall` router handler, recall notification to peer (online/offline/cross-gateway) | ✅ |

### Current test coverage

```
226 passed, 0 failed, 0 skipped
```

| Package | Tests | Notes |
|---------|-------|-------|
| `internal/gateway` | 187 | hub(9) + router(24) + gnet_handler(27) + redis_store(7) + grpc_client(2) + grpc_gateway(5) + hashring(9) + group_store(12) + unread_tracker(9) + object_store(6) + thumbnail(9) + router_read_receipt(11) + router_group(19) + server(3) + cluster(22) + router_recall(11) |
| `internal/logic` | 9 | gRPC server(3) + consumer(6) |
| `internal/mq` | 11 | producer(3) + consumer(8) |
| `internal/pkg/jwt` | 8 | sign/validate, expiry, tampering, malformed, claims window, multi-user |
| `internal/pkg/snowflake` | 7 | uniqueness, monotonic, worker ID, timestamp extraction, overflow |
| `internal/repo` | 7 | MySQL CRUD + history + pagination (skipped when MySQL not running) |
| `cmd/gateway` | 5 | 2 WebSocket + 3 gnet TCP integration tests |

## Configuration

The server loads `configs/config.json`, falling back to `Default()` if the file doesn't exist. A missing file triggers a log message then uses defaults; a malformed file returns an error. See `configs/config.example.json` for a complete annotated example.

Default config values:
- Transport: `"websocket"` (options: `"websocket"`, `"gnet"`, `"both"`)
- HTTP: `:8080` (serves /login, /register, /online, /health, /ws, /upload, /file, /search, /unread, /group/*)
- TCP: `:8081` (gnet raw TCP, port used when transport is "gnet" or "both")
- JWT secret: `"change-me-in-production"`, 7-day expiration, HS256
- Snowflake worker ID: 1
- Heartbeat: 30s interval, 3 max failures (WebSocket uses Ping/Pong; gnet uses application-level `CmdHeartbeat`)
- Connection: pong wait 60s, ping period 54s, max message 65536 bytes, send buffer 256, offline queue 1000/user
- Rate limit: enabled, 10 msg/s per user, burst 20
- WebSocket origins: empty list = allow all (development mode)
- gnet: num_event_loops 0 (auto), worker_pool_size 0 (auto)
- MySQL: `enabled: false`, `dsn: ""` — enable for message history (CmdHistory) and user persistence
- Kafka: `enabled: false`, `brokers: ["localhost:9092"]`, `topic: "im.message.persist"`
- Logic gRPC: `addr: ""` (empty = Gateway uses local MessageStore), `listen_addr: ""` (empty = Logic binds `:50051`)
- gRPC server: `addr: ""` (empty = disabled, set `:50050` with node_id + peer_addrs for multi-gateway)
- gRPC discovery: `mode: ""` (empty or "static" = use peer_addrs; "redis" = Redis service discovery with TTL heartbeat)
- Auth: `dev_mode: true` (no password required; set false for production with MySQL)
- Object storage: `enabled: false` (in-memory fallback), endpoint `localhost:9000`, bucket `im-files`, max_upload 10MB
- Stability: `max_connections: 0` (unlimited), HTTP timeouts (read 10s, write 10s, idle 120s), `shutdown_timeout: 30s`, `pprof_enabled: false`

### Wire formats summary

- **gnet TCP**: `[4-byte big-endian uint32 length][N-byte protobuf payload]`. First message must be `CmdLogin` with JWT.
- **gRPC Gateway↔Logic**: HTTP/2 + protobuf. Service defs in `api/proto/logic.proto`.
- **gRPC Gateway↔Gateway**: `ForwardRequest(message + uid)` / `ForwardResponse(delivered + error)`. Service defs in `api/proto/gateway.proto`.
- **Kafka**: Protobuf binary values, Snowflake MsgID (8-byte big-endian) as key, topic `im.message.persist`.

### gRPC (Gateway ↔ Logic)

```
Gateway (gRPC client)                    Logic (gRPC server, :50051)
  │                                              │
  │── QueryHistory(uid, peer, before, limit) ──▶│── MySQLStore.QueryHistory()
  │◀── HistoryResponse(messages, delivered) ────│
  │                                              │
  │── GetUser(uid) ────────────────────────────▶│── UserStore.GetByUID()
  │◀── UserResponse(uid, username, found) ──────│
```

### Kafka (Gateway → Logic)

```
Gateway                                 Kafka                  Logic Service
  │                                      │                         │
  │── mq.Producer.Publish(msg) ──────▶│ im.message.persist      │
  │   (fire-and-forget, non-blocking)    │                         │
  │                                      │── mq.Consumer.Run ──▶│
  │                                      │   (batch write)        │── MySQLStore.Save()
```

### Multi-gateway gRPC (Gateway ↔ Gateway, Phase 4)

```
Gateway-A                               Gateway-B
  │                                      │
  │── GrpcForwarder.Forward(uid, msg) ─▶│
  │   (hash ring lookup → gRPC call)      │── GrpcGatewayServer.ForwardMessage
  │                                      │   (local deliver or StoreOffline)
  │◀── ForwardResponse(delivered) ──────│

ClusterManager (health check + discovery):
  Health: probePeer(addr) → mark healthy/unhealthy → HashRing.Add/Remove
  Redis:  SETEX im:gateway:node:{id} {addr} {ttl}  → heartbeat refresh
          KEYS im:gateway:node:*                    → discover peers
          reconcilePeers(found)                     → add new, remove stale
```

## Infrastructure

**All middleware and backing services MUST run as Docker containers.** This includes Redis, MySQL, and any future infrastructure (Kafka, ETCD, etc.). The Go gateway itself may run natively during development for fast iteration, but all dependencies are containerized via `docker-compose.yml`.

```bash
# Start all middleware dependencies
docker-compose up -d

# Stop and clean up
docker-compose down
```

## Go environment

This project uses Go 1.26.5 at `E:\develop\Golang1.26.5`. If `GOROOT` is set to a different (empty) directory, override it:

```bash
# In bash:
export GOROOT="E:/develop/Golang1.26.5"
# In PowerShell:
$env:GOROOT="E:\develop\Golang1.26.5"
```

The `.vscode/settings.json` should have `"go.goroot": "E:\\develop\\Golang1.26.5"` configured.
