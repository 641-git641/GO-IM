# 03 — Phase 5 完成报告

> 版本: v4.0 | 日期: 2026-07-20 | 状态: ✅ 全部完成 | 基于: [01-架构设计](./01-architecture-design.md) + [06-Phase4-完成报告](./06-phase4-completion.md)
>
> Phase 1-4 已全部完成（详见 [06-phase4-completion.md](./06-phase4-completion.md)）。
> Phase 5 所有规划条目已实现，剩余微小改进项已移至 [TODO.md](../TODO.md) 追踪。

---

## Phase 5 完成概要

Phase 5（2026-07-20）的核心主题：让群聊功能"真正可用"（协议完整性 + 持久化），将文件上传/下载与消息通道打通，扩展 Logic 服务承担更多业务逻辑。

---

## Step 1: 群聊功能完善 ✅

### 1.1 WebSocket/TCP 群管理协议 ✅

已实现 5 个群组协议命令，前后端均已对接：

| Cmd | 编号 | Router Handler | 前端 useWebSocket |
|-----|------|---------------|-------------------|
| `CmdGroupCreate` | 12 | `handleGroupCreate` → GroupStore.Create | 更新 contactStore + 系统通知 |
| `CmdGroupJoin` | 13 | `handleGroupJoin` → GroupStore.AddMember | 拉取群信息 + member_joined 通知 |
| `CmdGroupLeave` | 14 | `handleGroupLeave` → GroupStore.RemoveMember | 移除群 + member_left 通知 |
| `CmdGroupInfo` | 15 | `handleGroupInfo` → GroupStore.Get | 更新 contactStore groupDetails |
| `CmdGroupList` | 16 | `handleGroupList` → GroupStore.GetUserGroups | 更新 contactStore groups |

**涉及文件**：
- `api/proto/message.go` — Cmd 常量 + Validate() 更新
- `internal/gateway/router.go` — 5 个 handler + Route 分发
- `web/src/hooks/useWebSocket.ts` — 5 个 case 完整处理逻辑
- `web/src/stores/contactStore.ts` — addGroup/removeGroup/updateGroupDetails actions

### 1.2 MySQL 群组持久化 ✅

- `groups` 表：id, name, owner_uid, created_at（`internal/repo/mysql.go` migrate）
- `group_members` 表：group_id, uid, joined_at
- `internal/gateway/mysql_group_store.go` — 实现 `GroupStore` 接口（Create/AddMember/RemoveMember/GetMembers/IsMember/GetUserGroups/Get/UpdateName/TransferOwnership）
- `internal/repo/mysql_group.go` — Logic 服务使用的独立实现（CreateGroup/AddMember/RemoveMember/GetMembers/GetGroup/ListGroups/IsMember）
- 事务保证：创建群时同时插入 groups + group_members；空群自动清理

### 1.3 群消息历史 ✅

- `MessageStore.QueryGroupHistory` — 独立方法，查询 `WHERE to_uid = groupID AND chat_type = 2`
- `Router.handleHistory` — `ChatTypeGroup` 时走 `QueryGroupHistory` 分支（gRPC 路径暂未实现，走本地 MessageStore）

### 1.4 群通知消息 ✅

- `sendGroupNotification` — member_joined / member_left / group_created 系统消息
- 复用 `CmdChat` + JSON 格式的 `{"type": "member_joined", "group_id": "…", "uid": "…"}` 内容
- 前端 `SystemNotice` 组件渲染群通知

---

## Step 2: 文件消息流程 ✅

### 2.1 CmdFile 协议 ✅

- `CmdFile = 17`：文件消息走完整通道：Router.Route → handleFile → 投递 → ACK → 持久化
- 复用 `MsgType.Image/Video/Voice/File` + JSON metadata（file_id, name, size, mime, width, height）

### 2.2 文件消息出现在历史记录 ✅

- 文件消息和普通聊天消息一样存入 MySQL `messages` 表
- `CmdHistory` 返回文件消息时，`FilePreview` 组件渲染缩略图/文件名 + `ImageLightbox` 大图预览

### 2.3 上传流程整合 ✅

```
1. POST /upload (multipart file) → 获得 file_id + thumbnail
2. WebSocket 发送 CmdChat + MsgType Image/File {file_id, name, …}
3. 接收方收到 → FilePreview 渲染 → 点击 → ImageLightbox 或 GET /file?id=xxx
```

---

## Step 3: Logic 服务扩展 ✅

### 3.1 SearchMessages gRPC ✅

- `api/proto/logic.proto` — SearchMessages RPC 已定义
- `internal/logic/server.go` — 完整实现，转发到 `MySQLStore.SearchMessages`

### 3.2 GroupService gRPC ✅

- `api/proto/logic.proto` — CreateGroup, JoinGroup, LeaveGroup, GetGroup, ListGroups RPC
- `internal/logic/server.go` — 通过 `repo.MySQLGroupStore` 实现所有 Group RPC

### 3.3 UnreadService gRPC ✅

- `api/proto/logic.proto` — IncrementUnread, MarkRead, GetUnreadCounts RPC
- `internal/logic/server.go` — 通过 `repo.MySQLUnreadStore` 实现
- MySQL `unread` 表：uid, peer, count

---

## Step 4: 多网关动态集群 ✅

### 4.1 节点健康检查 ✅

- `ClusterManager.probePeer` — gRPC health probe，可配置 `health_interval`
- 对端不可达时从 HashRing 摘除，恢复后自动加回

### 4.2 Redis 服务发现 ✅

- `ClusterManager.RunRedisDiscovery` — `SETEX` heartbeat + `KEYS` 扫描发现新节点
- 节点 TTL 过期自动摘除
- 配置：`grpc.discovery.mode: "redis"` 启用，`"static"` 或空使用 peer_addrs

### 4.3 配置简化 ✅

```json
{
  "grpc": {
    "addr": ":50050",
    "node_id": "gw-1",
    "peer_addrs": {},
    "forward_dial_timeout": "3s",
    "forward_rpc_timeout": "2s",
    "discovery": {
      "mode": "",
      "redis_key": "im:gateway:node:",
      "ttl": "15s",
      "health_interval": "5s"
    }
  }
}
```

---

## 剩余未完成项

已移至 [TODO.md](../TODO.md) 追踪：
- 代码质量：MySQL 群组存储去重（#40）、Dedup Redis 迁移（#54）、动态集群自动启动（#58-60）
- 文档：CLAUD.md 保持同步更新
