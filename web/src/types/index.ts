// ============================================================
// IM 前端 TypeScript 类型
// 与 api/proto/message.proto 中的 proto Message 结构保持一致
// ============================================================

/** 命令类型(与 Go 的 proto.CmdXxx 常量对应) */
export const Cmd = {
  None: 0,
  Chat: 1,
  Ack: 2,
  Login: 3,
  LoginResp: 4,
  Offline: 5,
  Heartbeat: 6,
  Kick: 7,
  History: 8,
  ReadReceipt: 9,
  UnreadCount: 10,
  Search: 11,
  GroupCreate: 12,
  GroupJoin: 13,
  GroupLeave: 14,
  GroupInfo: 15,
  GroupList: 16,
  File: 17,
  GroupInviteMember: 18,
  Recall: 19,
  FriendRequest: 20,
  FriendResponse: 21,
  Typing: 22,
  Forward: 23,
  Edit: 24,
} as const;

export type CmdType = (typeof Cmd)[keyof typeof Cmd];

/** 聊天类型 */
export const ChatType = {
  Single: 1,
  Group: 2,
} as const;

export type ChatTypeValue = (typeof ChatType)[keyof typeof ChatType];

/** 消息内容类型 */
export const MsgType = {
  Text: 1,
  Image: 2,
  Voice: 3,
  Video: 4,
  File: 5,
} as const;

export type MsgTypeValue = (typeof MsgType)[keyof typeof MsgType];

/**
 * 表示解码后的 protobuf Message。
 * int64 字段 (seq、msg_id、timestamp) 在 JS 中使用 bigint。
 * 我们以字符串形式存储它们,以便安全的 JSON 序列化和作为 React key。
 */
export interface IMMessage {
  seq: string; // 客户端序列号 (bigint → string)
  msgId: string; // 服务器雪花 ID (bigint → string)
  cmd: CmdType;
  from: string; // 发送者 UID(服务器设置)
  to: string; // 接收者 UID 或群组 ID
  chatType: ChatTypeValue;
  msgType: MsgTypeValue;
  content: string; // 纯文本或 JSON
  timestamp: string; // Unix 毫秒时间戳 (bigint → string)
  needAck: boolean;
}

/** 用于乐观 UI 的待发送/已发送状态 */
export type MessageStatus = 'sending' | 'sent' | 'failed' | 'read';

/** 带有 UI 状态的消息 */
export interface ChatMessage extends IMMessage {
  status: MessageStatus;
  recalled: boolean;
}

/** 会话摘要(在客户端根据消息构建) */
export interface Conversation {
  peerId: string; // 对方的 UID 或群组 ID
  name: string; // 显示名称
  chatType: ChatTypeValue;
  lastMessage: string; // 预览文本
  lastTime: number; // Unix 毫秒
  unread: number;
  messages: ChatMessage[];
  hasMore: boolean; // 是否有更多历史消息
}

/** /online 或 /login 返回的用户信息 */
export interface User {
  uid: string;
  username: string;
}

/** POST /login 的登录响应 */
export interface LoginResponse {
  uid: string;
  username: string;
  token: string;
}

/** 群组信息 —— 键与后端 JSON 字段名一致。 */
export interface GroupInfo {
  id: string;
  name: string;
  owner_uid: string;
  members: string[];
  member_count?: number;
  created_at: number;
}

/** /group/list 的群组列表项 */
export interface GroupListItem {
  id: string;
  name: string;
  owner_uid: string;
  member_count: number;
  created_at: number;
}

/** 文件上传响应 */
export interface UploadResponse {
  file_id: string;
  name: string;
  size: number;
  mime: string;
  width: number;
  height: number;
  thumb_width?: number;
  thumb_height?: number;
}

/** 嵌入 CmdFile 内容中的文件元数据 */
export interface FileMetadata {
  file_id: string;
  name: string;
  size: number;
  mime: string;
  width?: number;
  height?: number;
  text?: string; // 可选说明文字
}

/** 搜索参数 */
export interface SearchParams {
  q: string;
  peer?: string;
  chatType?: ChatTypeValue;
  msgType?: MsgTypeValue;
  before?: number;
  after?: number;
  cursor?: number;
  limit?: number;
}

/** 搜索结果消息 */
export interface SearchResultMessage {
  msg_id: string;
  cmd: number;
  from: string;
  to: string;
  chat_type: number;
  msg_type: number;
  content: string;
  timestamp: number;
  need_ack: boolean;
}

/** 搜索响应 */
export interface SearchResponse {
  query: string;
  messages: SearchResultMessage[];
  total: number;
  next_cursor: number;
}

/** 好友信息 */
export interface Friend {
  uid: string;
  friend_uid: string;
  status: number; // 0=待处理, 1=已接受, 2=已拒绝
  created_at: number;
}

/** 从接收方视角的好友请求 */
export interface FriendRequest {
  from_uid: string;
  username: string;
  created_at: number;
}

/** 嵌入消息内容中的回复元数据 */
export interface ReplyTo {
  msg_id: string;
  from: string;
  content: string;
}

/** 群组通知 (member_joined/member_left) */
export interface GroupNotification {
  type: 'member_joined' | 'member_left';
  group_id: string;
  uid: string;
}

// ---- 管理类型 ----

/** 管理后台系统统计 */
export interface AdminStats {
  status: string;
  online_users: number;
  total_users: number;
  total_messages: number;
  dependencies: Record<string, string>;
  memory: {
    alloc_mb: number;
    goroutines: number;
  };
}

/** 管理列表中的用户记录 */
export interface AdminUser {
  uid: string;
  username: string;
  role: string;
  is_disabled: boolean;
  created_at: number;
}

/** 分页用户列表响应 */
export interface AdminUsersResponse {
  users: AdminUser[];
  total: number;
  offset: number;
  limit: number;
}

/** 管理后台的消息浏览响应 */
export interface AdminBrowseResponse {
  messages: SearchResultMessage[];
  limit: number;
}

