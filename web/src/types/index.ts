// ============================================================
// IM Frontend TypeScript Types
// Mirrors the proto Message structure from api/proto/message.proto
// ============================================================

/** Command types (matching Go proto.CmdXxx constants) */
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

/** Chat types */
export const ChatType = {
  Single: 1,
  Group: 2,
} as const;

export type ChatTypeValue = (typeof ChatType)[keyof typeof ChatType];

/** Message content types */
export const MsgType = {
  Text: 1,
  Image: 2,
  Voice: 3,
  Video: 4,
  File: 5,
} as const;

export type MsgTypeValue = (typeof MsgType)[keyof typeof MsgType];

/**
 * Represents a decoded protobuf Message.
 * int64 fields (seq, msg_id, timestamp) use bigint in JS.
 * We store them as strings for safe JSON serialization and React keys.
 */
export interface IMMessage {
  seq: string; // client sequence (bigint → string)
  msgId: string; // server snowflake ID (bigint → string)
  cmd: CmdType;
  from: string; // sender UID (server-set)
  to: string; // receiver UID or group ID
  chatType: ChatTypeValue;
  msgType: MsgTypeValue;
  content: string; // plain text or JSON
  timestamp: string; // unix millisecond (bigint → string)
  needAck: boolean;
}

/** Pending/sent status for optimistic UI */
export type MessageStatus = 'sending' | 'sent' | 'failed' | 'read';

/** A message enriched with UI state */
export interface ChatMessage extends IMMessage {
  status: MessageStatus;
  recalled: boolean;
}

/** Conversation summary (built client-side from messages) */
export interface Conversation {
  peerId: string; // the other party's UID or group ID
  name: string; // display name
  chatType: ChatTypeValue;
  lastMessage: string; // preview text
  lastTime: number; // unix ms
  unread: number;
  messages: ChatMessage[];
  hasMore: boolean; // true if more history available
}

/** User info as returned by /online or /login */
export interface User {
  uid: string;
  username: string;
}

/** Login response from POST /login */
export interface LoginResponse {
  uid: string;
  username: string;
  token: string;
}

/** Group info — keys match backend JSON field names. */
export interface GroupInfo {
  id: string;
  name: string;
  owner_uid: string;
  members: string[];
  member_count?: number;
  created_at: number;
}

/** Group list item from /group/list */
export interface GroupListItem {
  id: string;
  name: string;
  owner_uid: string;
  member_count: number;
  created_at: number;
}

/** File upload response */
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

/** File metadata embedded in CmdFile content */
export interface FileMetadata {
  file_id: string;
  name: string;
  size: number;
  mime: string;
  width?: number;
  height?: number;
  text?: string; // optional caption
}

/** Search params */
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

/** Search result message */
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

/** Search response */
export interface SearchResponse {
  query: string;
  messages: SearchResultMessage[];
  total: number;
  next_cursor: number;
}

/** Friend info */
export interface Friend {
  uid: string;
  friend_uid: string;
  status: number; // 0=pending, 1=accepted, 2=rejected
  created_at: number;
}

/** Friend request from the receiver's perspective */
export interface FriendRequest {
  from_uid: string;
  username: string;
  created_at: number;
}

/** Reply metadata embedded in message content */
export interface ReplyTo {
  msg_id: string;
  from: string;
  content: string;
}

/** Group notification (member_joined/member_left) */
export interface GroupNotification {
  type: 'member_joined' | 'member_left';
  group_id: string;
  uid: string;
}

// ---- Admin types ----

/** Admin dashboard system stats */
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

/** User record for admin listing */
export interface AdminUser {
  uid: string;
  username: string;
  role: string;
  is_disabled: boolean;
  created_at: number;
}

/** Paginated user list response */
export interface AdminUsersResponse {
  users: AdminUser[];
  total: number;
  offset: number;
  limit: number;
}

/** Message browse response for admin */
export interface AdminBrowseResponse {
  messages: SearchResultMessage[];
  limit: number;
}

