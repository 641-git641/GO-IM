/**
 * 聊天 store —— 管理会话、消息和草稿。
 */

import { create } from 'zustand';
import type { ChatMessage, Conversation, ChatTypeValue, MsgTypeValue, CmdType } from '@/types';
import { Cmd, ChatType, MsgType } from '@/types';
import { bigintToString, formatTime, tryParseJSON } from '@/lib/utils';

interface ChatState {
  /** peerId → Conversation 的映射 */
  conversations: Map<string, Conversation>;
  /** 当前活跃会话 (peerId) */
  activePeer: string | null;
  /** 每个会话的草稿消息 */
  drafts: Map<string, string>;

  // 操作
  setActivePeer: (peerId: string | null) => void;
  setDraft: (peerId: string, text: string) => void;
  getDraft: (peerId: string) => string;

  /** 添加或更新会话记录 */
  upsertConversation: (peerId: string, name: string, chatType: ChatTypeValue) => void;

  /** 向会话添加一条收到的或发出的消息 */
  addMessage: (peerId: string, msg: ChatMessage, myUid: string) => void;

  /** 确认消息送达(已收到 ACK) */
  confirmMessage: (peerId: string, seq: string, msgId: string, timestamp: string) => void;

  /** 将消息标记为已撤回 */
  markRecalled: (peerId: string, msgId: string) => void;

  /** 向会话前置插入历史消息 */
  prependMessages: (peerId: string, messages: ChatMessage[]) => void;

  /** 将会话标记为没有更多历史消息 */
  setNoMoreHistory: (peerId: string) => void;

  /** 增加某个会话对象的未读数 */
  incrementUnread: (peerId: string) => void;

  /** 重置某个会话对象的未读数 */
  resetUnread: (peerId: string) => void;

  /** 获取未读总数 */
  totalUnread: () => number;

  /** 批量设置未读数(来自服务器响应) */
  setUnreadCounts: (counts: Record<string, number>) => void;

  /** 获取排序后的会话列表 */
  getConversationList: () => Conversation[];

  /** 删除会话(从列表中移除,不影响服务器) */
  deleteConversation: (peerId: string) => void;

  /** 仅从本地视图删除单条消息 */
  deleteMessage: (peerId: string, msgId: string) => void;

  /** 输入中指示器状态 (peerId → {uid, until}) */
  typingUsers: Map<string, { uid: string; until: number }>;
  /** 记录用户正在某个会话中输入 */
  setTyping: (peerId: string, uid: string) => void;
  /** 用于展示的活跃输入会话对象 ID */
  getActiveTyping: () => Map<string, string[]>;

  /** 将会话中的消息标记为已被指定用户已读 */
  markMessagesRead: (peerId: string, readerUid: string) => void;

  /** 手动将会话标记为未读 */
  markUnread: (peerId: string) => void;
}

export const useChatStore = create<ChatState>((set, get) => ({
  conversations: new Map(),
  activePeer: null,
  drafts: new Map(),
  typingUsers: new Map(),

  setActivePeer: (peerId) => set({ activePeer: peerId }),

  setDraft: (peerId, text) => {
    const drafts = new Map(get().drafts);
    if (text) {
      drafts.set(peerId, text);
    } else {
      drafts.delete(peerId);
    }
    set({ drafts });
  },

  getDraft: (peerId) => get().drafts.get(peerId) || '',

  upsertConversation: (peerId, name, chatType) => {
    const conversations = new Map(get().conversations);
    const existing = conversations.get(peerId);
    if (!existing) {
      conversations.set(peerId, {
        peerId,
        name,
        chatType,
        lastMessage: '',
        lastTime: Date.now(),
        unread: 0,
        messages: [],
        hasMore: true,
      });
      set({ conversations });
    } else if (name && name !== peerId && name !== existing.peerId && existing.name === existing.peerId) {
      // 若现有名称只是原始 peerId,则更新名称 (例如 "g_123" → "My Group")
      conversations.set(peerId, { ...existing, name });
      set({ conversations });
    }
  },

  addMessage: (peerId, msg, myUid) => {
    const conversations = new Map(get().conversations);
    const conv = conversations.get(peerId);
    if (!conv) return;

    // 检查重复 (按 msgId)
    if (msg.msgId !== '0' && conv.messages.some((m) => m.msgId === msg.msgId)) return;

    // 从 JSON 内容中提取展示文本,用于会话列表预览
    let previewText: string;
    if (msg.recalled) {
      previewText = '[消息已撤回]';
    } else if (msg.msgType === MsgType.Text) {
      const parsed = tryParseJSON<{ text?: string; type?: string; username?: string; name?: string; uid?: string }>(msg.content);
      if (parsed?.text) {
        previewText = parsed.text;
      } else if (parsed?.type === 'friend_request') {
        previewText = `${parsed.username || '用户'} 请求添加好友`;
      } else if (parsed?.type === 'friend_accepted') {
        previewText = '已同意好友请求';
      } else if (parsed?.type === 'group_created') {
        previewText = `群组 "${parsed.name || ''}" 已创建`;
      } else if (parsed?.type === 'member_joined') {
        previewText = `${parsed.uid || '用户'} 加入了群聊`;
      } else if (parsed?.type === 'member_left') {
        previewText = `${parsed.uid || '用户'} 退出了群聊`;
      } else {
        previewText = msg.content;
      }
    } else {
      previewText = `[${msg.msgType === MsgType.Image ? '图片' : msg.msgType === MsgType.Voice ? '语音' : msg.msgType === MsgType.Video ? '视频' : '文件'}]`;
    }

    const updated = {
      ...conv,
      messages: [...conv.messages, msg],
      lastMessage: previewText,
      lastTime: Number(msg.timestamp) || Date.now(),
      // 仅当不是当前活跃会话且非本人消息时才增加未读数
      unread:
        get().activePeer === peerId || msg.from === myUid
          ? conv.unread
          : conv.unread + 1,
    };

    conversations.set(peerId, updated);
    set({ conversations });
  },

  confirmMessage: (peerId, seq, msgId, timestamp) => {
    const conversations = new Map(get().conversations);
    const conv = conversations.get(peerId);
    if (!conv) return;

    const messages = conv.messages.map((m) => {
      if (m.seq === seq && m.status === 'sending') {
        return { ...m, msgId, timestamp, status: 'sent' as const };
      }
      return m;
    });

    conversations.set(peerId, { ...conv, messages });
    set({ conversations });
  },

  markRecalled: (peerId, msgId) => {
    const conversations = new Map(get().conversations);
    const conv = conversations.get(peerId);
    if (!conv) return;

    const messages = conv.messages.map((m) => {
      if (m.msgId === msgId) {
        return { ...m, recalled: true, content: '消息已撤回' };
      }
      return m;
    });

    conversations.set(peerId, { ...conv, messages });
    set({ conversations });
  },

  prependMessages: (peerId, messages) => {
    const conversations = new Map(get().conversations);
    const conv = conversations.get(peerId);
    if (!conv) return;

    // 过滤掉已存在的消息
    const existingIds = new Set(conv.messages.map((m) => m.msgId));
    const newMsgs = messages.filter((m) => !existingIds.has(m.msgId));

    if (newMsgs.length === 0) return;

    conversations.set(peerId, {
      ...conv,
      messages: [...newMsgs, ...conv.messages],
    });
    set({ conversations });
  },

  setNoMoreHistory: (peerId) => {
    const conversations = new Map(get().conversations);
    const conv = conversations.get(peerId);
    if (conv) {
      conversations.set(peerId, { ...conv, hasMore: false });
      set({ conversations });
    }
  },

  incrementUnread: (peerId) => {
    const conversations = new Map(get().conversations);
    const conv = conversations.get(peerId);
    if (conv && get().activePeer !== peerId) {
      conversations.set(peerId, { ...conv, unread: conv.unread + 1 });
      set({ conversations });
    }
  },

  resetUnread: (peerId) => {
    const conversations = new Map(get().conversations);
    const conv = conversations.get(peerId);
    if (conv && conv.unread > 0) {
      conversations.set(peerId, { ...conv, unread: 0 });
      set({ conversations });
    }
  },

  setUnreadCounts: (counts) => {
    const conversations = new Map(get().conversations);
    for (const [peerId, count] of Object.entries(counts)) {
      const conv = conversations.get(peerId);
      if (conv) {
        conversations.set(peerId, { ...conv, unread: count });
      }
    }
    set({ conversations });
  },

  totalUnread: () => {
    let total = 0;
    for (const conv of get().conversations.values()) {
      total += conv.unread;
    }
    return total;
  },

  getConversationList: () => {
    return Array.from(get().conversations.values()).sort(
      (a, b) => b.lastTime - a.lastTime,
    );
  },

  deleteConversation: (peerId) => {
    const conversations = new Map(get().conversations);
    conversations.delete(peerId);
    const drafts = new Map(get().drafts);
    drafts.delete(peerId);
    const nextActive =
      get().activePeer === peerId ? null : get().activePeer;
    set({ conversations, drafts, activePeer: nextActive });
  },

  deleteMessage: (peerId, msgId) => {
    const conversations = new Map(get().conversations);
    const conv = conversations.get(peerId);
    if (!conv) return;
    conversations.set(peerId, {
      ...conv,
      messages: conv.messages.filter((m) => m.msgId !== msgId),
    });
    set({ conversations });
  },

  setTyping: (peerId, uid) => {
    const typingUsers = new Map(get().typingUsers);
    typingUsers.set(peerId, { uid, until: Date.now() + 5000 }); // 5 秒超时
    set({ typingUsers });
  },

  getActiveTyping: () => {
    const now = Date.now();
    const active = new Map<string, string[]>();
    for (const [peerId, info] of get().typingUsers) {
      if (info.until > now) {
        const uids = active.get(peerId) || [];
        uids.push(info.uid);
        active.set(peerId, uids);
      }
    }
    return active;
  },

  markMessagesRead: (peerId, readerUid) => {
    const conversations = new Map(get().conversations);
    const conv = conversations.get(peerId);
    if (!conv) return;

    // 将我发出的所有消息标记为已读(已读者已看到它们)
    const messages = conv.messages.map((m) => {
      if (m.from !== readerUid && !m.recalled && m.status === 'sent') {
        return { ...m, status: 'read' as const };
      }
      return m;
    });

    conversations.set(peerId, { ...conv, messages });
    set({ conversations });
  },

  markUnread: (peerId) => {
    const conversations = new Map(get().conversations);
    const conv = conversations.get(peerId);
    if (conv && conv.unread === 0 && get().activePeer !== peerId) {
      conversations.set(peerId, { ...conv, unread: 1 });
      set({ conversations });
    }
  },
}));
