/**
 * Chat store — manages conversations, messages, and drafts.
 */

import { create } from 'zustand';
import type { ChatMessage, Conversation, ChatTypeValue, MsgTypeValue, CmdType } from '@/types';
import { Cmd, ChatType, MsgType } from '@/types';
import { bigintToString, formatTime } from '@/lib/utils';

interface ChatState {
  /** Map of peerId → Conversation */
  conversations: Map<string, Conversation>;
  /** Currently active conversation (peerId) */
  activePeer: string | null;
  /** Draft messages per conversation */
  drafts: Map<string, string>;

  // Actions
  setActivePeer: (peerId: string | null) => void;
  setDraft: (peerId: string, text: string) => void;
  getDraft: (peerId: string) => string;

  /** Add or update a conversation record */
  upsertConversation: (peerId: string, name: string, chatType: ChatTypeValue) => void;

  /** Add an incoming or sent message to a conversation */
  addMessage: (peerId: string, msg: ChatMessage, myUid: string) => void;

  /** Confirm message delivery (ACK received) */
  confirmMessage: (peerId: string, seq: string, msgId: string, timestamp: string) => void;

  /** Mark message as recalled */
  markRecalled: (peerId: string, msgId: string) => void;

  /** Prepend history messages to a conversation */
  prependMessages: (peerId: string, messages: ChatMessage[]) => void;

  /** Mark conversation as having no more history */
  setNoMoreHistory: (peerId: string) => void;

  /** Increment unread for a peer */
  incrementUnread: (peerId: string) => void;

  /** Reset unread for a peer */
  resetUnread: (peerId: string) => void;

  /** Get unread total */
  totalUnread: () => number;

  /** Set unread counts in bulk (from server response) */
  setUnreadCounts: (counts: Record<string, number>) => void;

  /** Get sorted conversation list */
  getConversationList: () => Conversation[];

  /** Delete a conversation (removes from list, does not affect server) */
  deleteConversation: (peerId: string) => void;

  /** Delete a single message from local view only */
  deleteMessage: (peerId: string, msgId: string) => void;

  /** Typing indicator state (peerId → {uid, until}) */
  typingUsers: Map<string, { uid: string; until: number }>;
  /** Record that a user is typing in a conversation */
  setTyping: (peerId: string, uid: string) => void;
  /** Active typing peerIds for display */
  getActiveTyping: () => Map<string, string[]>;

  /** Mark messages in a conversation as read by a specific user */
  markMessagesRead: (peerId: string, readerUid: string) => void;

  /** Manually mark a conversation as unread */
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
    if (!conversations.has(peerId)) {
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
    }
  },

  addMessage: (peerId, msg, myUid) => {
    const conversations = new Map(get().conversations);
    const conv = conversations.get(peerId);
    if (!conv) return;

    // Check for duplicate (by msgId)
    if (msg.msgId !== '0' && conv.messages.some((m) => m.msgId === msg.msgId)) return;

    const updated = {
      ...conv,
      messages: [...conv.messages, msg],
      lastMessage: msg.recalled
        ? '[消息已撤回]'
        : msg.msgType === MsgType.Text
          ? msg.content
          : `[${msg.msgType === MsgType.Image ? '图片' : msg.msgType === MsgType.Voice ? '语音' : msg.msgType === MsgType.Video ? '视频' : '文件'}]`,
      lastTime: Number(msg.timestamp) || Date.now(),
      // Only increment unread if NOT the active conversation and NOT from self
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

    // Filter out messages already present
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
    typingUsers.set(peerId, { uid, until: Date.now() + 5000 }); // 5s timeout
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

    // Mark all messages from me as read (the reader has seen them)
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
