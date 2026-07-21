import { create } from 'zustand';
import type { AdminStats, AdminUser, SearchResultMessage } from '@/types';
import {
  getAdminStats,
  getAdminUsers,
  deleteAdminUser,
  getAdminMessages,
  deleteAdminMessage,
} from '@/lib/api';

interface AdminState {
  // Dashboard
  stats: AdminStats | null;
  statsLoading: boolean;
  statsError: string | null;

  // User management
  users: AdminUser[];
  usersTotal: number;
  usersLoading: boolean;
  usersError: string | null;

  // Content moderation
  messages: SearchResultMessage[];
  messagesLoading: boolean;
  messagesError: string | null;

  // Active tab
  activeTab: 'dashboard' | 'users' | 'messages';

  // Actions
  setActiveTab: (tab: 'dashboard' | 'users' | 'messages') => void;
  fetchStats: (uid: string, token: string) => Promise<void>;
  fetchUsers: (uid: string, token: string, offset?: number, limit?: number) => Promise<void>;
  removeUser: (uid: string, token: string, targetUid: string) => Promise<void>;
  fetchMessages: (uid: string, token: string, before?: number, limit?: number) => Promise<void>;
  removeMessage: (uid: string, token: string, msgId: string) => Promise<void>;
}

export const useAdminStore = create<AdminState>((set, get) => ({
  stats: null,
  statsLoading: false,
  statsError: null,

  users: [],
  usersTotal: 0,
  usersLoading: false,
  usersError: null,

  messages: [],
  messagesLoading: false,
  messagesError: null,

  activeTab: 'dashboard',

  setActiveTab: (tab) => set({ activeTab: tab }),

  fetchStats: async (uid, token) => {
    set({ statsLoading: true, statsError: null });
    try {
      const stats = await getAdminStats(uid, token);
      set({ stats, statsLoading: false });
    } catch (e) {
      set({ statsError: (e as Error).message, statsLoading: false });
    }
  },

  fetchUsers: async (uid, token, offset = 0, limit = 50) => {
    set({ usersLoading: true, usersError: null });
    try {
      const data = await getAdminUsers(uid, token, offset, limit);
      set({ users: data.users, usersTotal: data.total, usersLoading: false });
    } catch (e) {
      set({ usersError: (e as Error).message, usersLoading: false });
    }
  },

  removeUser: async (uid, token, targetUid) => {
    await deleteAdminUser(uid, token, targetUid);
    await get().fetchUsers(uid, token);
  },

  fetchMessages: async (uid, token, before = 0, limit = 50) => {
    set({ messagesLoading: true, messagesError: null });
    try {
      const data = await getAdminMessages(uid, token, before, limit);
      set({ messages: data.messages, messagesLoading: false });
    } catch (e) {
      set({ messagesError: (e as Error).message, messagesLoading: false });
    }
  },

  removeMessage: async (uid, token, msgId) => {
    await deleteAdminMessage(uid, token, msgId);
    await get().fetchMessages(uid, token);
  },
}));
