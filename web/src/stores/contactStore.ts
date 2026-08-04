import { create } from 'zustand';
import type { GroupInfo, GroupListItem } from '@/types';

interface ContactState {
  onlineUsers: string[];
  groups: GroupListItem[];
  /** 群组详细信息缓存(以群组 ID 为键),供 GroupInfoPanel 使用 */
  groupDetails: Map<string, GroupInfo>;

  setOnlineUsers: (users: string[]) => void;
  addOnlineUser: (uid: string) => void;
  removeOnlineUser: (uid: string) => void;
  setGroups: (groups: GroupListItem[]) => void;
  addGroup: (group: GroupListItem) => void;
  removeGroup: (groupId: string) => void;
  setGroupDetail: (info: GroupInfo) => void;
  getGroupDetail: (groupId: string) => GroupInfo | undefined;
}

export const useContactStore = create<ContactState>((set, get) => ({
  onlineUsers: [],
  groups: [],
  groupDetails: new Map(),

  setOnlineUsers: (users) => set({ onlineUsers: users }),
  addOnlineUser: (uid) =>
    set((s) => ({
      onlineUsers: s.onlineUsers.includes(uid) ? s.onlineUsers : [...s.onlineUsers, uid],
    })),
  removeOnlineUser: (uid) =>
    set((s) => ({
      onlineUsers: s.onlineUsers.filter((u) => u !== uid),
    })),
  setGroups: (groups) => set({ groups }),

  addGroup: (group) =>
    set((s) => {
      const exists = s.groups.some((g) => g.id === group.id);
      if (exists) {
        return {
          groups: s.groups.map((g) => (g.id === group.id ? group : g)),
        };
      }
      return { groups: [...s.groups, group] };
    }),

  removeGroup: (groupId) =>
    set((s) => ({
      groups: s.groups.filter((g) => g.id !== groupId),
    })),

  setGroupDetail: (info) => {
    const groupDetails = new Map(get().groupDetails);
    groupDetails.set(info.id, info);
    set({ groupDetails });
  },

  getGroupDetail: (groupId) => get().groupDetails.get(groupId),
}));
