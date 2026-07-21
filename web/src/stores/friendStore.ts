import { create } from 'zustand';
import type { Friend, FriendRequest } from '@/types';

interface FriendState {
  friends: Friend[];
  pendingRequests: FriendRequest[];

  setFriends: (friends: Friend[]) => void;
  addFriend: (friend: Friend) => void;
  removeFriend: (friendUid: string) => void;
  setPendingRequests: (requests: FriendRequest[]) => void;
  addPendingRequest: (request: FriendRequest) => void;
  removePendingRequest: (fromUid: string) => void;
}

export const useFriendStore = create<FriendState>((set, get) => ({
  friends: [],
  pendingRequests: [],

  setFriends: (friends) => set({ friends }),
  addFriend: (friend) =>
    set((s) => {
      const exists = s.friends.some(
        (f) => f.uid === friend.uid,
      );
      if (exists) return s;
      return { friends: [...s.friends, friend] };
    }),
  removeFriend: (friendUid) =>
    set((s) => ({
      friends: s.friends.filter((f) => f.uid !== friendUid),
    })),
  setPendingRequests: (requests) => set({ pendingRequests: requests }),
  addPendingRequest: (request) =>
    set((s) => {
      const exists = s.pendingRequests.some(
        (r) => r.from_uid === request.from_uid,
      );
      if (exists) return s;
      return { pendingRequests: [...s.pendingRequests, request] };
    }),
  removePendingRequest: (fromUid) =>
    set((s) => ({
      pendingRequests: s.pendingRequests.filter(
        (r) => r.from_uid !== fromUid,
      ),
    })),
}));
