import { create } from 'zustand';

export type WSStatus = 'connecting' | 'connected' | 'disconnected';

interface WSState {
  status: WSStatus;
  setStatus: (status: WSStatus) => void;
}

export const useWSStore = create<WSState>((set) => ({
  status: 'disconnected',
  setStatus: (status) => set({ status }),
}));
