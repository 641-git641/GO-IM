import { create } from 'zustand';
import { saveAuth, clearAuth, getStoredUid, getStoredUsername, getToken, isTokenExpired } from '@/lib/auth';

/** 从 JWT 载荷中解码角色字段。 */
function getRoleFromToken(token: string): boolean {
  try {
    // JWT 使用 base64url(非标准 base64):将 - → +、_ → /
    const base64 = token.split('.')[1].replace(/-/g, '+').replace(/_/g, '/');
    const payload = JSON.parse(atob(base64));
    return payload.role === 'admin';
  } catch {
    return false;
  }
}

interface AuthState {
  uid: string;
  username: string;
  token: string;
  isLoggedIn: boolean;
  isAdmin: boolean;

  login: (uid: string, username: string, token: string) => void;
  logout: () => void;
  restore: () => boolean;
}

export const useAuthStore = create<AuthState>((set) => ({
  uid: '',
  username: '',
  token: '',
  isLoggedIn: false,
  isAdmin: false,

  login: (uid, username, token) => {
    saveAuth(uid, username, token);
    set({ uid, username, token, isLoggedIn: true, isAdmin: getRoleFromToken(token) });
  },

  logout: () => {
    clearAuth();
    set({ uid: '', username: '', token: '', isLoggedIn: false, isAdmin: false });
  },

  restore: () => {
    const uid = getStoredUid();
    const username = getStoredUsername();
    const token = getToken();
    if (uid && token && !isTokenExpired()) {
      set({
        uid,
        username: username || uid,
        token,
        isLoggedIn: true,
        isAdmin: getRoleFromToken(token),
      });
      return true;
    }
    return false;
  },
}));
