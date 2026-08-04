/**
 * 认证辅助函数:token 存储与 JWT 工具。
 */

const TOKEN_KEY = 'im_token';
const UID_KEY = 'im_uid';
const USERNAME_KEY = 'im_username';

export function getToken(): string | null {
  return sessionStorage.getItem(TOKEN_KEY);
}

export function getStoredUid(): string | null {
  return sessionStorage.getItem(UID_KEY);
}

export function getStoredUsername(): string | null {
  return sessionStorage.getItem(USERNAME_KEY);
}

export function saveAuth(uid: string, username: string, token: string): void {
  sessionStorage.setItem(TOKEN_KEY, token);
  sessionStorage.setItem(UID_KEY, uid);
  sessionStorage.setItem(USERNAME_KEY, username);
}

export function clearAuth(): void {
  sessionStorage.removeItem(TOKEN_KEY);
  sessionStorage.removeItem(UID_KEY);
  sessionStorage.removeItem(USERNAME_KEY);
}

/** 检查存储的 JWT 是否已过期(简单的客户端检查) */
export function isTokenExpired(): boolean {
  const token = getToken();
  if (!token) return true;

  try {
    // JWT 载荷是第二个分段 (base64url)
    // JWT 使用 base64url(非标准 base64):将 - → +、_ → /
    const base64 = token.split('.')[1].replace(/-/g, '+').replace(/_/g, '/');
    const payload = JSON.parse(atob(base64));
    const exp = payload.exp * 1000; // JWT exp 单位为秒
    return Date.now() >= exp;
  } catch {
    return true; // 无法解析的 token → 视为已过期
  }
}
