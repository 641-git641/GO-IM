/**
 * Auth helpers: token storage and JWT utilities.
 */

const TOKEN_KEY = 'im_token';
const UID_KEY = 'im_uid';
const USERNAME_KEY = 'im_username';

export function getToken(): string | null {
  return localStorage.getItem(TOKEN_KEY);
}

export function getStoredUid(): string | null {
  return localStorage.getItem(UID_KEY);
}

export function getStoredUsername(): string | null {
  return localStorage.getItem(USERNAME_KEY);
}

export function saveAuth(uid: string, username: string, token: string): void {
  localStorage.setItem(TOKEN_KEY, token);
  localStorage.setItem(UID_KEY, uid);
  localStorage.setItem(USERNAME_KEY, username);
}

export function clearAuth(): void {
  localStorage.removeItem(TOKEN_KEY);
  localStorage.removeItem(UID_KEY);
  localStorage.removeItem(USERNAME_KEY);
}

/** Check if the stored JWT is expired (simple client-side check) */
export function isTokenExpired(): boolean {
  const token = getToken();
  if (!token) return true;

  try {
    // JWT payload is the second segment (base64url)
    const payload = JSON.parse(atob(token.split('.')[1]));
    const exp = payload.exp * 1000; // JWT exp is in seconds
    return Date.now() >= exp;
  } catch {
    return true; // unparseable token → treat as expired
  }
}
