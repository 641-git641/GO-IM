// API and connection constants

/** Base URL for HTTP API calls (use Vite proxy in dev) */
export const API_BASE = '';

/** WebSocket URL — derive from current location */
export function getWsURL(): string {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  return `${proto}//${location.host}/ws`;
}

/** Reconnection backoff (ms) */
export const RECONNECT_DELAYS = [1000, 2000, 4000, 8000, 16000, 30000];

/** Maximum reconnect delay */
export const MAX_RECONNECT_DELAY = 30000;

/** Default history page size */
export const HISTORY_LIMIT = 30;

/** Maximum history page size */
export const HISTORY_LIMIT_MAX = 100;

/** Message recall window (ms) — 2 minutes */
export const RECALL_WINDOW_MS = 2 * 60 * 1000;

/** Message edit window (ms) — 5 minutes */
export const EDIT_WINDOW_MS = 5 * 60 * 1000;

/** Max file upload size (10 MB) */
export const MAX_UPLOAD_SIZE = 10 * 1024 * 1024;
