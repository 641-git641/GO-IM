// API 与连接常量

/** HTTP API 调用的基础 URL(开发环境使用 Vite 代理) */
export const API_BASE = '';

/** WebSocket URL —— 从当前地址推导 */
export function getWsURL(): string {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  return `${proto}//${location.host}/ws`;
}

/** 重连退避间隔 (ms) */
export const RECONNECT_DELAYS = [1000, 2000, 4000, 8000, 16000, 30000];

/** 最大重连延迟 */
export const MAX_RECONNECT_DELAY = 30000;

/** 默认历史记录分页大小 */
export const HISTORY_LIMIT = 30;

/** 最大历史记录分页大小 */
export const HISTORY_LIMIT_MAX = 100;

/** 消息撤回时间窗口 (ms) —— 2 分钟 */
export const RECALL_WINDOW_MS = 2 * 60 * 1000;

/** 消息编辑时间窗口 (ms) —— 5 分钟 */
export const EDIT_WINDOW_MS = 5 * 60 * 1000;

/** 最大文件上传大小 (10 MB) */
export const MAX_UPLOAD_SIZE = 10 * 1024 * 1024;
