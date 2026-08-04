/**
 * int64 ↔ string 转换、日期格式化与 JSON 解析的工具函数。
 *
 * 重要:来自 @bufbuild/protobuf 的 proto int64 字段 (seq、msg_id、timestamp)
 * 会以 bigint 形式到达。我们会立即将它们转换为字符串,以避免精度损失,
 * 并确保它们能被 Zustand/JSON 安全序列化。
 */

/** 将 bigint(或 number)转换为安全的字符串 */
export function bigintToString(n: bigint | number | string): string {
  if (typeof n === 'string') return n;
  return String(n);
}

/** 将字符串转回 bigint,用于构造消息 */
export function stringToBigint(s: string): bigint {
  return BigInt(s);
}

/** 将 Unix 毫秒时间戳字符串格式化为 HH:mm */
export function formatTime(ts: string | number | bigint): string {
  const ms = typeof ts === 'bigint' ? Number(ts) : typeof ts === 'string' ? Number(ts) : ts;
  const d = new Date(ms);
  return d.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });
}

/** 将 Unix 毫秒时间戳格式化为相对或绝对日期 */
export function formatDate(ts: string | number | bigint): string {
  const ms = typeof ts === 'bigint' ? Number(ts) : typeof ts === 'string' ? Number(ts) : ts;
  const d = new Date(ms);
  const now = new Date();
  const diff = now.getTime() - ms;

  // 今天:显示时间
  if (diff < 24 * 3600 * 1000 && d.getDate() === now.getDate()) {
    return d.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });
  }
  // 昨天:比较完整日期(年-月-日),而不仅是当月的第几天
  const yesterday = new Date(now);
  yesterday.setDate(yesterday.getDate() - 1);
  if (
    d.getFullYear() === yesterday.getFullYear() &&
    d.getMonth() === yesterday.getMonth() &&
    d.getDate() === yesterday.getDate()
  ) {
    return '昨天';
  }
  // 今年
  if (d.getFullYear() === now.getFullYear()) {
    return d.toLocaleDateString('zh-CN', { month: '2-digit', day: '2-digit' });
  }
  return d.toLocaleDateString('zh-CN', { year: 'numeric', month: '2-digit', day: '2-digit' });
}

/** 尝试将字符串解析为 JSON;若不是合法 JSON 则返回 null */
export function tryParseJSON<T = unknown>(s: string): T | null {
  try {
    return JSON.parse(s);
  } catch {
    return null;
  }
}

/** 检查内容是否为群组通知 */
export function isGroupNotification(
  content: string,
): import('@/types').GroupNotification | null {
  const parsed = tryParseJSON<{ type: string; group_id: string; uid: string }>(content);
  if (parsed && (parsed.type === 'member_joined' || parsed.type === 'member_left')) {
    return parsed as import('@/types').GroupNotification;
  }
  return null;
}

/**
 * 检查内容是否为系统级消息(群组通知、好友请求、群组创建等),
 * 此类消息应渲染为居中通知而不是聊天气泡。
 */
export function isSystemMessage(content: string): string | null {
  const parsed = tryParseJSON<{ type?: string }>(content);
  if (!parsed?.type) return null;

  const systemTypes = [
    'member_joined',
    'member_left',
    'friend_request',
    'friend_accepted',
    'group_created',
  ];
  return systemTypes.includes(parsed.type) ? parsed.type : null;
}

/** 检查消息是否为已撤回消息 */
export function isRecalled(content: string): boolean {
  const parsed = tryParseJSON<{ recalled: boolean }>(content);
  return parsed?.recalled === true;
}

/** 根据对方 ID 或群组信息生成会话显示名称 */
export function getConversationName(
  peerId: string,
  myUid: string,
  groupName?: string,
): string {
  if (peerId.startsWith('g_')) return groupName || peerId;
  return peerId === myUid ? '我' : peerId;
}

/** 根据名称生成头像字母 */
export function getAvatarLetters(name: string): string {
  return name.slice(0, 2).toUpperCase();
}

/** 获取用于展示的 MIME 类型分类 */
export function getFileCategory(mime: string): 'image' | 'video' | 'audio' | 'file' {
  if (mime.startsWith('image/')) return 'image';
  if (mime.startsWith('video/')) return 'video';
  if (mime.startsWith('audio/')) return 'audio';
  return 'file';
}

/** 构造文件下载 URL */
export function getFileURL(
  fileId: string,
  uid: string,
  token: string,
  thumb = false,
): string {
  const params = new URLSearchParams({ id: fileId, uid, token });
  if (thumb) params.set('thumb', '1');
  return `${location.origin}/file?${params.toString()}`;
}
