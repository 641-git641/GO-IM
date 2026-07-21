/**
 * Utilities for int64 ↔ string conversion, date formatting, and JSON parsing.
 *
 * IMPORTANT: proto int64 fields (seq, msg_id, timestamp) arrive as bigint
 * from @bufbuild/protobuf. We convert them to strings immediately to avoid
 * precision loss and to keep them safely serializable for Zustand/JSON.
 */

/** Convert a bigint (or number) to a safe string */
export function bigintToString(n: bigint | number | string): string {
  if (typeof n === 'string') return n;
  return String(n);
}

/** Convert a string back to bigint for message construction */
export function stringToBigint(s: string): bigint {
  return BigInt(s);
}

/** Format a unix millisecond timestamp string to HH:mm */
export function formatTime(ts: string | number | bigint): string {
  const ms = typeof ts === 'bigint' ? Number(ts) : typeof ts === 'string' ? Number(ts) : ts;
  const d = new Date(ms);
  return d.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });
}

/** Format a unix millisecond timestamp to a relative or absolute date */
export function formatDate(ts: string | number | bigint): string {
  const ms = typeof ts === 'bigint' ? Number(ts) : typeof ts === 'string' ? Number(ts) : ts;
  const d = new Date(ms);
  const now = new Date();
  const diff = now.getTime() - ms;

  // Today: show time
  if (diff < 24 * 3600 * 1000 && d.getDate() === now.getDate()) {
    return d.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });
  }
  // Yesterday: compare full date (year-month-day), not just day-of-month
  const yesterday = new Date(now);
  yesterday.setDate(yesterday.getDate() - 1);
  if (
    d.getFullYear() === yesterday.getFullYear() &&
    d.getMonth() === yesterday.getMonth() &&
    d.getDate() === yesterday.getDate()
  ) {
    return '昨天';
  }
  // This year
  if (d.getFullYear() === now.getFullYear()) {
    return d.toLocaleDateString('zh-CN', { month: '2-digit', day: '2-digit' });
  }
  return d.toLocaleDateString('zh-CN', { year: 'numeric', month: '2-digit', day: '2-digit' });
}

/** Try to parse a string as JSON; return null if not valid JSON */
export function tryParseJSON<T = unknown>(s: string): T | null {
  try {
    return JSON.parse(s);
  } catch {
    return null;
  }
}

/** Check if content is a group notification */
export function isGroupNotification(
  content: string,
): import('@/types').GroupNotification | null {
  const parsed = tryParseJSON<{ type: string; group_id: string; uid: string }>(content);
  if (parsed && (parsed.type === 'member_joined' || parsed.type === 'member_left')) {
    return parsed as import('@/types').GroupNotification;
  }
  return null;
}

/** Check if a message is a recalled message */
export function isRecalled(content: string): boolean {
  const parsed = tryParseJSON<{ recalled: boolean }>(content);
  return parsed?.recalled === true;
}

/** Generate a conversation display name from peer ID or group info */
export function getConversationName(
  peerId: string,
  myUid: string,
  groupName?: string,
): string {
  if (peerId.startsWith('g_')) return groupName || peerId;
  return peerId === myUid ? '我' : peerId;
}

/** Generate avatar letters from a name */
export function getAvatarLetters(name: string): string {
  return name.slice(0, 2).toUpperCase();
}

/** Get MIME type category for display */
export function getFileCategory(mime: string): 'image' | 'video' | 'audio' | 'file' {
  if (mime.startsWith('image/')) return 'image';
  if (mime.startsWith('video/')) return 'video';
  if (mime.startsWith('audio/')) return 'audio';
  return 'file';
}

/** Build a file download URL */
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
