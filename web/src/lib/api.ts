/**
 * HTTP API client for REST endpoints.
 */

import type { LoginResponse, User, GroupInfo, GroupListItem, UploadResponse, SearchResponse, SearchParams, AdminStats, AdminUsersResponse, AdminBrowseResponse } from '@/types';
import { getToken, getStoredUid } from './auth';

type RequestMethod = 'GET' | 'POST';

function getAuthParams(): URLSearchParams {
  const uid = localStorage.getItem('im-uid') || '';
  const token = localStorage.getItem('im-token') || '';
  return new URLSearchParams({ uid, token });
}

async function request<T>(
  path: string,
  method: RequestMethod = 'GET',
  body?: URLSearchParams | FormData,
): Promise<T> {
  const headers: Record<string, string> = {};

  // Attach auth params (uid + token) to every request.
  const authParams = getAuthParams();
  let url = path;
  if (method === 'GET') {
    // Append auth to query string.
    const sep = path.includes('?') ? '&' : '?';
    url = path + sep + authParams.toString();
  } else if (body instanceof URLSearchParams) {
    // Merge auth into the form body.
    authParams.forEach((value, key) => { body.set(key, value); });
  }
  // FormData: auth is NOT injected — callers must pass uid/token manually.

  if (body instanceof URLSearchParams) {
    headers['Content-Type'] = 'application/x-www-form-urlencoded';
  }
  // FormData sets its own Content-Type with boundary

  const res = await fetch(url, {
    method,
    headers,
    body: body instanceof URLSearchParams ? body.toString() : (body as FormData),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`API ${method} ${path} failed (${res.status}): ${text}`);
  }

  return res.json();
}

// ---- Auth ----

export async function login(uid: string, username?: string, password?: string): Promise<LoginResponse> {
  const params = new URLSearchParams({ uid });
  if (username) params.set('username', username);
  if (password) params.set('password', password);
  return request<LoginResponse>('/login', 'POST', params);
}

export async function register(uid: string, username: string, password: string): Promise<LoginResponse> {
  const params = new URLSearchParams({ uid, username, password });
  return request<LoginResponse>('/register', 'POST', params);
}

export async function changePassword(uid: string, oldPassword: string, newPassword: string): Promise<{ status: string }> {
  const params = new URLSearchParams({ uid, old_password: oldPassword, new_password: newPassword });
  return request('/change-password', 'POST', params);
}

// ---- Users ----

export async function getOnlineUsers(): Promise<{ count: number; users: string[] }> {
  return request('/online');
}

export async function getHealth(): Promise<{
  status: string;
  connections: number;
  dependencies: Record<string, string>;
  memory: { alloc_mb: number; goroutines: number };
}> {
  return request('/health');
}

// ---- Groups ----

export async function createGroup(uid: string, name: string): Promise<GroupInfo> {
  const params = new URLSearchParams({ uid, name });
  return request('/group/create', 'POST', params);
}

export async function joinGroup(uid: string, groupId: string): Promise<{ ok: string }> {
  const params = new URLSearchParams({ uid, group_id: groupId });
  return request('/group/join', 'POST', params);
}

export async function leaveGroup(uid: string, groupId: string): Promise<{ ok: string }> {
  const params = new URLSearchParams({ uid, group_id: groupId });
  return request('/group/leave', 'POST', params);
}

export async function kickGroupMember(uid: string, groupId: string, targetUid: string): Promise<{ ok: string }> {
  const params = new URLSearchParams({ uid, group_id: groupId, target_uid: targetUid });
  return request('/group/kick', 'POST', params);
}

export async function renameGroup(uid: string, groupId: string, name: string): Promise<{ ok: string; name: string }> {
  const params = new URLSearchParams({ uid, group_id: groupId, name });
  return request('/group/rename', 'POST', params);
}

export async function transferGroup(uid: string, groupId: string, toUid: string): Promise<{ ok: string; owner_uid: string }> {
  const params = new URLSearchParams({ uid, group_id: groupId, to_uid: toUid });
  return request('/group/transfer', 'POST', params);
}

export async function getGroupMembers(groupId: string): Promise<{ group_id: string; members: string[] }> {
  return request(`/group/members?group_id=${encodeURIComponent(groupId)}`);
}

export async function getGroupList(uid: string): Promise<{ groups: GroupListItem[] }> {
  return request(`/group/list?uid=${encodeURIComponent(uid)}`);
}

// ---- Files ----

export async function uploadFile(
  file: File,
  uid: string,
  token: string,
  onProgress?: (pct: number) => void,
): Promise<UploadResponse> {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    const formData = new FormData();
    formData.append('uid', uid);
    formData.append('token', token);
    formData.append('file', file);

    xhr.upload.onprogress = (e) => {
      if (e.lengthComputable && onProgress) {
        onProgress(Math.round((e.loaded / e.total) * 100));
      }
    };

    xhr.onload = () => {
      if (xhr.status === 200) {
        try {
          resolve(JSON.parse(xhr.responseText));
        } catch {
          reject(new Error('Invalid JSON response'));
        }
      } else {
        reject(new Error(`Upload failed: ${xhr.status} ${xhr.responseText}`));
      }
    };

    xhr.onerror = () => reject(new Error('Upload network error'));
    xhr.open('POST', '/upload');
    xhr.send(formData);
  });
}

export function getFileURL(fileId: string, uid: string, token: string, thumb = false): string {
  const params = new URLSearchParams({ id: fileId, uid, token });
  if (thumb) params.set('thumb', '1');
  return `/file?${params.toString()}`;
}

// ---- Search ----

export async function searchMessages(
  uid: string,
  token: string,
  params: SearchParams,
): Promise<SearchResponse> {
  const sp = new URLSearchParams({ uid, token, q: params.q });
  if (params.peer) sp.set('peer', params.peer);
  if (params.chatType) sp.set('chat_type', String(params.chatType));
  if (params.msgType) sp.set('msg_type', String(params.msgType));
  if (params.before) sp.set('before', String(params.before));
  if (params.after) sp.set('after', String(params.after));
  if (params.cursor) sp.set('cursor', String(params.cursor));
  if (params.limit) sp.set('limit', String(params.limit));

  return request(`/search?${sp.toString()}`);
}

// ---- Friends ----

export interface FriendListResponse {
  uid: string;
  friends: { uid: string; friend_uid: string; status: number; created_at: number }[];
  pending_requests: { from_uid: string; username: string; created_at: number }[];
}

export async function sendFriendRequest(uid: string, toUid: string): Promise<{ status: string }> {
  const params = new URLSearchParams({ uid, to_uid: toUid });
  return request('/friend/request', 'POST', params);
}

export async function acceptFriendRequest(uid: string, fromUid: string): Promise<{ status: string }> {
  const params = new URLSearchParams({ uid, from_uid: fromUid });
  return request('/friend/accept', 'POST', params);
}

export async function rejectFriendRequest(uid: string, fromUid: string): Promise<{ status: string }> {
  const params = new URLSearchParams({ uid, from_uid: fromUid });
  return request('/friend/reject', 'POST', params);
}

export async function removeFriend(uid: string, friendUid: string): Promise<{ status: string }> {
  const params = new URLSearchParams({ uid, friend_uid: friendUid });
  return request('/friend/remove', 'POST', params);
}

export async function getFriendList(uid: string): Promise<FriendListResponse> {
  return request(`/friend/list?uid=${encodeURIComponent(uid)}`);
}

// ---- Unread ----

export async function getUnreadCounts(uid: string): Promise<{ uid: string; counts: Record<string, number> }> {
  return request(`/unread?uid=${encodeURIComponent(uid)}`);
}

// ---- Admin ----

function adminParams(uid: string, token: string): URLSearchParams {
  return new URLSearchParams({ uid, token });
}

export async function getAdminStats(uid: string, token: string): Promise<AdminStats> {
  return request(`/admin/stats?${adminParams(uid, token).toString()}`);
}

export async function getAdminUsers(uid: string, token: string, offset = 0, limit = 50): Promise<AdminUsersResponse> {
  const params = adminParams(uid, token);
  params.set('offset', String(offset));
  params.set('limit', String(limit));
  return request(`/admin/users?${params.toString()}`);
}

export async function deleteAdminUser(uid: string, token: string, targetUid: string): Promise<{ status: string }> {
  const params = adminParams(uid, token);
  params.set('target_uid', targetUid);
  return request('/admin/users/delete', 'POST', params);
}

export async function getAdminMessages(uid: string, token: string, before = 0, limit = 50): Promise<AdminBrowseResponse> {
  const params = adminParams(uid, token);
  if (before > 0) params.set('before', String(before));
  params.set('limit', String(limit));
  return request(`/admin/messages?${params.toString()}`);
}

export async function deleteAdminMessage(uid: string, token: string, msgId: string): Promise<{ status: string }> {
  const params = adminParams(uid, token);
  params.set('msg_id', msgId);
  return request('/admin/messages/delete', 'POST', params);
}
