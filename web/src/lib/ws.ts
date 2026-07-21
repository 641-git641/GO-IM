/**
 * WebSocket manager with protobuf binary encoding.
 *
 * Since @bufbuild/protobuf requires a build step (buf generate), we define
 * a lightweight manual encode/decode for the 10-field Message proto schema.
 * This avoids the codegen dependency and lets us iterate faster.
 *
 * Wire format (proto3 binary):
 *   Each field: (field_number << 3) | wire_type, followed by value
 *   wire_type 0 = varint (int32/int64/bool)
 *   wire_type 2 = length-delimited (string)
 *
 * Message fields:
 *   1: seq (int64, wire_type 0)
 *   2: msg_id (int64, wire_type 0)
 *   3: cmd (int32, wire_type 0)
 *   4: from (string, wire_type 2)
 *   5: to (string, wire_type 2)
 *   6: chat_type (int32, wire_type 0)
 *   7: msg_type (int32, wire_type 0)
 *   8: content (string, wire_type 2)
 *   9: timestamp (int64, wire_type 0)
 *   10: need_ack (bool, wire_type 0)
 */

import { Cmd, ChatType, MsgType } from '@/types';
import type { CmdType, IMMessage, ChatTypeValue, MsgTypeValue } from '@/types';
import { getWsURL } from './constants';
import { getToken, clearAuth, saveAuth, isTokenExpired } from './auth';
import { RECONNECT_DELAYS, MAX_RECONNECT_DELAY } from './constants';
import { useAuthStore } from '@/stores/authStore';

// ---- Protobuf encode/decode (minimal, dependency-free) ----

const WireType = {
  Varint: 0,
  Len: 2,
} as const;
type WireTypeValue = (typeof WireType)[keyof typeof WireType];

function encodeVarint(value: bigint | number): number[] {
  const bytes: number[] = [];
  let v = typeof value === 'bigint' ? value : BigInt(value);
  while (v > 127n) {
    bytes.push(Number((v & 0x7fn) | 0x80n));
    v >>= 7n;
  }
  bytes.push(Number(v));
  return bytes;
}

function decodeVarint(data: Uint8Array, offset: number): [bigint, number] {
  let result = 0n;
  let shift = 0n;
  let pos = offset;
  let byte: number;
  do {
    byte = data[pos++];
    result |= BigInt(byte & 0x7f) << shift;
    shift += 7n;
  } while (byte & 0x80);
  return [result, pos];
}

function encodeTag(fieldNum: number, wireType: WireTypeValue): number {
  return (fieldNum << 3) | wireType;
}

function encodeBytes(value: string): Uint8Array {
  const encoder = new TextEncoder();
  return encoder.encode(value);
}

function decodeBytes(data: Uint8Array, offset: number, length: number): string {
  const decoder = new TextDecoder();
  return decoder.decode(data.slice(offset, offset + length));
}

/** Encode an IMMessage to protobuf binary */
export function encodeMessage(msg: IMMessage): Uint8Array {
  const parts: number[][] = [];

  // Field 1: seq (int64)
  if (msg.seq && msg.seq !== '0') {
    parts.push([encodeTag(1, WireType.Varint), ...encodeVarint(BigInt(msg.seq))]);
  }
  // Field 2: msg_id (int64)
  if (msg.msgId && msg.msgId !== '0') {
    parts.push([encodeTag(2, WireType.Varint), ...encodeVarint(BigInt(msg.msgId))]);
  }
  // Field 3: cmd (int32)
  if (msg.cmd) {
    parts.push([encodeTag(3, WireType.Varint), ...encodeVarint(BigInt(msg.cmd))]);
  }
  // Field 4: from
  if (msg.from) {
    const b = encodeBytes(msg.from);
    parts.push(
      [encodeTag(4, WireType.Len), ...encodeVarint(BigInt(b.length))],
      Array.from(b),
    );
  }
  // Field 5: to
  if (msg.to) {
    const b = encodeBytes(msg.to);
    parts.push(
      [encodeTag(5, WireType.Len), ...encodeVarint(BigInt(b.length))],
      Array.from(b),
    );
  }
  // Field 6: chat_type
  if (msg.chatType) {
    parts.push([encodeTag(6, WireType.Varint), ...encodeVarint(BigInt(msg.chatType))]);
  }
  // Field 7: msg_type
  if (msg.msgType) {
    parts.push([encodeTag(7, WireType.Varint), ...encodeVarint(BigInt(msg.msgType))]);
  }
  // Field 8: content
  if (msg.content) {
    const b = encodeBytes(msg.content);
    parts.push(
      [encodeTag(8, WireType.Len), ...encodeVarint(BigInt(b.length))],
      Array.from(b),
    );
  }
  // Field 9: timestamp
  if (msg.timestamp && msg.timestamp !== '0') {
    parts.push([encodeTag(9, WireType.Varint), ...encodeVarint(BigInt(msg.timestamp))]);
  }
  // Field 10: need_ack
  if (msg.needAck) {
    parts.push([encodeTag(10, WireType.Varint), 1]);
  }

  const totalLen = parts.reduce((sum, p) => sum + p.length, 0);
  const result = new Uint8Array(totalLen);
  let offset = 0;
  for (const p of parts) {
    result.set(p, offset);
    offset += p.length;
  }
  return result;
}

/** Decode protobuf binary to IMMessage */
export function decodeMessage(data: Uint8Array): IMMessage | null {
  try {
    const msg: Partial<IMMessage> = {
      seq: '0',
      msgId: '0',
      cmd: 0 as CmdType,
      from: '',
      to: '',
      chatType: 1 as ChatTypeValue,
      msgType: 1 as MsgTypeValue,
      content: '',
      timestamp: '0',
      needAck: false,
    };

    let pos = 0;
    while (pos < data.length) {
      const [tag, newPos] = decodeVarint(data, pos);
      pos = newPos;
      const fieldNum = Number(tag >> 3n);
      const wireType = Number(tag & 0x7n) as WireTypeValue;

      if (wireType === WireType.Varint) {
        const [value, nextPos] = decodeVarint(data, pos);
        pos = nextPos;
        switch (fieldNum) {
          case 1: msg.seq = String(value); break;
          case 2: msg.msgId = String(value); break;
          case 3: msg.cmd = Number(value) as CmdType; break;
          case 6: msg.chatType = Number(value) as ChatTypeValue; break;
          case 7: msg.msgType = Number(value) as MsgTypeValue; break;
          case 9: msg.timestamp = String(value); break;
          case 10: msg.needAck = value === 1n; break;
        }
      } else if (wireType === WireType.Len) {
        const [length, afterLen] = decodeVarint(data, pos);
        pos = afterLen;
        const len = Number(length);
        const value = decodeBytes(data, pos, len);
        pos += len;
        switch (fieldNum) {
          case 4: msg.from = value; break;
          case 5: msg.to = value; break;
          case 8: msg.content = value; break;
        }
      }
    }

    return msg as IMMessage;
  } catch {
    return null;
  }
}

// ---- Subscription types ----

export type MessageHandler = (msg: IMMessage) => void;
type SubscriberMap = Map<CmdType, Set<MessageHandler>>;

// ---- WSManager singleton ----

class WSManager {
  private ws: WebSocket | null = null;
  private subscribers: SubscriberMap = new Map();
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private reconnectAttempt = 0;
  private intentionalClose = false;
  private kicked = false;
  private seqCounter = 0;
  private loginTimeout: ReturnType<typeof setTimeout> | null = null;

  // Callbacks for store updates
  private onStatusChange?: (status: 'connecting' | 'connected' | 'disconnected') => void;

  setStatusCallback(cb: (status: 'connecting' | 'connected' | 'disconnected') => void) {
    this.onStatusChange = cb;
  }

  /** Get next client sequence number */
  nextSeq(): string {
    return String(++this.seqCounter);
  }

  /** Connect to the WebSocket server */
  connect(token?: string): void {
    const t = token || getToken();
    if (!t) {
      console.error('[WS] No token, cannot connect');
      return;
    }

    this.intentionalClose = false;
    this.onStatusChange?.('connecting');

    const url = `${getWsURL()}?token=${encodeURIComponent(t)}`;
    console.log('[WS] Connecting to', url);

    try {
      this.ws = new WebSocket(url);
      this.ws.binaryType = 'arraybuffer';

      this.ws.onopen = () => {
        console.log('[WS] Connected');
        this.reconnectAttempt = 0;
        // Set a timeout: if server doesn't send LoginResp within 10s, reconnect
        this.loginTimeout = setTimeout(() => {
          if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            console.log('[WS] LoginResp timeout — no server response');
            this.ws.close(4000, 'Login timeout');
          }
        }, 10_000);
      };

      this.ws.onmessage = (event: MessageEvent) => {
        if (event.data instanceof ArrayBuffer) {
          const msg = decodeMessage(new Uint8Array(event.data));
          if (msg) {
            this.dispatch(msg);
          }
        }
      };

      this.ws.onclose = (event: CloseEvent) => {
        console.log('[WS] Disconnected:', event.code, event.reason);
        this.ws = null;
        this.onStatusChange?.('disconnected');

        if (this.intentionalClose) return;
        if (this.kicked) {
          console.log('[WS] Kicked, not reconnecting');
          clearAuth();
          return;
        }

        this.scheduleReconnect();
      };

      this.ws.onerror = (event: Event) => {
        console.error('[WS] Error:', event);
      };
    } catch (err) {
      console.error('[WS] Connection failed:', err);
      this.scheduleReconnect();
    }
  }

  /** Disconnect gracefully */
  disconnect(): void {
    this.intentionalClose = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.ws) {
      this.ws.close(1000, 'User disconnected');
      this.ws = null;
    }
    this.onStatusChange?.('disconnected');
  }

  /** Send a message */
  send(msg: IMMessage): boolean {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      console.warn('[WS] Not connected, cannot send');
      return false;
    }
    try {
      const data = encodeMessage(msg);
      this.ws.send(data.buffer as ArrayBuffer);
      return true;
    } catch (err) {
      console.error('[WS] Send error:', err);
      return false;
    }
  }

  /** Subscribe to messages of a specific Cmd */
  subscribe(cmd: CmdType, handler: MessageHandler): () => void {
    let handlers = this.subscribers.get(cmd);
    if (!handlers) {
      handlers = new Set();
      this.subscribers.set(cmd, handlers);
    }
    handlers.add(handler);
    return () => {
      handlers?.delete(handler);
    };
  }

  /** Subscribe to ALL messages (wildcard) */
  subscribeAll(handler: MessageHandler): () => void {
    // Using a special key: 0 (CmdNone) as wildcard
    return this.subscribe(0 as CmdType, handler);
  }

  /** Check connection state */
  isConnected(): boolean {
    return this.ws?.readyState === WebSocket.OPEN;
  }

  /** Clear the LoginResp timeout (called when server confirms connection) */
  clearLoginTimeout(): void {
    if (this.loginTimeout) {
      clearTimeout(this.loginTimeout);
      this.loginTimeout = null;
    }
  }

  /** Mark as kicked (stop reconnect) */
  markKicked(): void {
    this.kicked = true;
    this.disconnect();
  }

  // ---- Private ----

  private dispatch(msg: IMMessage): void {
    // Dispatch to specific cmd subscribers
    const handlers = this.subscribers.get(msg.cmd);
    if (handlers) {
      for (const h of handlers) h(msg);
    }
    // Dispatch to wildcard subscribers
    const allHandlers = this.subscribers.get(0 as CmdType);
    if (allHandlers) {
      for (const h of allHandlers) h(msg);
    }
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer) return;

    const delay = Math.min(
      RECONNECT_DELAYS[Math.min(this.reconnectAttempt, RECONNECT_DELAYS.length - 1)],
      MAX_RECONNECT_DELAY,
    );
    this.reconnectAttempt++;

    console.log(`[WS] Reconnecting in ${delay}ms (attempt ${this.reconnectAttempt})`);

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;

      // Check token expiry before reconnecting
      if (isTokenExpired()) {
        console.log('[WS] Token expired, cannot reconnect');
        this.onStatusChange?.('disconnected');
        clearAuth();
        useAuthStore.getState().logout();
        return;
      }

      this.connect();
    }, delay);
  }
}

// Export singleton
export const wsManager = new WSManager();
