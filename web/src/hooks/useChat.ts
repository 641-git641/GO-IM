import { useCallback } from 'react';
import { wsManager } from '@/lib/ws';
import { useAuthStore } from '@/stores/authStore';
import { useChatStore } from '@/stores/chatStore';
import { Cmd, ChatType, MsgType } from '@/types';
import type { ChatMessage, ChatTypeValue, MsgTypeValue, FileMetadata } from '@/types';
import { HISTORY_LIMIT } from '@/lib/constants';

export function useChat() {
  const { uid } = useAuthStore();
  const { addMessage, setNoMoreHistory, upsertConversation } = useChatStore();

  /** Send a text message */
  const sendText = useCallback(
    (peerId: string, text: string, chatType: ChatTypeValue = ChatType.Single) => {
      if (!text.trim()) return false;

      // Ensure conversation exists
      upsertConversation(peerId, peerId, chatType);

      const seq = wsManager.nextSeq();
      const now = String(Date.now());

      const msg: ChatMessage = {
        seq,
        msgId: '0',
        cmd: Cmd.Chat,
        from: uid,
        to: peerId,
        chatType,
        msgType: MsgType.Text,
        content: text,
        timestamp: now,
        needAck: true,
        status: 'sending',
        recalled: false,
      };

      // Optimistic add
      addMessage(peerId, msg, uid);

      // Send via WebSocket
      wsManager.send({
        seq,
        msgId: '0',
        cmd: Cmd.Chat,
        from: uid,
        to: peerId,
        chatType,
        msgType: MsgType.Text,
        content: text,
        timestamp: now,
        needAck: true,
      });

      return true;
    },
    [uid, addMessage, upsertConversation],
  );

  /** Send a file message (after upload) */
  const sendFile = useCallback(
    (peerId: string, metadata: FileMetadata, chatType: ChatTypeValue = ChatType.Single) => {
      upsertConversation(peerId, peerId, chatType);

      const seq = wsManager.nextSeq();
      const now = String(Date.now());
      const content = JSON.stringify(metadata);

      let msgType: MsgTypeValue = MsgType.File;
      if (metadata.mime?.startsWith('image/')) msgType = MsgType.Image;
      else if (metadata.mime?.startsWith('video/')) msgType = MsgType.Video;
      else if (metadata.mime?.startsWith('audio/')) msgType = MsgType.Voice;

      const msg: ChatMessage = {
        seq,
        msgId: '0',
        cmd: Cmd.File,
        from: uid,
        to: peerId,
        chatType,
        msgType,
        content,
        timestamp: now,
        needAck: true,
        status: 'sending',
        recalled: false,
      };

      addMessage(peerId, msg, uid);

      wsManager.send({
        seq,
        msgId: '0',
        cmd: Cmd.File,
        from: uid,
        to: peerId,
        chatType,
        msgType,
        content,
        timestamp: now,
        needAck: true,
      });

      return true;
    },
    [uid, addMessage, upsertConversation],
  );

  /** Load message history */
  const loadHistory = useCallback(
    (peerId: string, chatType: ChatTypeValue = ChatType.Single, before?: string) => {
      const conv = useChatStore.getState().conversations.get(peerId);
      if (conv && !conv.hasMore) return;

      // Save existing message IDs before we request history. The global handler
      // (useWebSocket) will add incoming history Chat messages to the conversation
      // in DESC order (newest first). We track which messages pre-existed so we can
      // extract the new ones, reverse them to chronological order, and rebuild.
      const existingIds = new Set((conv?.messages || []).map((m) => m.msgId));

      // Find oldest message timestamp
      let beforeTs = before || String(Date.now());
      if (!before && conv && conv.messages.length > 0) {
        beforeTs = conv.messages[0].timestamp;
      }

      let completeReceived = false;

      const unsubHistory = wsManager.subscribe(Cmd.History, () => {
        completeReceived = true;
        unsubHistory();

        // Get the conversation after the global handler has added history messages.
        const updatedConv = useChatStore.getState().conversations.get(peerId);
        if (!updatedConv) return;

        const allMessages = updatedConv.messages;
        // Separate pre-existing messages from newly loaded history messages.
        const oldMessages = allMessages.filter((m) => existingIds.has(m.msgId));
        const newMessages = allMessages.filter((m) => !existingIds.has(m.msgId));

        if (newMessages.length === 0) {
          setNoMoreHistory(peerId);
          return;
        }

        // History arrives in DESC order (newest first). Reverse to chronological
        // (oldest first) so it renders top-to-bottom correctly.
        newMessages.reverse();

        // Rebuild: history messages first (oldest at top), then pre-existing messages.
        const rebuilt = [...newMessages, ...oldMessages];

        useChatStore.setState((s) => {
          const conversations = new Map(s.conversations);
          conversations.set(peerId, { ...updatedConv, messages: rebuilt });
          return { conversations };
        });

        if (newMessages.length < HISTORY_LIMIT) {
          setNoMoreHistory(peerId);
        }
      });

      // Send history request, retrying if WS not yet connected
      const sendHistoryRequest = () => {
        const sent = wsManager.send({
          seq: String(HISTORY_LIMIT),
          msgId: '0',
          cmd: Cmd.History,
          from: uid,
          to: peerId,
          chatType,
          msgType: MsgType.Text,
          content: '',
          timestamp: beforeTs,
          needAck: false,
        });
        if (!sent) {
          // WS not connected yet, retry in 300ms
          if (!completeReceived) {
            setTimeout(sendHistoryRequest, 300);
          }
        }
      };
      sendHistoryRequest();

      // Timeout fallback
      setTimeout(() => {
        if (!completeReceived) {
          unsubHistory();
        }
      }, 8000);
    },
    [uid, setNoMoreHistory],
  );

  /** Send a read receipt */
  const sendReadReceipt = useCallback(
    (peerId: string, lastMsgId: string) => {
      if (!uid || !peerId) return;

      wsManager.send({
        seq: lastMsgId,
        msgId: '0',
        cmd: Cmd.ReadReceipt,
        from: uid,
        to: peerId,
        chatType: ChatType.Single,
        msgType: MsgType.Text,
        content: '',
        timestamp: String(Date.now()),
        needAck: false,
      });
    },
    [uid],
  );

  /** Recall a message */
  const recallMessage = useCallback(
    (peerId: string, msgId: string) => {
      wsManager.send({
        seq: msgId,
        msgId: '0',
        cmd: Cmd.Recall,
        from: uid,
        to: peerId,
        chatType: ChatType.Single,
        msgType: MsgType.Text,
        content: JSON.stringify({ recalled: true, msg_id: msgId }),
        timestamp: String(Date.now()),
        needAck: false,
      });
    },
    [uid],
  );

  /** Forward a message to another conversation */
  const forwardMessage = useCallback(
    (targetPeerId: string, originalMsg: ChatMessage, chatType: ChatTypeValue = ChatType.Single) => {
      upsertConversation(targetPeerId, targetPeerId, chatType);

      const seq = wsManager.nextSeq();
      const now = String(Date.now());

      // Build forward content with metadata about the original message
      const forwardContent = JSON.stringify({
        forwarded: true,
        original_from: originalMsg.from,
        original_content: originalMsg.content,
        text: originalMsg.msgType === MsgType.Text ? originalMsg.content : '[转发的消息]',
      });

      const msg: ChatMessage = {
        seq,
        msgId: '0',
        cmd: Cmd.Forward,
        from: uid,
        to: targetPeerId,
        chatType,
        msgType: originalMsg.msgType,
        content: forwardContent,
        timestamp: now,
        needAck: true,
        status: 'sending',
        recalled: false,
      };

      addMessage(targetPeerId, msg, uid);

      wsManager.send({
        seq,
        msgId: '0',
        cmd: Cmd.Forward,
        from: uid,
        to: targetPeerId,
        chatType,
        msgType: originalMsg.msgType,
        content: forwardContent,
        timestamp: now,
        needAck: true,
      });

      return true;
    },
    [uid, addMessage, upsertConversation],
  );

  /** Edit a previously sent message */
  const editMessage = useCallback(
    (peerId: string, originalMsgId: string, newText: string, chatType: ChatTypeValue = ChatType.Single) => {
      if (!newText.trim()) return false;

      wsManager.send({
        seq: originalMsgId,
        msgId: '0',
        cmd: Cmd.Edit,
        from: uid,
        to: peerId,
        chatType,
        msgType: MsgType.Text,
        content: newText,
        timestamp: String(Date.now()),
        needAck: false,
      });

      return true;
    },
    [uid],
  );

  return { sendText, sendFile, loadHistory, sendReadReceipt, recallMessage, forwardMessage, editMessage };
}
