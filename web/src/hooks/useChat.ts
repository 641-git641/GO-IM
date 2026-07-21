import { useCallback } from 'react';
import { wsManager } from '@/lib/ws';
import { useAuthStore } from '@/stores/authStore';
import { useChatStore } from '@/stores/chatStore';
import { Cmd, ChatType, MsgType } from '@/types';
import type { ChatMessage, ChatTypeValue, MsgTypeValue, FileMetadata } from '@/types';
import { isRecalled, isGroupNotification } from '@/lib/utils';
import { HISTORY_LIMIT } from '@/lib/constants';

export function useChat() {
  const { uid } = useAuthStore();
  const { addMessage, prependMessages, setNoMoreHistory, upsertConversation, activePeer } =
    useChatStore();

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

      // Find oldest message timestamp
      let beforeTs = before || String(Date.now());
      if (!before && conv && conv.messages.length > 0) {
        beforeTs = conv.messages[0].timestamp;
      }

      // Set up a one-shot listener for CmdChat and CmdHistory messages
      const messages: ChatMessage[] = [];
      let completeReceived = false;

      const unsubChat = wsManager.subscribe(Cmd.Chat, (msg) => {
        if (msg.to === peerId || msg.from === peerId) {
          // Skip recalled and group notification messages in history
          if (isRecalled(msg.content)) return;
          if (isGroupNotification(msg.content)) return;

          const chatMsg: ChatMessage = {
            ...msg,
            chatType: msg.chatType as ChatTypeValue,
            msgType: msg.msgType as MsgTypeValue,
            status: 'sent',
            recalled: false,
          };
          messages.push(chatMsg);
        }
      });

      const unsubHistory = wsManager.subscribe(Cmd.History, () => {
        completeReceived = true;
        if (messages.length === 0) {
          setNoMoreHistory(peerId);
        } else {
          // DB returns DESC (newest first), but the UI renders top-to-bottom
          // oldest-first. Reverse to restore chronological order.
          messages.reverse();
          prependMessages(peerId, messages);
          if (messages.length < HISTORY_LIMIT) {
            setNoMoreHistory(peerId);
          }
        }
        unsubChat();
        unsubHistory();
      });

      // Send history request
      wsManager.send({
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

      // Timeout fallback
      setTimeout(() => {
        if (!completeReceived) {
          unsubChat();
          unsubHistory();
          if (messages.length > 0) {
            prependMessages(peerId, messages);
          }
        }
      }, 5000);
    },
    [uid, prependMessages, setNoMoreHistory],
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
