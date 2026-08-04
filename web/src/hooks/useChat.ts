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

  /** 发送文本消息 */
  const sendText = useCallback(
    (peerId: string, text: string, chatType: ChatTypeValue = ChatType.Single) => {
      if (!text.trim()) return false;

      // 确保会话存在
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

      // 乐观添加(先本地显示)
      addMessage(peerId, msg, uid);

      // 通过 WebSocket 发送
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

  /** 发送文件消息(上传完成后) */
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

  /** 加载历史消息 */
  const loadHistory = useCallback(
    (peerId: string, chatType: ChatTypeValue = ChatType.Single, before?: string) => {
      const conv = useChatStore.getState().conversations.get(peerId);
      if (conv && !conv.hasMore) return;

      // 在请求历史之前保存已有的消息 ID。全局处理器
      // (useWebSocket) 会将收到的历史 Chat 消息以 DESC 顺序
      // (新的在前)加入会话。我们记录哪些消息已存在,以便
      // 提取新消息、按时间正序排列并重建列表。
      const existingIds = new Set((conv?.messages || []).map((m) => m.msgId));

      // 查找最旧消息的时间戳
      let beforeTs = before || String(Date.now());
      if (!before && conv && conv.messages.length > 0) {
        beforeTs = conv.messages[0].timestamp;
      }

      let completeReceived = false;

      const unsubHistory = wsManager.subscribe(Cmd.History, () => {
        completeReceived = true;
        unsubHistory();

        // 在全局处理器添加历史消息后获取会话。
        const updatedConv = useChatStore.getState().conversations.get(peerId);
        if (!updatedConv) return;

        const allMessages = updatedConv.messages;
        // 区分已存在的消息与刚加载的历史消息。
        const oldMessages = allMessages.filter((m) => existingIds.has(m.msgId));
        const newMessages = allMessages.filter((m) => !existingIds.has(m.msgId));

        if (newMessages.length === 0) {
          setNoMoreHistory(peerId);
          return;
        }

        // 历史消息以 DESC 顺序到达(新的在前)。反转成时间正序
        // (旧的在前),以便自上而下正确渲染。
        newMessages.reverse();

        // 重建:历史消息在前(最旧的在顶部),然后是已存在的消息。
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

      // 发送历史请求,若 WS 尚未连接则重试
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
          // WS 尚未连接,300ms 后重试
          if (!completeReceived) {
            setTimeout(sendHistoryRequest, 300);
          }
        }
      };
      sendHistoryRequest();

      // 超时兜底
      setTimeout(() => {
        if (!completeReceived) {
          unsubHistory();
        }
      }, 8000);
    },
    [uid, setNoMoreHistory],
  );

  /** 发送已读回执 */
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

  /** 撤回一条消息 */
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

  /** 将消息转发到另一个会话 */
  const forwardMessage = useCallback(
    (targetPeerId: string, originalMsg: ChatMessage, chatType: ChatTypeValue = ChatType.Single) => {
      upsertConversation(targetPeerId, targetPeerId, chatType);

      const seq = wsManager.nextSeq();
      const now = String(Date.now());

      // 构造带有原消息元数据的转发内容
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

  /** 编辑一条已发送的消息 */
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
