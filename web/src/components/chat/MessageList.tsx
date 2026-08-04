import { useRef, useEffect } from 'react';
import MessageBubble from './MessageBubble';
import SystemNotice, { renderSystemNotice } from './SystemNotice';
import type { ChatMessage } from '@/types';
import { ChatType } from '@/types';
import { isGroupNotification, isSystemMessage } from '@/lib/utils';

interface MessageListProps {
  messages: ChatMessage[];
  myUid: string;
  onRecall?: (msgId: string) => void;
  onReply?: (message: ChatMessage) => void;
  onForward?: (message: ChatMessage) => void;
  onEdit?: (msgId: string, newText: string) => void;
  onDelete?: (msgId: string) => void;
  /** 会话内高亮搜索词 */
  highlight?: string;
  /** 当前聚焦搜索匹配项的 MsgId */
  currentMatchId?: string;
}

export default function MessageList({ messages, myUid, onRecall, onReply, onForward, onEdit, onDelete, highlight, currentMatchId }: MessageListProps) {
  const bottomRef = useRef<HTMLDivElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const shouldAutoScroll = useRef(true);

  // 新消息时自动滚动到底部,但仅当已接近底部时
  useEffect(() => {
    if (shouldAutoScroll.current) {
      bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
    }
  }, [messages.length]);

  const handleScroll = () => {
    const el = containerRef.current;
    if (!el) return;
    // 若距底部 100px 以内,自动滚动
    const { scrollTop, scrollHeight, clientHeight } = el;
    shouldAutoScroll.current = scrollHeight - scrollTop - clientHeight < 100;
  };

  return (
    <div
      ref={containerRef}
      onScroll={handleScroll}
      className="flex-1 overflow-y-auto px-4 py-3 space-y-2 bg-gray-50 dark:bg-gray-950"
    >
      {messages.length === 0 && (
        <div className="flex items-center justify-center h-full">
          <p className="text-sm text-gray-400">暂无消息，发送第一条消息吧</p>
        </div>
      )}

      {messages.map((msg, i) => {
        // 检查是否为系统消息(群组通知、好友请求等)
        const sysType = isSystemMessage(msg.content);
        if (sysType) {
          const noticeText = renderSystemNotice(msg.content);
          if (noticeText) {
            return (
              <div key={msg.msgId || msg.seq || i} className="flex justify-center py-1">
                <span className="px-3 py-1 text-xs text-gray-500 dark:text-gray-400 bg-gray-100 dark:bg-gray-800 rounded-full">
                  {noticeText}
                </span>
              </div>
            );
          }
        }

        // 向后兼容:同时检查旧的通知格式
        if (msg.chatType === ChatType.Group) {
          const notification = isGroupNotification(msg.content);
          if (notification) {
            return <SystemNotice key={msg.msgId || msg.seq || i} notification={notification} />;
          }
        }

        const showAvatar =
          i === 0 || messages[i - 1].from !== msg.from;

        return (
          <MessageBubble
            key={msg.msgId || msg.seq || i}
            message={msg}
            isMine={msg.from === myUid}
            showAvatar={showAvatar}
            onRecall={onRecall}
            onReply={onReply}
            onForward={onForward}
            onEdit={onEdit}
            onDelete={onDelete}
            highlight={highlight}
            isCurrentMatch={highlight && currentMatchId ? (msg.msgId || msg.seq) === currentMatchId : false}
          />
        );
      })}

      <div ref={bottomRef} />
    </div>
  );
}
