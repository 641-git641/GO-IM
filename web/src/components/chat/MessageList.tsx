import { useRef, useEffect } from 'react';
import MessageBubble from './MessageBubble';
import SystemNotice from './SystemNotice';
import type { ChatMessage } from '@/types';
import { ChatType } from '@/types';
import { isGroupNotification } from '@/lib/utils';

interface MessageListProps {
  messages: ChatMessage[];
  myUid: string;
  onRecall?: (msgId: string) => void;
  onReply?: (message: ChatMessage) => void;
  onForward?: (message: ChatMessage) => void;
  onEdit?: (msgId: string, newText: string) => void;
  onDelete?: (msgId: string) => void;
  /** Search term for in-conversation highlighting */
  highlight?: string;
  /** MsgId of the currently focused search match */
  currentMatchId?: string;
}

export default function MessageList({ messages, myUid, onRecall, onReply, onForward, onEdit, onDelete, highlight, currentMatchId }: MessageListProps) {
  const bottomRef = useRef<HTMLDivElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const shouldAutoScroll = useRef(true);

  // Auto-scroll to bottom on new messages, but only if already near bottom
  useEffect(() => {
    if (shouldAutoScroll.current) {
      bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
    }
  }, [messages.length]);

  const handleScroll = () => {
    const el = containerRef.current;
    if (!el) return;
    // If within 100px of bottom, auto-scroll
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
        // Check if this is a group system notification
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
