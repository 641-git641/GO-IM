import { useState } from 'react';
import type { Conversation } from '@/types';
import { ChatType, MsgType } from '@/types';
import { formatDate, getAvatarLetters } from '@/lib/utils';
import { Users, X, MessageSquareDot } from 'lucide-react';

interface ConversationItemProps {
  conversation: Conversation;
  isActive: boolean;
  onClick: () => void;
  onDelete?: (peerId: string) => void;
  onMarkUnread?: (peerId: string) => void;
}

export default function ConversationItem({ conversation, isActive, onClick, onDelete, onMarkUnread }: ConversationItemProps) {
  const { name, peerId, lastMessage, lastTime, unread, chatType } = conversation;
  const avatar = getAvatarLetters(name);
  const [showDelete, setShowDelete] = useState(false);

  const handleDelete = (e: React.MouseEvent) => {
    e.stopPropagation();
    onDelete?.(peerId);
  };

  const handleMarkUnread = (e: React.MouseEvent) => {
    e.stopPropagation();
    onMarkUnread?.(peerId);
  };

  return (
    <button
      onClick={onClick}
      onMouseEnter={() => setShowDelete(true)}
      onMouseLeave={() => setShowDelete(false)}
      className={`w-full px-4 py-3 flex items-center gap-3 text-left transition-colors group relative ${
        isActive ? 'bg-primary-50 dark:bg-primary-900/20' : 'hover:bg-gray-50 dark:hover:bg-gray-800'
      }`}
    >
      {/* Avatar */}
      <div className="relative flex-shrink-0">
        <div className="w-11 h-11 rounded-full bg-primary-500 flex items-center justify-center text-white font-semibold text-sm">
          {avatar}
        </div>
        {chatType === ChatType.Group && (
          <div className="absolute -bottom-0.5 -right-0.5 w-4 h-4 bg-green-500 rounded-full flex items-center justify-center">
            <Users className="w-2.5 h-2.5 text-white" />
          </div>
        )}
      </div>

      {/* Content */}
      <div className="flex-1 min-w-0">
        <div className="flex items-center justify-between">
          <h3 className="text-sm font-semibold text-gray-900 dark:text-gray-100 truncate">{name}</h3>
          <span className="text-[10px] text-gray-400 dark:text-gray-500 flex-shrink-0 ml-2">
            {formatDate(lastTime)}
          </span>
        </div>
        <div className="flex items-center justify-between mt-0.5">
          <p className="text-xs text-gray-500 dark:text-gray-400 truncate">{lastMessage || '暂无消息'}</p>
          {unread > 0 && (
            <span className="ml-2 px-1.5 py-0.5 bg-red-500 text-white text-[10px] font-semibold rounded-full flex-shrink-0 min-w-[18px] text-center">
              {unread > 99 ? '99+' : unread}
            </span>
          )}
        </div>
      </div>

      {/* Delete button (appears on hover) */}
      {showDelete && onDelete && (
        <div
          onClick={handleDelete}
          className="absolute right-2 top-1/2 -translate-y-1/2 p-1 rounded-full hover:bg-red-100 dark:hover:bg-red-900/30 text-gray-400 dark:text-gray-500 hover:text-red-500 dark:hover:text-red-400 transition-colors cursor-pointer"
          title="删除会话"
        >
          <X className="w-4 h-4" />
        </div>
      )}

      {/* Mark unread button (appears on hover, to the left of delete) */}
      {showDelete && onMarkUnread && unread === 0 && (
        <div
          onClick={handleMarkUnread}
          className="absolute right-9 top-1/2 -translate-y-1/2 p-1 rounded-full hover:bg-blue-100 dark:hover:bg-blue-900/30 text-gray-400 dark:text-gray-500 hover:text-blue-500 dark:hover:text-blue-400 transition-colors cursor-pointer"
          title="标为未读"
        >
          <MessageSquareDot className="w-4 h-4" />
        </div>
      )}
    </button>
  );
}
