import type { Conversation } from '@/types';
import { ChatType } from '@/types';
import { formatDate, getAvatarLetters } from '@/lib/utils';
import { useContactStore } from '@/stores/contactStore';
import { Users } from 'lucide-react';

interface ConversationItemProps {
  conversation: Conversation;
  isActive: boolean;
  onClick: () => void;
}

export default function ConversationItem({ conversation, isActive, onClick }: ConversationItemProps) {
  const { name, peerId, lastMessage, lastTime, unread, chatType } = conversation;
  const contactStore = useContactStore();

  // 解析显示名称:对于群组,优先使用 contactStore 中的名称而非原始 peerId
  const displayName = (() => {
    if (name && name !== peerId) return name;
    if (peerId.startsWith('g_')) {
      const group = contactStore.groups.find(g => g.id === peerId);
      if (group?.name) return group.name;
      const detail = contactStore.getGroupDetail(peerId);
      if (detail?.name) return detail.name;
    }
    return name || peerId;
  })();

  const avatar = getAvatarLetters(displayName);

  return (
    <button
      onClick={onClick}
      className={`w-full px-4 py-3 flex items-center gap-3 text-left transition-colors group relative ${
        isActive ? 'bg-primary-50 dark:bg-primary-900/20' : 'hover:bg-gray-50 dark:hover:bg-gray-800'
      }`}
    >
      {/* 头像 */}
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

      {/* 内容 */}
      <div className="flex-1 min-w-0">
        <div className="flex items-center justify-between">
          <h3 className="text-sm font-semibold text-gray-900 dark:text-gray-100 truncate">{displayName}</h3>
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
    </button>
  );
}
