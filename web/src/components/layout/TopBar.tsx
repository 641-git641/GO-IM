import { useParams } from 'react-router-dom';
import { Phone, Settings } from 'lucide-react';
import { ChatType } from '@/types';

interface TopBarProps {
  peerName: string;
  chatType?: number;
  isOnline?: boolean;
  onCall?: () => void;
  /** Called when the Settings gear is clicked (group chat → GroupInfoPanel, single chat → FriendInfoPanel) */
  onSettingsClick?: () => void;
}

export default function TopBar({ peerName, chatType = 1, isOnline = false, onCall, onSettingsClick }: TopBarProps) {

  return (
    <div className="h-14 px-4 flex items-center justify-between border-b border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900 flex-shrink-0">
      <div className="flex items-center gap-3 min-w-0">
        <div className="w-9 h-9 rounded-full bg-primary-500 flex items-center justify-center text-white font-semibold text-sm flex-shrink-0">
          {peerName.slice(0, 2).toUpperCase()}
        </div>
        <div className="min-w-0">
          <h2 className="text-sm font-semibold text-gray-900 dark:text-gray-100 truncate">{peerName}</h2>
          <p className="text-xs text-gray-500 dark:text-gray-400">
            {chatType === ChatType.Group
              ? '群聊'
              : isOnline
                ? '在线'
                : '离线'}
          </p>
        </div>
      </div>

      <div className="flex items-center gap-1">
        {chatType === ChatType.Single && (
          <button
            onClick={onCall}
            className="p-2 rounded-lg text-gray-500 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-800 hover:text-primary-600 dark:hover:text-primary-400 transition-colors"
            title="语音通话"
          >
            <Phone className="w-5 h-5" />
          </button>
        )}
        <button
          onClick={onSettingsClick}
          className="p-2 rounded-lg text-gray-500 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-800 hover:text-primary-600 dark:hover:text-primary-400 transition-colors"
          title={chatType === ChatType.Group ? '群组设置' : '好友设置'}
        >
          <Settings className="w-5 h-5" />
        </button>
      </div>
    </div>
  );
}
