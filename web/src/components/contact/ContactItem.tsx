import { getAvatarLetters } from '@/lib/utils';
import { Users } from 'lucide-react';

interface ContactItemProps {
  uid: string;
  name: string;
  isOnline: boolean;
  isGroup?: boolean;
  memberCount?: number;
  onClick: () => void;
}

export default function ContactItem({
  uid,
  name,
  isOnline,
  isGroup = false,
  memberCount,
  onClick,
}: ContactItemProps) {
  const avatar = getAvatarLetters(name);

  return (
    <button
      onClick={onClick}
      className="w-full px-4 py-3 flex items-center gap-3 text-left hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors"
    >
      {/* Avatar */}
      <div className="relative flex-shrink-0">
        <div className="w-10 h-10 rounded-full bg-primary-500 flex items-center justify-center text-white font-semibold text-sm">
          {avatar}
        </div>
        {isOnline && !isGroup && (
          <div className="absolute -bottom-0.5 -right-0.5 w-3.5 h-3.5 bg-green-500 rounded-full border-2 border-white dark:border-gray-900" />
        )}
        {isGroup && (
          <div className="absolute -bottom-0.5 -right-0.5 w-4 h-4 bg-green-500 rounded-full flex items-center justify-center">
            <Users className="w-2.5 h-2.5 text-white" />
          </div>
        )}
      </div>

      {/* Info */}
      <div className="flex-1 min-w-0">
        <h3 className="text-sm font-medium text-gray-900 dark:text-gray-100 truncate">{name}</h3>
        <p className="text-xs text-gray-500 dark:text-gray-400">
          {isGroup ? `${memberCount || '?'} 名成员` : isOnline ? '在线' : '离线'}
        </p>
      </div>

      {isOnline && !isGroup && (
        <span className="w-2 h-2 rounded-full bg-green-500 flex-shrink-0" />
      )}
    </button>
  );
}
