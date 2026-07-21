import type { GroupNotification } from '@/types';

interface SystemNoticeProps {
  notification: GroupNotification;
}

export default function SystemNotice({ notification }: SystemNoticeProps) {
  const { type, uid } = notification;

  const text =
    type === 'member_joined'
      ? `${uid} 加入了群聊`
      : type === 'member_left'
        ? `${uid} 退出了群聊`
        : '';

  return (
    <div className="flex justify-center py-1">
      <span className="px-3 py-1 text-xs text-gray-500 dark:text-gray-400 bg-gray-100 dark:bg-gray-800 rounded-full">
        {text}
      </span>
    </div>
  );
}
