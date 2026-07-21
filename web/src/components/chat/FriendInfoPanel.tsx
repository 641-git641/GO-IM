import { X, MessageSquareDot, Trash2 } from 'lucide-react';

interface FriendInfoPanelProps {
  peerId: string;
  peerName: string;
  open: boolean;
  onClose: () => void;
  onMarkUnread?: () => void;
  onDeleteConversation?: () => void;
}

export default function FriendInfoPanel({
  peerId,
  peerName,
  open,
  onClose,
  onMarkUnread,
  onDeleteConversation,
}: FriendInfoPanelProps) {
  if (!open) return null;

  return (
    <div className="fixed inset-y-0 right-0 z-40 w-80 bg-white dark:bg-gray-900 border-l border-gray-200 dark:border-gray-800 shadow-xl">
      <div className="flex flex-col h-full">
        {/* Header */}
        <div className="h-14 px-4 flex items-center justify-between border-b border-gray-200 dark:border-gray-800 flex-shrink-0">
          <h3 className="text-base font-semibold text-gray-900 dark:text-gray-100">好友设置</h3>
          <button onClick={onClose} className="p-1 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-800">
            <X className="w-5 h-5 text-gray-500" />
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-4 space-y-4">
          {/* Avatar & name */}
          <div className="text-center">
            <div className="w-16 h-16 rounded-full bg-primary-500 flex items-center justify-center text-white text-xl font-bold mx-auto">
              {peerName.slice(0, 2).toUpperCase()}
            </div>
            <h2 className="text-lg font-bold text-gray-900 dark:text-gray-100 mt-2">{peerName}</h2>
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-1 font-mono">{peerId}</p>
          </div>

          {/* Actions */}
          <div className="pt-3 border-t border-gray-200 dark:border-gray-800 space-y-2">
            {onMarkUnread && (
              <button
                onClick={() => { onMarkUnread(); onClose(); }}
                className="w-full flex items-center justify-center gap-2 py-2 rounded-lg text-sm text-blue-600 hover:bg-blue-50 dark:hover:bg-blue-900/20 transition-colors"
              >
                <MessageSquareDot className="w-4 h-4" />
                标为未读
              </button>
            )}
            {onDeleteConversation && (
              <button
                onClick={() => {
                  if (confirm('确定要删除该会话吗？消息记录将被清除。')) {
                    onDeleteConversation();
                    onClose();
                  }
                }}
                className="w-full flex items-center justify-center gap-2 py-2 rounded-lg text-sm text-red-500 hover:bg-red-50 dark:hover:bg-red-900/20 transition-colors"
              >
                <Trash2 className="w-4 h-4" />
                删除会话
              </button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
