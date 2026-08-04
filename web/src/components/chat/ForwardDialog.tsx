import { useState } from 'react';
import { X, Send, Users, User } from 'lucide-react';
import { useChatStore } from '@/stores/chatStore';
import type { ChatMessage } from '@/types';

interface ForwardDialogProps {
  message: ChatMessage;
  onForward: (targetPeerId: string) => void;
  onClose: () => void;
}

export default function ForwardDialog({ message, onForward, onClose }: ForwardDialogProps) {
  const conversations = useChatStore((s) => s.getConversationList());
  const [search, setSearch] = useState('');

  // 过滤会话:排除当前会话,按搜索文本过滤
  const filtered = conversations.filter((conv) => {
    if (!search.trim()) return true;
    const q = search.toLowerCase();
    return conv.name.toLowerCase().includes(q) || conv.peerId.toLowerCase().includes(q);
  });

  const previewText =
    message.msgType === 1
      ? message.content.length > 50
        ? message.content.slice(0, 50) + '...'
        : message.content
      : '[非文本消息]';

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white rounded-xl shadow-2xl w-96 max-h-[80vh] flex flex-col">
        {/* 头部 */}
        <div className="h-12 px-4 flex items-center justify-between border-b border-gray-100 flex-shrink-0">
          <h3 className="text-sm font-semibold text-gray-900">转发消息</h3>
          <button onClick={onClose} className="p-1 rounded-lg hover:bg-gray-100">
            <X className="w-4 h-4 text-gray-500" />
          </button>
        </div>

        {/* 预览 */}
        <div className="px-4 py-3 bg-gray-50 border-b border-gray-100 text-sm text-gray-600">
          <span className="text-xs text-gray-400">转发来自 {message.from} 的消息：</span>
          <p className="mt-1 text-gray-700 line-clamp-2">{previewText}</p>
        </div>

        {/* 搜索 */}
        <div className="px-4 py-2 border-b border-gray-100">
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="搜索会话..."
            className="w-full px-3 py-1.5 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-primary-400"
          />
        </div>

        {/* 会话列表 */}
        <div className="flex-1 overflow-y-auto p-2">
          {filtered.length === 0 ? (
            <p className="text-center text-sm text-gray-400 py-8">暂无会话</p>
          ) : (
            filtered.map((conv) => (
              <button
                key={conv.peerId}
                onClick={() => onForward(conv.peerId)}
                className="w-full flex items-center gap-3 px-3 py-2.5 rounded-lg hover:bg-gray-50 transition-colors text-left"
              >
                <div className="w-9 h-9 rounded-full bg-primary-500 flex items-center justify-center text-white text-xs font-semibold flex-shrink-0">
                  {conv.name.slice(0, 2).toUpperCase()}
                </div>
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-medium text-gray-900 truncate">{conv.name}</p>
                  <p className="text-xs text-gray-400 truncate">{conv.lastMessage}</p>
                </div>
                <div className="flex items-center gap-1 text-primary-500">
                  <Send className="w-4 h-4" />
                </div>
              </button>
            ))
          )}
        </div>
      </div>
    </div>
  );
}
