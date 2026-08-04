import { useState, type FormEvent } from 'react';
import { useNavigate } from 'react-router-dom';
import { useSearch } from '@/hooks/useSearch';
import { useChatStore } from '@/stores/chatStore';
import { formatTime } from '@/lib/utils';
import { Search, ArrowUpRight } from 'lucide-react';

export default function SearchPage() {
  const [query, setQuery] = useState('');
  const { results, searching, error, search, loadMore } = useSearch();
  const navigate = useNavigate();
  const setActivePeer = useChatStore((s) => s.setActivePeer);

  const handleSubmit = (e: FormEvent) => {
    e.preventDefault();
    if (!query.trim()) return;
    search({ q: query.trim(), limit: 20 });
  };

  /** 跳转到包含该消息的会话 */
  const handleResultClick = (peerId: string) => {
    setActivePeer(peerId);
    navigate(`/chat/${peerId}`);
  };

  return (
    <div className="flex flex-col h-full">
      <div className="h-14 px-4 flex items-center border-b border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900 flex-shrink-0">
        <h2 className="text-base font-semibold text-gray-900 dark:text-gray-100">搜索消息</h2>
      </div>

      <div className="flex-1 flex flex-col overflow-hidden">
        {/* 搜索栏 */}
        <form onSubmit={handleSubmit} className="p-4 border-b border-gray-100 dark:border-gray-800">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
            <input
              type="text"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="搜索消息内容..."
              className="w-full pl-10 pr-4 py-2.5 rounded-lg border border-gray-300 dark:border-gray-700 text-sm focus:ring-2 focus:ring-primary-500 focus:border-primary-500 outline-none bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100"
            />
          </div>
        </form>

        {/* 结果 */}
        <div className="flex-1 overflow-y-auto">
          {searching && (
            <div className="p-4 text-center text-sm text-gray-400">搜索中...</div>
          )}

          {error && (
            <div className="p-4 text-center text-sm text-red-500">{error}</div>
          )}

          {results && results.messages.length === 0 && (
            <div className="p-4 text-center text-sm text-gray-400">
              未找到 "{results.query}" 的消息
            </div>
          )}

          {results && results.messages.length > 0 && (
            <div>
              <div className="px-4 py-2 text-xs text-gray-500 dark:text-gray-400 bg-gray-50 dark:bg-gray-800">
                找到 {results.total} 条结果
              </div>
              {results.messages.map((msg) => {
                // 确定会话对象:单聊使用对方,群聊使用群组 ID
                const peerId = msg.chat_type === 2 ? msg.to : (msg.from || msg.to);
                return (
                  <div
                    key={msg.msg_id}
                    onClick={() => handleResultClick(peerId)}
                    className="px-4 py-3 border-b border-gray-50 dark:border-gray-800 hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors cursor-pointer group"
                  >
                    <div className="flex items-center justify-between mb-1">
                      <span className="text-sm font-medium text-gray-900 dark:text-gray-100">{msg.from}</span>
                      <div className="flex items-center gap-2">
                        <span className="text-xs text-gray-400">{formatTime(msg.timestamp)}</span>
                        <ArrowUpRight className="w-3.5 h-3.5 text-gray-300 opacity-0 group-hover:opacity-100 transition-opacity" />
                      </div>
                    </div>
                    <p className="text-sm text-gray-600 dark:text-gray-400 line-clamp-2">{msg.content}</p>
                    <p className="text-xs text-gray-400 mt-0.5">
                      {msg.chat_type === 2 ? '群聊' : `与 ${msg.to} 的私聊`} — 点击跳转到对话
                    </p>
                  </div>
                );
              })}
              {results.next_cursor > 0 && (
                <button
                  onClick={() => loadMore({ q: query.trim(), limit: 20 })}
                  className="w-full py-3 text-sm text-primary-600 dark:text-primary-400 hover:bg-primary-50 dark:hover:bg-primary-900/20 transition-colors"
                >
                  加载更多
                </button>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
