import { useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useChatStore } from '@/stores/chatStore';
import ConversationList from '@/components/conversation/ConversationList';
import ChatWindow from '@/components/chat/ChatWindow';

export default function ChatPage() {
  const { peerId } = useParams<{ peerId?: string }>();
  const { setActivePeer, activePeer } = useChatStore();

  useEffect(() => {
    if (peerId) {
      setActivePeer(peerId);
    } else {
      setActivePeer(null);
    }
  }, [peerId, setActivePeer]);

  return (
    <div className="flex h-full">
      {/* Conversation sidebar */}
      <div className="w-72 lg:w-80 border-r border-gray-200 dark:border-gray-800 flex-shrink-0 hidden md:flex flex-col">
        <div className="h-14 px-4 flex items-center border-b border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900 flex-shrink-0">
          <h2 className="text-base font-semibold text-gray-900 dark:text-gray-100">消息</h2>
        </div>
        <ConversationList />
      </div>

      {/* Chat window or placeholder */}
      <div className="flex-1 flex flex-col min-w-0">
        {activePeer ? (
          <ChatWindow peerId={activePeer} />
        ) : (
          <div className="flex-1 flex items-center justify-center bg-gray-50 dark:bg-gray-950">
            <div className="text-center space-y-2">
              <div className="w-20 h-20 rounded-full bg-primary-100 dark:bg-primary-900/30 flex items-center justify-center mx-auto">
                <svg className="w-10 h-10 text-primary-400 dark:text-primary-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z" />
                </svg>
              </div>
              <h3 className="text-lg font-medium text-gray-700 dark:text-gray-300">欢迎使用 IM</h3>
              <p className="text-sm text-gray-500 dark:text-gray-400">选择一个对话开始聊天</p>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
