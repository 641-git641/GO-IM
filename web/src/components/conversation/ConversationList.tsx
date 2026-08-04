import { useNavigate, useParams } from 'react-router-dom';
import { useChatStore } from '@/stores/chatStore';
import ConversationItem from './ConversationItem';

export default function ConversationList() {
  // 直接订阅 conversations Map,以便变化时组件重新渲染
  const conversationsMap = useChatStore((s) => s.conversations);
  const conversations = Array.from(conversationsMap.values()).sort(
    (a, b) => b.lastTime - a.lastTime,
  );
  const navigate = useNavigate();
  const { peerId } = useParams<{ peerId?: string }>();

  if (conversations.length === 0) {
    return (
      <div className="flex-1 flex items-center justify-center p-4">
        <div className="text-center">
          <p className="text-sm text-gray-400 dark:text-gray-500">暂无对话</p>
          <p className="text-xs text-gray-400 dark:text-gray-500 mt-1">
            在通讯录中选择用户开始聊天
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="flex-1 overflow-y-auto">
      {conversations.map((conv) => (
        <ConversationItem
          key={conv.peerId}
          conversation={conv}
          isActive={peerId === conv.peerId}
          onClick={() => navigate(`/chat/${conv.peerId}`)}
        />
      ))}
    </div>
  );
}
