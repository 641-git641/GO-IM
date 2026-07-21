import { useEffect, useState, useCallback, useMemo, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { useChatStore } from '@/stores/chatStore';
import { useAuthStore } from '@/stores/authStore';
import { useContactStore } from '@/stores/contactStore';
import { useChat } from '@/hooks/useChat';
import { useFileUpload } from '@/hooks/useFileUpload';
import { ChatType, Cmd, MsgType } from '@/types';
import type { FileMetadata, GroupInfo, ChatMessage } from '@/types';
import { wsManager } from '@/lib/ws';
import TopBar from '@/components/layout/TopBar';
import GroupInfoPanel from '@/components/group/GroupInfoPanel';
import FriendInfoPanel from './FriendInfoPanel';
import MessageList from './MessageList';
import ChatInput from './ChatInput';
import ForwardDialog from './ForwardDialog';
import ChatSearch from './ChatSearch';

interface ChatWindowProps {
  peerId: string;
}

export default function ChatWindow({ peerId }: ChatWindowProps) {
  const { uid } = useAuthStore();
  const navigate = useNavigate();
  const conversations = useChatStore((s) => s.conversations);
  const typingUsers = useChatStore((s) => s.typingUsers);
  const { onlineUsers, groupDetails, getGroupDetail } = useContactStore();
  const { sendText, sendFile, loadHistory, sendReadReceipt, recallMessage, forwardMessage, editMessage } = useChat();
  const { upload, uploading, progress, reset: resetUpload } = useFileUpload();
  const { resetUnread, deleteMessage, markUnread, deleteConversation } = useChatStore();

  const [groupPanelOpen, setGroupPanelOpen] = useState(false);
  const [friendPanelOpen, setFriendPanelOpen] = useState(false);
  const [replyTo, setReplyTo] = useState<ChatMessage | null>(null);
  const [typingUid, setTypingUid] = useState<string | null>(null);
  const [forwardMsg, setForwardMsg] = useState<ChatMessage | null>(null);
  const typingTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // In-conversation search state
  const [searchQuery, setSearchQuery] = useState('');
  const [currentMatch, setCurrentMatch] = useState(0);
  const matchIdsRef = useRef<string[]>([]);

  // Watch typing state for current peer
  useEffect(() => {
    const info = typingUsers.get(peerId);
    if (info && info.until > Date.now() && info.uid !== uid) {
      setTypingUid(info.uid);
      // Clear typing after timeout
      if (typingTimerRef.current) clearTimeout(typingTimerRef.current);
      typingTimerRef.current = setTimeout(() => {
        setTypingUid(null);
      }, 5000);
    } else if (!info || info.until <= Date.now()) {
      setTypingUid(null);
    }
    return () => {
      if (typingTimerRef.current) clearTimeout(typingTimerRef.current);
    };
  }, [peerId, typingUsers, uid]);

  const conv = conversations.get(peerId);
  const isGroup = peerId.startsWith('g_');
  const chatType = isGroup ? ChatType.Group : (conv?.chatType || ChatType.Single);

  // Reset unread when viewing conversation
  useEffect(() => {
    resetUnread(peerId);
  }, [peerId, resetUnread]);

  // Load history on mount
  useEffect(() => {
    loadHistory(peerId, chatType);
  }, [peerId, chatType, loadHistory]);

  // Request group info on mount for groups (so name resolves after refresh)
  useEffect(() => {
    if (isGroup) {
      // Request GroupInfo for this group
      wsManager.send({
        seq: '0', msgId: '0', cmd: Cmd.GroupInfo, from: uid, to: peerId,
        chatType: ChatType.Group, msgType: MsgType.Text, content: '', timestamp: '0', needAck: false,
      });
      // Also request GroupList to populate all groups in contact store
      wsManager.send({
        seq: '0', msgId: '0', cmd: Cmd.GroupList, from: uid, to: '',
        chatType: ChatType.Group, msgType: MsgType.Text, content: '', timestamp: '0', needAck: false,
      });
    }
  }, [isGroup, peerId, uid]);

  // Get peer name: check conversation name, contactStore groups, then fallback to peerId
  const peerName = (() => {
    if (peerId.startsWith('g_')) {
      // Try conversation name first (non-raw-ID names)
      if (conv?.name && conv.name !== peerId) return conv.name;
      // Try contact store groups
      const contactGroups = useContactStore.getState().groups;
      const group = contactGroups.find(g => g.id === peerId);
      if (group?.name) return group.name;
      // Try group details
      const detail = getGroupDetail(peerId);
      if (detail?.name) return detail.name;
      return peerId;
    }
    return peerId;
  })();

  const isPeerOnline = !peerId.startsWith('g_') && onlineUsers.includes(peerId);

  // Group members for @mention support
  const groupMembers = useMemo(() => {
    if (!isGroup) return undefined;
    const detail = getGroupDetail(peerId);
    return detail?.members || [];
  }, [isGroup, peerId, groupDetails, getGroupDetail]);

  // Open settings panel (group or friend) when gear icon is clicked
  const handleSettingsClick = useCallback(() => {
    if (isGroup) {
      setGroupPanelOpen(true);
      // Request group info via WebSocket
      wsManager.send({
        seq: '0',
        msgId: '0',
        cmd: Cmd.GroupInfo,
        from: uid,
        to: peerId,
        chatType: ChatType.Group,
        msgType: MsgType.Text,
        content: '',
        timestamp: '0',
        needAck: false,
      });
    } else {
      setFriendPanelOpen(true);
    }
  }, [isGroup, uid, peerId]);

  // Build group info for the panel
  const groupDetail = getGroupDetail(peerId);
  const groupInfo: GroupInfo | null = groupDetail
    ? groupDetail
    : conv && isGroup
      ? {
          id: peerId,
          name: conv.name,
          owner_uid: '',
          members: [],
          created_at: 0,
        }
      : null;

  const handleSendText = useCallback(
    (text: string) => {
      if (replyTo) {
        // Embed reply metadata in content JSON
        const replyContent = replyTo.content.length > 200
          ? replyTo.content.slice(0, 200) + '...'
          : replyTo.content;
        const content = JSON.stringify({
          text,
          reply_to: {
            msg_id: replyTo.msgId,
            from: replyTo.from,
            content: replyContent,
          },
        });
        sendText(peerId, content, chatType);
        setReplyTo(null);
      } else {
        sendText(peerId, text, chatType);
      }
    },
    [peerId, chatType, sendText, replyTo],
  );

  const handleSendFile = useCallback(
    async (file: File) => {
      const result = await upload(file);
      if (result) {
        const metadata: FileMetadata = {
          file_id: result.file_id,
          name: result.name,
          size: result.size,
          mime: result.mime,
          width: result.width,
          height: result.height,
        };
        sendFile(peerId, metadata, chatType);
        resetUpload();
      }
    },
    [peerId, chatType, upload, sendFile, resetUpload],
  );

  const handleRecall = useCallback(
    (msgId: string) => {
      recallMessage(peerId, msgId);
    },
    [peerId, recallMessage],
  );

  const handleReply = useCallback((message: ChatMessage) => {
    setReplyTo(message);
  }, []);

  const handleCancelReply = useCallback(() => {
    setReplyTo(null);
  }, []);

  const handleForward = useCallback((message: ChatMessage) => {
    setForwardMsg(message);
  }, []);

  const handleForwardSend = useCallback(
    (targetPeerId: string) => {
      if (!forwardMsg) return;
      const targetConv = conversations.get(targetPeerId);
      const targetChatType = targetConv?.chatType || ChatType.Single;
      forwardMessage(targetPeerId, forwardMsg, targetChatType);
      setForwardMsg(null);
    },
    [forwardMsg, conversations, forwardMessage],
  );

  const handleEdit = useCallback(
    (msgId: string, newText: string) => {
      editMessage(peerId, msgId, newText, chatType);
    },
    [peerId, chatType, editMessage],
  );

  const handleDelete = useCallback(
    (msgId: string) => {
      if (confirm('确定要删除这条消息吗？（仅从您的视图中移除）')) {
        deleteMessage(peerId, msgId);
      }
    },
    [peerId, deleteMessage],
  );

  const handleMarkUnread = useCallback(() => {
    markUnread(peerId);
  }, [peerId, markUnread]);

  const handleDeleteConversation = useCallback(() => {
    deleteConversation(peerId);
    navigate('/chat', { replace: true });
  }, [peerId, deleteConversation, navigate]);

  // ---- In-conversation search ----

  const messages = conv?.messages || [];

  // Compute matching message IDs
  const matchIds = useMemo(() => {
    if (!searchQuery.trim()) return [];
    const q = searchQuery.toLowerCase();
    return messages
      .filter((m) => !m.recalled && m.msgType === MsgType.Text && m.content.toLowerCase().includes(q))
      .map((m) => m.msgId || m.seq);
  }, [messages, searchQuery]);

  // Keep matchIdsRef in sync for callbacks
  matchIdsRef.current = matchIds;

  // Scroll to the current match
  useEffect(() => {
    if (matchIds.length > 0 && currentMatch < matchIds.length) {
      const el = document.querySelector(`[data-msg-id="${matchIds[currentMatch]}"]`);
      if (el) {
        el.scrollIntoView({ behavior: 'smooth', block: 'center' });
      }
    }
  }, [currentMatch, matchIds]);

  const handleSearchNext = useCallback(() => {
    if (matchIdsRef.current.length === 0) return;
    setCurrentMatch((prev) => (prev + 1) % matchIdsRef.current.length);
  }, []);

  const handleSearchPrev = useCallback(() => {
    if (matchIdsRef.current.length === 0) return;
    setCurrentMatch((prev) => (prev - 1 + matchIdsRef.current.length) % matchIdsRef.current.length);
  }, []);

  const handleSearchClose = useCallback(() => {
    setSearchQuery('');
    setCurrentMatch(0);
  }, []);

  // Ctrl+F shortcut
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key === 'f') {
        e.preventDefault();
        setSearchQuery('');
        setCurrentMatch(0);
      }
      if (e.key === 'Escape' && searchQuery) {
        handleSearchClose();
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [searchQuery, handleSearchClose]);

  const handleSearchQueryChange = useCallback((q: string) => {
    setSearchQuery(q);
    setCurrentMatch(0);
  }, []);

  return (
    <div className="flex flex-col h-full relative">
      <TopBar
        peerName={peerName}
        chatType={chatType}
        isOnline={isPeerOnline}
        onSettingsClick={handleSettingsClick}
      />
      {/* In-conversation search bar */}
      {searchQuery !== '' && (
        <ChatSearch
          query={searchQuery}
          onQueryChange={handleSearchQueryChange}
          matchCount={matchIds.length}
          currentMatch={currentMatch}
          onNext={handleSearchNext}
          onPrev={handleSearchPrev}
          onClose={handleSearchClose}
        />
      )}

      <MessageList
        messages={conv?.messages || []}
        myUid={uid}
        onRecall={handleRecall}
        onReply={handleReply}
        onForward={handleForward}
        onEdit={handleEdit}
        onDelete={handleDelete}
        highlight={searchQuery || undefined}
        currentMatchId={searchQuery && matchIds.length > 0 ? matchIds[currentMatch] : undefined}
      />
      {/* Typing indicator */}
      {typingUid && (
        <div className="px-4 py-1 bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 dark:text-gray-400 italic border-t border-gray-100 dark:border-gray-800">
          {typingUid} 正在输入...
        </div>
      )}

      <ChatInput
        onSend={handleSendText}
        onSendFile={handleSendFile}
        uploading={uploading}
        uploadProgress={progress}
        disabled={uploading}
        peerId={peerId}
        chatType={chatType}
        groupMembers={groupMembers}
        replyTo={replyTo}
        onCancelReply={handleCancelReply}
      />

      {/* Group info side panel */}
      {isGroup && groupInfo && (
        <GroupInfoPanel
          group={groupInfo}
          open={groupPanelOpen}
          onClose={() => setGroupPanelOpen(false)}
          onMarkUnread={handleMarkUnread}
          onDeleteConversation={handleDeleteConversation}
        />
      )}

      {/* Friend settings side panel */}
      {!isGroup && (
        <FriendInfoPanel
          peerId={peerId}
          peerName={peerName}
          open={friendPanelOpen}
          onClose={() => setFriendPanelOpen(false)}
          onMarkUnread={handleMarkUnread}
          onDeleteConversation={handleDeleteConversation}
        />
      )}

      {/* Forward dialog */}
      {forwardMsg && (
        <ForwardDialog
          message={forwardMsg}
          onForward={handleForwardSend}
          onClose={() => setForwardMsg(null)}
        />
      )}
    </div>
  );
}
