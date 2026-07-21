import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useContactStore } from '@/stores/contactStore';
import { useAuthStore } from '@/stores/authStore';
import { useFriendStore } from '@/stores/friendStore';
import { getFriendList, acceptFriendRequest, rejectFriendRequest } from '@/lib/api';
import { wsManager } from '@/lib/ws';
import { Cmd, ChatType, MsgType } from '@/types';
import ContactItem from './ContactItem';
import { Check, X } from 'lucide-react';

export default function ContactList() {
  const { onlineUsers, groups } = useContactStore();
  const { uid } = useAuthStore();
  const { friends, pendingRequests, setFriends, setPendingRequests, addFriend, removePendingRequest } = useFriendStore();
  const [processingUid, setProcessingUid] = useState<string | null>(null);
  const navigate = useNavigate();

  // Load friend data
  useEffect(() => {
    if (!uid) return;
    getFriendList()
      .then((data) => {
        setFriends(data.friends || []);
        setPendingRequests(data.pending_requests || []);
      })
      .catch(() => {});
  }, [uid, setFriends, setPendingRequests]);

  const handleAccept = async (fromUid: string) => {
    if (!uid) return;
    setProcessingUid(fromUid);
    try {
      await acceptFriendRequest(fromUid);
      addFriend({ uid: fromUid, friend_uid: uid, status: 1, created_at: Date.now() });
      removePendingRequest(fromUid);
      // Notify via WebSocket.
      wsManager.send({
        seq: '0',
        msgId: '0',
        cmd: Cmd.FriendResponse,
        from: uid,
        to: fromUid,
        chatType: ChatType.Single,
        msgType: MsgType.Text,
        content: JSON.stringify({ action: 'accept' }),
        timestamp: String(Date.now()),
        needAck: false,
      });
    } catch {
      // ignore
    } finally {
      setProcessingUid(null);
    }
  };

  const handleReject = async (fromUid: string) => {
    if (!uid) return;
    setProcessingUid(fromUid);
    try {
      await rejectFriendRequest(fromUid);
      removePendingRequest(fromUid);
    } catch {
      // ignore
    } finally {
      setProcessingUid(null);
    }
  };
  return (
    <div className="flex-1 overflow-y-auto">
      {/* Pending requests section */}
      {pendingRequests.length > 0 && (
        <div>
          <div className="px-4 py-2 text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase bg-gray-50 dark:bg-gray-800 sticky top-0">
            新的好友请求 · {pendingRequests.length}
          </div>
          {pendingRequests.map((req) => (
            <div key={req.from_uid} className="flex items-center">
              <div className="flex-1 min-w-0">
                <ContactItem
                  uid={req.from_uid}
                  name={req.username || req.from_uid}
                  isOnline={onlineUsers.includes(req.from_uid)}
                  onClick={() => navigate(`/user/${req.from_uid}`)}
                />
              </div>
              <div className="flex items-center gap-1 pr-3">
                <button
                  onClick={(e) => { e.stopPropagation(); handleAccept(req.from_uid); }}
                  disabled={processingUid === req.from_uid}
                  className="p-1.5 rounded-lg text-green-600 dark:text-green-400 hover:bg-green-50 dark:hover:bg-green-900/20 transition-colors disabled:opacity-50"
                  title="接受"
                >
                  <Check className="w-4 h-4" />
                </button>
                <button
                  onClick={(e) => { e.stopPropagation(); handleReject(req.from_uid); }}
                  disabled={processingUid === req.from_uid}
                  className="p-1.5 rounded-lg text-red-500 dark:text-red-400 hover:bg-red-50 dark:hover:bg-red-900/20 transition-colors disabled:opacity-50"
                  title="拒绝"
                >
                  <X className="w-4 h-4" />
                </button>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Friends section */}
      {friends.length > 0 && (
        <div>
          <div className="px-4 py-2 text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase bg-gray-50 dark:bg-gray-800 sticky top-0">
            好友 · {friends.length}
          </div>
          {friends.map((f) => (
            <ContactItem
              key={f.uid}
              uid={f.uid}
              name={f.uid}
              isOnline={onlineUsers.includes(f.uid)}
              onClick={() => navigate(`/user/${f.uid}`)}
            />
          ))}
        </div>
      )}

      {/* Groups section */}
      {groups.length > 0 && (
        <div>
          <div className="px-4 py-2 text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase bg-gray-50 dark:bg-gray-800 sticky top-0">
            群组 · {groups.length}
          </div>
          {groups.map((group) => (
            <ContactItem
              key={group.id}
              uid={group.id}
              name={group.name}
              isOnline={false}
              isGroup={true}
              memberCount={group.member_count}
              onClick={() => navigate(`/chat/${group.id}`)}
            />
          ))}
        </div>
      )}

      {pendingRequests.length === 0 && friends.length === 0 && groups.length === 0 && (
        <div className="flex items-center justify-center h-32">
          <p className="text-sm text-gray-400">暂无好友或群组，请先添加好友</p>
        </div>
      )}
    </div>
  );
}
