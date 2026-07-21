import { useState, useEffect, useCallback } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useAuthStore } from '@/stores/authStore';
import { useContactStore } from '@/stores/contactStore';
import { useFriendStore } from '@/stores/friendStore';
import { useChatStore } from '@/stores/chatStore';
import { wsManager } from '@/lib/ws';
import { getFriendList, sendFriendRequest, acceptFriendRequest, removeFriend } from '@/lib/api';
import { Cmd, ChatType, MsgType } from '@/types';
import { ArrowLeft, MessageCircle, UserPlus, UserCheck, UserX, Clock } from 'lucide-react';

export default function UserProfilePage() {
  const { uid: profileUid } = useParams<{ uid: string }>();
  const { uid: myUid } = useAuthStore();
  const { onlineUsers } = useContactStore();
  const { friends, pendingRequests, setFriends, setPendingRequests, addFriend, removeFriend: removeFriendFromStore, addPendingRequest, removePendingRequest } = useFriendStore();
  const setActivePeer = useChatStore((s) => s.setActivePeer);
  const navigate = useNavigate();

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [actionLoading, setActionLoading] = useState(false);

  const isOnline = onlineUsers.includes(profileUid || '');
  const isSelf = profileUid === myUid;

  // Determine friendship status
  const friendRel = friends.find((f) => f.uid === profileUid);
  const isFriend = !!friendRel;
  const pendingReq = pendingRequests.find((r) => r.from_uid === profileUid);
  const hasPending = !!pendingReq;

  const loadFriendData = useCallback(async () => {
    if (!myUid) return;
    try {
      const data = await getFriendList();
      setFriends(data.friends);
      setPendingRequests(data.pending_requests);
    } catch {
      // ignore errors — friend system might be unavailable
    } finally {
      setLoading(false);
    }
  }, [myUid, setFriends, setPendingRequests]);

  useEffect(() => {
    loadFriendData();
  }, [loadFriendData]);

  const handleSendRequest = async () => {
    if (!profileUid || !myUid) return;
    setActionLoading(true);
    setError('');
    try {
      await sendFriendRequest(profileUid);
      // Also notify via WebSocket
      wsManager.send({
        seq: '0',
        msgId: '0',
        cmd: Cmd.FriendRequest,
        from: myUid,
        to: profileUid,
        chatType: ChatType.Single,
        msgType: MsgType.Text,
        content: '',
        timestamp: String(Date.now()),
        needAck: false,
      });
      setError('好友请求已发送');
    } catch (err) {
      setError(err instanceof Error ? err.message : '发送失败');
    } finally {
      setActionLoading(false);
    }
  };

  const handleAccept = async () => {
    if (!profileUid || !myUid) return;
    setActionLoading(true);
    setError('');
    try {
      await acceptFriendRequest(profileUid);
      addFriend({ uid: profileUid, friend_uid: myUid, status: 1, created_at: Date.now() });
      removePendingRequest(profileUid);
      // Notify via WebSocket
      wsManager.send({
        seq: '0',
        msgId: '0',
        cmd: Cmd.FriendResponse,
        from: myUid,
        to: profileUid,
        chatType: ChatType.Single,
        msgType: MsgType.Text,
        content: JSON.stringify({ action: 'accept' }),
        timestamp: String(Date.now()),
        needAck: false,
      });
      setError('已接受好友请求');
    } catch (err) {
      setError(err instanceof Error ? err.message : '操作失败');
    } finally {
      setActionLoading(false);
    }
  };

  const handleRemove = async () => {
    if (!profileUid || !myUid) return;
    if (!confirm('确定要删除该好友吗？')) return;
    setActionLoading(true);
    setError('');
    try {
      await removeFriend(profileUid);
      removeFriendFromStore(profileUid);
      setError('已删除好友');
    } catch (err) {
      setError(err instanceof Error ? err.message : '删除失败');
    } finally {
      setActionLoading(false);
    }
  };

  const handleSendMessage = () => {
    if (!profileUid) return;
    setActivePeer(profileUid);
    navigate(`/chat/${profileUid}`);
  };

  if (!profileUid) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <p className="text-sm text-gray-400">用户不存在</p>
      </div>
    );
  }

  return (
    <div className="flex-1 overflow-y-auto bg-gray-50 dark:bg-gray-950">
      <div className="max-w-2xl mx-auto p-6">
        {/* Back button */}
        <button
          onClick={() => navigate(-1)}
          className="flex items-center gap-2 text-sm text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300 mb-4 transition-colors"
        >
          <ArrowLeft className="w-4 h-4" />
          返回
        </button>

        {/* Profile card */}
        <div className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 overflow-hidden">
          {/* Header */}
          <div className="p-6 text-center">
            <div className="w-20 h-20 rounded-full bg-primary-500 flex items-center justify-center text-white text-2xl font-bold mx-auto">
              {profileUid.slice(0, 2).toUpperCase()}
            </div>
            <h1 className="mt-3 text-xl font-bold text-gray-900 dark:text-gray-100">{profileUid}</h1>
            <div className="flex items-center justify-center gap-2 mt-1">
              <span className={`w-2.5 h-2.5 rounded-full ${isOnline ? 'bg-green-500' : 'bg-gray-300'}`} />
              <span className="text-sm text-gray-500 dark:text-gray-400">{isOnline ? '在线' : '离线'}</span>
            </div>
          </div>

          {/* Actions */}
          {!isSelf && (
            <div className="px-6 pb-6 space-y-3">
              {error && (
                <div className={`p-3 rounded-lg text-sm ${
                  error.includes('已') || error.includes('请求已发送')
                    ? 'bg-green-50 text-green-600'
                    : 'bg-red-50 text-red-600'
                }`}>
                  {error}
                </div>
              )}

              <div className="flex gap-2">
                {/* Send Message */}
                <button
                  onClick={handleSendMessage}
                  className="flex-1 flex items-center justify-center gap-2 py-2.5 bg-primary-500 text-white text-sm font-medium rounded-lg hover:bg-primary-600 transition-colors"
                >
                  <MessageCircle className="w-4 h-4" />
                  发消息
                </button>

                {/* Friend action */}
                {isFriend ? (
                  <button
                    onClick={handleRemove}
                    disabled={actionLoading}
                    className="flex-1 flex items-center justify-center gap-2 py-2.5 border border-red-200 text-red-600 text-sm font-medium rounded-lg hover:bg-red-50 transition-colors disabled:opacity-50"
                  >
                    <UserX className="w-4 h-4" />
                    删除好友
                  </button>
                ) : hasPending ? (
                  <button
                    onClick={handleAccept}
                    disabled={actionLoading}
                    className="flex-1 flex items-center justify-center gap-2 py-2.5 bg-green-500 text-white text-sm font-medium rounded-lg hover:bg-green-600 transition-colors disabled:opacity-50"
                  >
                    <UserCheck className="w-4 h-4" />
                    {actionLoading ? '处理中...' : '接受请求'}
                  </button>
                ) : (
                  <button
                    onClick={handleSendRequest}
                    disabled={actionLoading}
                    className="flex-1 flex items-center justify-center gap-2 py-2.5 border border-primary-300 text-primary-600 text-sm font-medium rounded-lg hover:bg-primary-50 transition-colors disabled:opacity-50"
                  >
                    <UserPlus className="w-4 h-4" />
                    {actionLoading ? '发送中...' : '添加好友'}
                  </button>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
