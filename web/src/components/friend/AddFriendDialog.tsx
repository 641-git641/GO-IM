import { useState, type FormEvent } from 'react';
import { X } from 'lucide-react';
import { sendFriendRequest } from '@/lib/api';
import { useAuthStore } from '@/stores/authStore';
import { wsManager } from '@/lib/ws';
import { Cmd, ChatType, MsgType } from '@/types';

interface AddFriendDialogProps {
  open: boolean;
  onClose: () => void;
}

export default function AddFriendDialog({ open, onClose }: AddFriendDialogProps) {
  const [targetUid, setTargetUid] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const { uid: myUid, username } = useAuthStore();

  if (!open) return null;

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    const trimmed = targetUid.trim();
    if (!trimmed || !myUid) return;

    if (trimmed === myUid) {
      setError('不能添加自己为好友');
      return;
    }

    setLoading(true);
    setError('');

    try {
      await sendFriendRequest(trimmed);
      // 通过 WebSocket 实时通知。
      wsManager.send({
        seq: '0',
        msgId: '0',
        cmd: Cmd.FriendRequest,
        from: myUid,
        to: trimmed,
        chatType: ChatType.Single,
        msgType: MsgType.Text,
        content: '',
        timestamp: String(Date.now()),
        needAck: false,
      });
      setError('好友请求已发送');
      setTargetUid('');
      setTimeout(() => {
        setError('');
        onClose();
      }, 1200);
    } catch (err) {
      setError(err instanceof Error ? err.message : '发送失败');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-sm mx-4 p-6 space-y-4">
        <div className="flex items-center justify-between">
          <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100">添加好友</h3>
          <button onClick={onClose} className="p-1 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-800">
            <X className="w-5 h-5 text-gray-500" />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">用户 UID</label>
            <input
              type="text"
              value={targetUid}
              onChange={(e) => setTargetUid(e.target.value)}
              placeholder="输入要添加的用户 UID"
              autoFocus
              className="w-full px-4 py-2.5 rounded-lg border border-gray-300 dark:border-gray-700 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-primary-500 focus:border-primary-500 outline-none text-sm"
            />
          </div>

          {error && (
            <div className={`p-2 rounded-lg text-xs ${
              error.includes('已发送') || error.includes('好友请求')
                ? 'bg-green-50 dark:bg-green-900/20 text-green-600 dark:text-green-400'
                : 'bg-red-50 dark:bg-red-900/20 text-red-600 dark:text-red-400'
            }`}>
              {error}
            </div>
          )}

          <div className="flex gap-2 justify-end">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 text-sm text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-lg transition-colors"
            >
              取消
            </button>
            <button
              type="submit"
              disabled={loading || !targetUid.trim()}
              className="px-4 py-2 text-sm bg-primary-600 text-white rounded-lg hover:bg-primary-700 disabled:opacity-50 transition-colors"
            >
              {loading ? '发送中...' : '添加好友'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
