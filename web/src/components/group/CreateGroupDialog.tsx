import { useState, useEffect, type FormEvent } from 'react';
import { X, Check, Users } from 'lucide-react';
import { createGroup, getGroupList, getFriendList } from '@/lib/api';
import { useAuthStore } from '@/stores/authStore';
import { useContactStore } from '@/stores/contactStore';
import { useFriendStore } from '@/stores/friendStore';

interface CreateGroupDialogProps {
  open: boolean;
  onClose: () => void;
}

export default function CreateGroupDialog({ open, onClose }: CreateGroupDialogProps) {
  const [name, setName] = useState('');
  const [selectedFriends, setSelectedFriends] = useState<Set<string>>(new Set());
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const { uid } = useAuthStore();
  const { setGroups } = useContactStore();
  const { friends, setFriends } = useFriendStore();

  // 对话框打开时加载好友列表。
  useEffect(() => {
    if (open && uid) {
      getFriendList()
        .then((data) => {
          setFriends(data.friends || []);
        })
        .catch(() => {});
    }
  }, [open, uid, setFriends]);

  if (!open) return null;

  const toggleFriend = (friendUid: string) => {
    setSelectedFriends((prev) => {
      const next = new Set(prev);
      if (next.has(friendUid)) {
        next.delete(friendUid);
      } else {
        next.add(friendUid);
      }
      return next;
    });
  };

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    if (!name.trim() || !uid) return;

    setLoading(true);
    setError('');

    try {
      const members = Array.from(selectedFriends);
      await createGroup(name.trim(), members);
      // 刷新群组列表。
      const data = await getGroupList();
      setGroups(data.groups);
      onClose();
      setName('');
      setSelectedFriends(new Set());
    } catch (err) {
      setError(err instanceof Error ? err.message : '创建失败');
    } finally {
      setLoading(false);
    }
  };

  const handleClose = () => {
    setName('');
    setSelectedFriends(new Set());
    setError('');
    onClose();
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-sm mx-4 p-6 space-y-4 max-h-[80vh] flex flex-col">
        <div className="flex items-center justify-between">
          <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100">创建群组</h3>
          <button onClick={handleClose} className="p-1 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-800">
            <X className="w-5 h-5 text-gray-500" />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="flex-1 overflow-y-auto space-y-4">
          {/* 群组名称 */}
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">群组名称</label>
            <input
              type="text"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="输入群组名称"
              autoFocus
              className="w-full px-4 py-2.5 rounded-lg border border-gray-300 dark:border-gray-700 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-primary-500 focus:border-primary-500 outline-none text-sm"
            />
          </div>

          {/* 好友选择 */}
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              邀请好友 <span className="text-gray-400 font-normal">（可选，{selectedFriends.size} 人已选）</span>
            </label>
            {friends.length === 0 ? (
              <p className="text-xs text-gray-400 dark:text-gray-500 py-2">
                暂无好友，请先在通讯录中添加好友
              </p>
            ) : (
              <div className="space-y-1 max-h-48 overflow-y-auto border border-gray-200 dark:border-gray-700 rounded-lg">
                {friends.map((f) => {
                  const isSelected = selectedFriends.has(f.uid);
                  return (
                    <button
                      key={f.uid}
                      type="button"
                      onClick={() => toggleFriend(f.uid)}
                      className={`w-full flex items-center gap-3 px-3 py-2.5 text-sm transition-colors ${
                        isSelected
                          ? 'bg-primary-50 dark:bg-primary-900/20 text-primary-700 dark:text-primary-300'
                          : 'hover:bg-gray-50 dark:hover:bg-gray-800 text-gray-700 dark:text-gray-300'
                      }`}
                    >
                      <div className={`w-5 h-5 rounded border-2 flex items-center justify-center flex-shrink-0 ${
                        isSelected
                          ? 'bg-primary-500 border-primary-500'
                          : 'border-gray-300 dark:border-gray-600'
                      }`}>
                        {isSelected && <Check className="w-3 h-3 text-white" />}
                      </div>
                      <Users className="w-4 h-4 text-gray-400 flex-shrink-0" />
                      <span className="truncate">{f.uid}</span>
                    </button>
                  );
                })}
              </div>
            )}
          </div>

          {error && (
            <div className="p-2 rounded-lg bg-red-50 dark:bg-red-900/20 text-red-600 dark:text-red-400 text-xs">{error}</div>
          )}

          <div className="flex gap-2 justify-end pt-2">
            <button
              type="button"
              onClick={handleClose}
              className="px-4 py-2 text-sm text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-lg transition-colors"
            >
              取消
            </button>
            <button
              type="submit"
              disabled={loading || !name.trim()}
              className="px-4 py-2 text-sm bg-primary-600 text-white rounded-lg hover:bg-primary-700 disabled:opacity-50 transition-colors"
            >
              {loading ? '创建中...' : `创建${selectedFriends.size > 0 ? ` (${selectedFriends.size + 1}人)` : ''}`}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
