import { useState, useEffect } from 'react';
import { useOnlineUsers } from '@/hooks/useOnlineUsers';
import { useContactStore } from '@/stores/contactStore';
import { useAuthStore } from '@/stores/authStore';
import { getGroupList } from '@/lib/api';
import ContactList from '@/components/contact/ContactList';
import CreateGroupDialog from '@/components/group/CreateGroupDialog';
import AddFriendDialog from '@/components/friend/AddFriendDialog';
import { Plus, UserPlus } from 'lucide-react';

export default function ContactsPage() {
  useOnlineUsers();
  const [showCreateGroup, setShowCreateGroup] = useState(false);
  const [showAddFriend, setShowAddFriend] = useState(false);
  const { uid } = useAuthStore();
  const { setGroups } = useContactStore();

  useEffect(() => {
    if (!uid) return;
    getGroupList().then((data) => {
      setGroups(data.groups || []);
    }).catch(() => {});
  }, [uid, setGroups]);

  return (
    <div className="flex flex-col h-full">
      <div className="h-14 px-4 flex items-center justify-between border-b border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900 flex-shrink-0">
        <h2 className="text-base font-semibold text-gray-900 dark:text-gray-100">通讯录</h2>
        <div className="flex items-center gap-1">
          <button
            onClick={() => setShowAddFriend(true)}
            className="p-2 rounded-lg text-gray-500 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-800 hover:text-primary-600 dark:hover:text-primary-400 transition-colors"
            title="添加好友"
          >
            <UserPlus className="w-5 h-5" />
          </button>
          <button
            onClick={() => setShowCreateGroup(true)}
            className="p-2 rounded-lg text-gray-500 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-800 hover:text-primary-600 dark:hover:text-primary-400 transition-colors"
            title="创建群组"
          >
            <Plus className="w-5 h-5" />
          </button>
        </div>
      </div>
      <ContactList />
      <AddFriendDialog open={showAddFriend} onClose={() => setShowAddFriend(false)} />
      <CreateGroupDialog open={showCreateGroup} onClose={() => setShowCreateGroup(false)} />
    </div>
  );
}
