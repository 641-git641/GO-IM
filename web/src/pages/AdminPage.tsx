import { useEffect } from 'react';
import { useAuthStore } from '@/stores/authStore';
import { useAdminStore } from '@/stores/adminStore';
import { formatTime, tryParseJSON } from '@/lib/utils';
import type { AdminStats, AdminUser, SearchResultMessage } from '@/types';
import {
  Activity,
  Users,
  MessageSquare,
  Cpu,
  Zap,
  Server,
  ShieldAlert,
  Trash2,
} from 'lucide-react';

export default function AdminPage() {
  const { uid, token, isAdmin } = useAuthStore();
  const {
    activeTab,
    setActiveTab,
    // Dashboard
    stats,
    statsLoading,
    statsError,
    fetchStats,
    // Users
    users,
    usersTotal,
    usersLoading,
    usersError,
    fetchUsers,
    removeUser,
    // Messages
    messages,
    messagesLoading,
    messagesError,
    fetchMessages,
    removeMessage,
  } = useAdminStore();

  useEffect(() => {
    if (isAdmin && uid && token) {
      fetchStats(uid, token);
    }
  }, [isAdmin, uid, token, fetchStats]);

  if (!isAdmin) {
    return (
      <div className="flex-1 flex items-center justify-center bg-gray-50 dark:bg-gray-950">
        <div className="text-center space-y-3">
          <ShieldAlert className="w-16 h-16 text-red-400 mx-auto" />
          <h1 className="text-xl font-bold text-gray-900 dark:text-gray-100">无权访问</h1>
          <p className="text-sm text-gray-500">您没有管理员权限</p>
        </div>
      </div>
    );
  }

  const tabs = [
    { key: 'dashboard' as const, label: '仪表盘', icon: Activity },
    { key: 'users' as const, label: '用户管理', icon: Users },
    { key: 'messages' as const, label: '消息审核', icon: MessageSquare },
  ];

  return (
    <div className="flex-1 overflow-y-auto bg-gray-50 dark:bg-gray-950">
      <div className="max-w-6xl mx-auto p-6 space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-xl font-bold text-gray-900 dark:text-gray-100">管理后台</h1>
            <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">系统监控与内容管理</p>
          </div>
        </div>

        {/* Tab bar */}
        <div className="flex gap-1 bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 p-1">
          {tabs.map((tab) => {
            const Icon = tab.icon;
            const isActive = activeTab === tab.key;
            return (
              <button
                key={tab.key}
                onClick={() => {
                  setActiveTab(tab.key);
                  if (tab.key === 'users' && users.length === 0 && uid && token) fetchUsers(uid, token);
                  if (tab.key === 'messages' && messages.length === 0 && uid && token) fetchMessages(uid, token);
                }}
                className={`flex-1 flex items-center justify-center gap-2 py-2.5 rounded-lg text-sm font-medium transition-colors ${
                  isActive
                    ? 'bg-primary-500 text-white shadow-sm'
                    : 'text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
                }`}
              >
                <Icon className="w-4 h-4" />
                {tab.label}
              </button>
            );
          })}
        </div>

        {/* Tab content */}
        {activeTab === 'dashboard' && (
          <DashboardTab stats={stats} loading={statsLoading} error={statsError} />
        )}
        {activeTab === 'users' && (
          <UsersTab
            users={users}
            total={usersTotal}
            loading={usersLoading}
            error={usersError}
            onDelete={(targetUid) => { if (uid && token) removeUser(uid, token, targetUid); }}
            uid={uid}
            token={token}
          />
        )}
        {activeTab === 'messages' && (
          <MessagesTab
            messages={messages}
            loading={messagesLoading}
            error={messagesError}
            onDelete={(msgId) => { if (uid && token) removeMessage(uid, token, msgId); }}
          />
        )}
      </div>
    </div>
  );
}

// ---- Dashboard Tab ----

function DashboardTab({ stats, loading, error }: {
  stats: AdminStats | null;
  loading: boolean;
  error: string | null;
}) {
  if (loading) {
    return <div className="text-center py-12 text-sm text-gray-400">加载中...</div>;
  }
  if (error) {
    return <div className="text-center py-12 text-sm text-red-500">{error}</div>;
  }
  if (!stats) return null;

  const cards = [
    { label: '在线用户', value: stats.online_users, icon: Zap, color: 'text-green-600' },
    { label: '总用户数', value: stats.total_users, icon: Users, color: 'text-blue-600' },
    { label: '总消息数', value: stats.total_messages, icon: MessageSquare, color: 'text-purple-600' },
    { label: '内存 (MB)', value: stats.memory.alloc_mb, icon: Cpu, color: 'text-orange-600' },
    { label: 'Goroutines', value: stats.memory.goroutines, icon: Server, color: 'text-teal-600' },
  ];

  return (
    <div className="space-y-6">
      {/* Stat cards */}
      <div className="grid grid-cols-2 lg:grid-cols-5 gap-4">
        {cards.map((card) => (
          <div
            key={card.label}
            className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 p-4"
          >
            <card.icon className={`w-5 h-5 ${card.color} mb-2`} />
            <p className="text-2xl font-bold text-gray-900 dark:text-gray-100">{card.value}</p>
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">{card.label}</p>
          </div>
        ))}
      </div>

      {/* Dependencies */}
      <div className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 overflow-hidden">
        <div className="px-5 py-4 border-b border-gray-100 dark:border-gray-800">
          <h2 className="text-sm font-semibold text-gray-900 dark:text-gray-100">依赖服务状态</h2>
        </div>
        <div className="p-5 grid grid-cols-2 gap-3">
          {Object.entries(stats.dependencies).map(([name, status]) => (
            <div key={name} className="flex items-center justify-between">
              <span className="text-sm text-gray-600 dark:text-gray-400 capitalize">{name}</span>
              <span
                className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-medium ${
                  status === 'ok'
                    ? 'bg-green-50 dark:bg-green-900/20 text-green-600 dark:text-green-400'
                    : 'bg-red-50 dark:bg-red-900/20 text-red-600 dark:text-red-400'
                }`}
              >
                <span className={`w-1.5 h-1.5 rounded-full ${status === 'ok' ? 'bg-green-500' : 'bg-red-500'}`} />
                {status === 'ok' ? '正常' : status}
              </span>
            </div>
          ))}
        </div>
      </div>

      {/* System status header */}
      <div className="flex items-center gap-2">
        <span className={`w-2.5 h-2.5 rounded-full ${stats.status === 'ok' ? 'bg-green-500' : 'bg-yellow-500'}`} />
        <span className="text-sm text-gray-600 dark:text-gray-400">
          系统状态: {stats.status === 'ok' ? '运行正常' : '部分降级'}
        </span>
      </div>
    </div>
  );
}

// ---- Users Tab ----

function UsersTab({ users, total, loading, error, onDelete, uid, token }: {
  users: AdminUser[];
  total: number;
  loading: boolean;
  error: string | null;
  onDelete: (targetUid: string) => void;
  uid: string;
  token: string;
}) {
  if (loading) {
    return <div className="text-center py-12 text-sm text-gray-400">加载中...</div>;
  }
  if (error) {
    return <div className="text-center py-12 text-sm text-red-500">加载失败: {error}</div>;
  }

  return (
    <div className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 overflow-hidden">
      <div className="px-5 py-4 border-b border-gray-100 dark:border-gray-800 flex items-center justify-between">
        <h2 className="text-sm font-semibold text-gray-900 dark:text-gray-100">
          用户列表 · {total}
        </h2>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-100 dark:border-gray-800">
              <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 dark:text-gray-400">UID</th>
              <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 dark:text-gray-400">用户名</th>
              <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 dark:text-gray-400">角色</th>
              <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 dark:text-gray-400">状态</th>
              <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 dark:text-gray-400">注册时间</th>
              <th className="text-right px-5 py-3 text-xs font-medium text-gray-500 dark:text-gray-400">操作</th>
            </tr>
          </thead>
          <tbody>
            {users.map((u) => (
              <tr key={u.uid} className="border-b border-gray-50 dark:border-gray-800 hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors">
                <td className="px-5 py-3 text-gray-900 dark:text-gray-100 font-mono text-xs">{u.uid}</td>
                <td className="px-5 py-3 text-gray-700 dark:text-gray-300">{u.username}</td>
                <td className="px-5 py-3">
                  <span className={`inline-flex px-2 py-0.5 rounded-full text-xs font-medium ${
                    u.role === 'admin'
                      ? 'bg-purple-50 dark:bg-purple-900/20 text-purple-600 dark:text-purple-400'
                      : 'bg-gray-100 dark:bg-gray-800 text-gray-600 dark:text-gray-400'
                  }`}>
                    {u.role === 'admin' ? '管理员' : '用户'}
                  </span>
                </td>
                <td className="px-5 py-3">
                  <span className={`inline-flex items-center gap-1.5 text-xs ${
                    u.is_disabled ? 'text-red-500' : 'text-green-500'
                  }`}>
                    <span className={`w-1.5 h-1.5 rounded-full ${u.is_disabled ? 'bg-red-500' : 'bg-green-500'}`} />
                    {u.is_disabled ? '已禁用' : '正常'}
                  </span>
                </td>
                <td className="px-5 py-3 text-xs text-gray-500 dark:text-gray-400">
                  {formatTime(u.created_at)}
                </td>
                <td className="px-5 py-3 text-right">
                  <button
                    onClick={() => {
                      if (u.uid === uid) {
                        alert('不能删除自己');
                        return;
                      }
                      if (confirm(`确定要删除用户 ${u.username || u.uid} 吗？此操作不可撤销。`)) {
                        onDelete(u.uid);
                      }
                    }}
                    className="inline-flex items-center gap-1 px-2 py-1 rounded text-xs text-red-500 hover:bg-red-50 dark:hover:bg-red-900/20 transition-colors"
                  >
                    <Trash2 className="w-3 h-3" />
                    删除
                  </button>
                </td>
              </tr>
            ))}
            {users.length === 0 && (
              <tr>
                <td colSpan={6} className="px-5 py-8 text-center text-sm text-gray-400">暂无用户</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ---- Messages Tab ----

/** Extract human-readable text from message content (may be JSON) */
function extractMessageText(content: string): string {
  const parsed = tryParseJSON<{ text?: string; type?: string; name?: string; username?: string; uid?: string; from_uid?: string }>(content);
  if (!parsed) return content;
  if (parsed.text) return parsed.text;
  if (parsed.type === 'friend_request') return `[好友请求] ${parsed.username || parsed.from_uid || ''}`;
  if (parsed.type === 'friend_accepted') return '[已同意好友请求]';
  if (parsed.type === 'group_created') return `[创建群组] ${parsed.name || ''}`;
  if (parsed.type === 'member_joined') return `[${parsed.uid || ''} 加入群聊]`;
  if (parsed.type === 'member_left') return `[${parsed.uid || ''} 退出群聊]`;
  return content;
}

function MessagesTab({ messages, loading, error, onDelete }: {
  messages: SearchResultMessage[];
  loading: boolean;
  error: string | null;
  onDelete: (msgId: string) => void;
}) {
  if (loading) {
    return <div className="text-center py-12 text-sm text-gray-400">加载中...</div>;
  }
  if (error) {
    return <div className="text-center py-12 text-sm text-red-500">加载失败: {error}</div>;
  }

  return (
    <div className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 overflow-hidden">
      <div className="px-5 py-4 border-b border-gray-100 dark:border-gray-800 flex items-center justify-between">
        <h2 className="text-sm font-semibold text-gray-900 dark:text-gray-100">最近消息</h2>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-100 dark:border-gray-800">
              <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 dark:text-gray-400">ID</th>
              <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 dark:text-gray-400">发送者</th>
              <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 dark:text-gray-400">接收者</th>
              <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 dark:text-gray-400">内容</th>
              <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 dark:text-gray-400">时间</th>
              <th className="text-right px-5 py-3 text-xs font-medium text-gray-500 dark:text-gray-400">操作</th>
            </tr>
          </thead>
          <tbody>
            {messages.map((msg) => (
              <tr key={msg.msg_id} className="border-b border-gray-50 dark:border-gray-800 hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors">
                <td className="px-5 py-3 text-gray-500 dark:text-gray-400 font-mono text-xs">{msg.msg_id}</td>
                <td className="px-5 py-3 text-gray-700 dark:text-gray-300">{msg.from}</td>
                <td className="px-5 py-3 text-gray-500 dark:text-gray-400 font-mono text-xs">{msg.to}</td>
                <td className="px-5 py-3 text-gray-900 dark:text-gray-100 max-w-xs truncate">
                  {(() => {
                    const text = extractMessageText(msg.content);
                    return text.length > 60 ? text.slice(0, 60) + '...' : text;
                  })()}
                </td>
                <td className="px-5 py-3 text-xs text-gray-500 dark:text-gray-400">{formatTime(msg.timestamp)}</td>
                <td className="px-5 py-3 text-right">
                  <button
                    onClick={() => {
                      if (confirm('确定要删除此消息吗？')) {
                        onDelete(msg.msg_id);
                      }
                    }}
                    className="inline-flex items-center gap-1 px-2 py-1 rounded text-xs text-red-500 hover:bg-red-50 dark:hover:bg-red-900/20 transition-colors"
                  >
                    <Trash2 className="w-3 h-3" />
                    删除
                  </button>
                </td>
              </tr>
            ))}
            {messages.length === 0 && (
              <tr>
                <td colSpan={6} className="px-5 py-8 text-center text-sm text-gray-400">暂无消息</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
