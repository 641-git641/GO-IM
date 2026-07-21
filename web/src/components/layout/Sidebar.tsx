import { NavLink } from 'react-router-dom';
import {
  MessageCircle,
  Users,
  Search,
  Settings,
  LogOut,
  Shield,
} from 'lucide-react';
import { useAuthStore } from '@/stores/authStore';
import { useWSStore } from '@/stores/wsStore';
import { wsManager } from '@/lib/ws';
import { useUnreadCounts } from '@/hooks/useUnreadCounts';

const navItems = [
  { to: '/chat', icon: MessageCircle, label: '消息' },
  { to: '/contacts', icon: Users, label: '通讯录' },
  { to: '/search', icon: Search, label: '搜索' },
  { to: '/settings', icon: Settings, label: '设置' },
];

export default function Sidebar() {
  const { uid, username, isAdmin, logout } = useAuthStore();
  const status = useWSStore((s) => s.status);
  const { totalUnread } = useUnreadCounts();

  const handleLogout = () => {
    wsManager.disconnect();
    logout();
  };

  return (
    <aside className="w-16 lg:w-64 flex flex-col bg-gray-50 dark:bg-gray-950 border-r border-gray-200 dark:border-gray-800 flex-shrink-0">
      {/* User info */}
      <div className="p-3 lg:p-4 border-b border-gray-200 dark:border-gray-800">
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 rounded-full bg-primary-500 flex items-center justify-center text-white font-semibold text-sm flex-shrink-0">
            {username.slice(0, 2).toUpperCase()}
          </div>
          <div className="hidden lg:block flex-1 min-w-0">
            <p className="text-sm font-medium text-gray-900 dark:text-gray-100 truncate">{username}</p>
            <p className="text-xs text-gray-500 dark:text-gray-400 truncate">{uid}</p>
          </div>
        </div>
        {/* Connection status */}
        <div className="hidden lg:flex items-center gap-1.5 mt-2">
          <span
            className={`w-2 h-2 rounded-full ${
              status === 'connected'
                ? 'bg-green-500'
                : status === 'connecting'
                  ? 'bg-yellow-500 animate-pulse'
                  : 'bg-red-500'
            }`}
          />
          <span className="text-xs text-gray-400 dark:text-gray-500">
            {status === 'connected' ? '已连接' : status === 'connecting' ? '连接中...' : '已断开'}
          </span>
        </div>
      </div>

      {/* Navigation */}
      <nav className="flex-1 p-2 space-y-1">
        {isAdmin && (
          <NavLink
            to="/admin"
            className={({ isActive }) =>
              `flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-colors ${
                isActive
                  ? 'bg-primary-100 dark:bg-primary-900/30 text-primary-700 dark:text-primary-300'
                  : 'text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-800 hover:text-gray-900 dark:hover:text-gray-200'
              }`
            }
          >
            <Shield className="w-5 h-5 flex-shrink-0" />
            <span className="hidden lg:inline">管理后台</span>
          </NavLink>
        )}
        {navItems.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            className={({ isActive }) =>
              `flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-colors ${
                isActive
                  ? 'bg-primary-100 dark:bg-primary-900/30 text-primary-700 dark:text-primary-300'
                  : 'text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-800 hover:text-gray-900 dark:hover:text-gray-200'
              }`
            }
          >
            <div className="relative">
              <item.icon className="w-5 h-5 flex-shrink-0" />
              {item.to === '/chat' && totalUnread > 0 && (
                <span className="absolute -top-1 -right-1 w-4 h-4 bg-red-500 text-white text-[10px] font-bold rounded-full flex items-center justify-center">
                  {totalUnread > 99 ? '99+' : totalUnread}
                </span>
              )}
            </div>
            <span className="hidden lg:inline">{item.label}</span>
          </NavLink>
        ))}
      </nav>

      {/* Logout */}
      <div className="p-2 border-t border-gray-200 dark:border-gray-800">
        <button
          onClick={handleLogout}
          className="flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium text-gray-500 dark:text-gray-400 hover:bg-red-50 dark:hover:bg-red-900/20 hover:text-red-600 dark:hover:text-red-400 transition-colors w-full"
        >
          <LogOut className="w-5 h-5 flex-shrink-0" />
          <span className="hidden lg:inline">退出登录</span>
        </button>
      </div>
    </aside>
  );
}
