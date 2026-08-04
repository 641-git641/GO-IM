import { useState } from 'react';
import { useAuthStore } from '@/stores/authStore';
import { wsManager } from '@/lib/ws';
import { changePassword } from '@/lib/api';
import { User, Moon, Sun, Key, Shield, Info, LogOut, Check, X } from 'lucide-react';

/** 基于 <html> 上的 CSS class 实现的简单暗色模式切换。
 *  挂载时读取 localStorage(或回退到系统偏好),与
 *  index.html 中的防闪烁初始化脚本保持一致。 */
function useDarkMode() {
  const [dark, setDark] = useState(() => {
    if (typeof document === 'undefined') return false;
    const cls = document.documentElement.classList.contains('dark');
    // 与初始化脚本逻辑一致:若无存储的偏好,则遵循系统设置。
    try {
      const stored = localStorage.getItem('im-dark-mode');
      if (stored === '1') return true;
      if (stored === '0') return false;
    } catch { /* 忽略 */ }
    return cls || window.matchMedia('(prefers-color-scheme: dark)').matches;
  });
  const toggle = () => {
    const next = !dark;
    setDark(next);
    document.documentElement.classList.toggle('dark', next);
    try {
      localStorage.setItem('im-dark-mode', next ? '1' : '0');
    } catch { /* 忽略 */ }
  };
  return { dark, toggle };
}

export default function SettingsPage() {
  const { uid, username, logout } = useAuthStore();
  const { dark, toggle: toggleDark } = useDarkMode();

  const [showPasswordForm, setShowPasswordForm] = useState(false);
  const [oldPassword, setOldPassword] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [passwordMsg, setPasswordMsg] = useState<{ type: 'success' | 'error'; text: string } | null>(null);
  const [changing, setChanging] = useState(false);

  const handleLogout = () => {
    wsManager.disconnect();
    logout();
  };

  const handleChangePassword = async () => {
    setPasswordMsg(null);

    if (!oldPassword || !newPassword || !confirmPassword) {
      setPasswordMsg({ type: 'error', text: '请填写所有密码字段' });
      return;
    }
    if (newPassword !== confirmPassword) {
      setPasswordMsg({ type: 'error', text: '两次输入的新密码不一致' });
      return;
    }
    if (newPassword.length < 6) {
      setPasswordMsg({ type: 'error', text: '新密码至少需要 6 个字符' });
      return;
    }

    setChanging(true);
    try {
      await changePassword(oldPassword, newPassword);
      setPasswordMsg({ type: 'success', text: '密码修改成功' });
      setOldPassword('');
      setNewPassword('');
      setConfirmPassword('');
      setShowPasswordForm(false);
    } catch (err) {
      const msg = err instanceof Error ? err.message : '网络错误，请稍后重试';
      setPasswordMsg({ type: 'error', text: msg });
    } finally {
      setChanging(false);
    }
  };

  return (
    <div className="flex-1 overflow-y-auto bg-gray-50 dark:bg-gray-950">
      <div className="max-w-2xl mx-auto p-6 space-y-6">
        {/* 头部 */}
        <div>
          <h1 className="text-xl font-bold text-gray-900 dark:text-gray-100">设置</h1>
          <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">管理您的账户和应用偏好</p>
        </div>

        {/* 个人信息区块 */}
        <section className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 overflow-hidden">
          <div className="px-5 py-4 border-b border-gray-100 dark:border-gray-800 dark:border-gray-800">
            <div className="flex items-center gap-2">
              <User className="w-4 h-4 text-gray-400 dark:text-gray-500" />
              <h2 className="text-sm font-semibold text-gray-900 dark:text-gray-100">个人信息</h2>
            </div>
          </div>
          <div className="p-5 space-y-3">
            <div className="flex items-center justify-between">
              <span className="text-sm text-gray-500 dark:text-gray-400">头像</span>
              <div className="w-12 h-12 rounded-full bg-primary-500 flex items-center justify-center text-white text-lg font-bold">
                {username.slice(0, 2).toUpperCase()}
              </div>
            </div>
            <div className="flex items-center justify-between py-1">
              <span className="text-sm text-gray-500 dark:text-gray-400">用户名</span>
              <span className="text-sm font-medium text-gray-900 dark:text-gray-100">{username}</span>
            </div>
            <div className="flex items-center justify-between py-1">
              <span className="text-sm text-gray-500 dark:text-gray-400">UID</span>
              <span className="text-sm font-mono text-gray-600 dark:text-gray-400">{uid}</span>
            </div>
          </div>
        </section>

        {/* 外观区块 */}
        <section className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 overflow-hidden">
          <div className="px-5 py-4 border-b border-gray-100 dark:border-gray-800">
            <div className="flex items-center gap-2">
              {dark ? <Moon className="w-4 h-4 text-gray-400 dark:text-gray-500" /> : <Sun className="w-4 h-4 text-gray-400 dark:text-gray-500" />}
              <h2 className="text-sm font-semibold text-gray-900 dark:text-gray-100">外观</h2>
            </div>
          </div>
          <div className="p-5">
            <div className="flex items-center justify-between">
              <div>
                <span className="text-sm text-gray-900 dark:text-gray-100">暗色模式</span>
                <p className="text-xs text-gray-400 mt-0.5">切换应用的颜色主题</p>
              </div>
              <button
                onClick={toggleDark}
                className={`relative w-11 h-6 rounded-full transition-colors duration-200 ${
                  dark ? 'bg-primary-500' : 'bg-gray-300'
                }`}
              >
                <span
                  className={`absolute top-0.5 left-0.5 w-5 h-5 rounded-full bg-white shadow transition-transform duration-200 ${
                    dark ? 'translate-x-5' : ''
                  }`}
                />
              </button>
            </div>
          </div>
        </section>

        {/* 安全区块 */}
        <section className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 overflow-hidden">
          <div className="px-5 py-4 border-b border-gray-100 dark:border-gray-800">
            <div className="flex items-center gap-2">
              <Shield className="w-4 h-4 text-gray-400 dark:text-gray-500" />
              <h2 className="text-sm font-semibold text-gray-900 dark:text-gray-100">安全</h2>
            </div>
          </div>
          <div className="p-5 space-y-3">
            <button
              onClick={() => setShowPasswordForm(!showPasswordForm)}
              className="w-full flex items-center justify-between py-1 hover:bg-gray-50 dark:hover:bg-gray-800 -mx-2 px-2 rounded-lg transition-colors"
            >
              <div className="flex items-center gap-2">
                <Key className="w-4 h-4 text-gray-400 dark:text-gray-500" />
                <span className="text-sm text-gray-900 dark:text-gray-100">修改密码</span>
              </div>
              <span className="text-xs text-gray-400">{showPasswordForm ? '收起' : '展开'}</span>
            </button>

            {showPasswordForm && (
              <div className="space-y-3 pl-6 pt-2">
                <div>
                  <label className="block text-xs text-gray-500 mb-1">当前密码</label>
                  <input
                    type="password"
                    value={oldPassword}
                    onChange={(e) => setOldPassword(e.target.value)}
                    className="w-full px-3 py-2 border border-gray-200 dark:border-gray-700 rounded-lg text-sm focus:outline-none focus:border-primary-400 focus:ring-1 focus:ring-primary-400 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100"
                    placeholder="输入当前密码"
                  />
                </div>
                <div>
                  <label className="block text-xs text-gray-500 mb-1">新密码</label>
                  <input
                    type="password"
                    value={newPassword}
                    onChange={(e) => setNewPassword(e.target.value)}
                    className="w-full px-3 py-2 border border-gray-200 dark:border-gray-700 rounded-lg text-sm focus:outline-none focus:border-primary-400 focus:ring-1 focus:ring-primary-400 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100"
                    placeholder="输入新密码（至少 6 位）"
                  />
                </div>
                <div>
                  <label className="block text-xs text-gray-500 mb-1">确认新密码</label>
                  <input
                    type="password"
                    value={confirmPassword}
                    onChange={(e) => setConfirmPassword(e.target.value)}
                    className="w-full px-3 py-2 border border-gray-200 dark:border-gray-700 rounded-lg text-sm focus:outline-none focus:border-primary-400 focus:ring-1 focus:ring-primary-400 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100"
                    placeholder="再次输入新密码"
                  />
                </div>

                {passwordMsg && (
                  <div
                    className={`flex items-center gap-1.5 text-xs ${
                      passwordMsg.type === 'success' ? 'text-green-600' : 'text-red-500'
                    }`}
                  >
                    {passwordMsg.type === 'success' ? (
                      <Check className="w-3.5 h-3.5" />
                    ) : (
                      <X className="w-3.5 h-3.5" />
                    )}
                    {passwordMsg.text}
                  </div>
                )}

                <div className="flex gap-2">
                  <button
                    onClick={handleChangePassword}
                    disabled={changing}
                    className="px-4 py-2 bg-primary-500 text-white text-sm font-medium rounded-lg hover:bg-primary-600 disabled:opacity-50 transition-colors"
                  >
                    {changing ? '修改中...' : '确认修改'}
                  </button>
                  <button
                    onClick={() => {
                      setShowPasswordForm(false);
                      setPasswordMsg(null);
                    }}
                    className="px-4 py-2 text-sm text-gray-500 hover:text-gray-700 transition-colors"
                  >
                    取消
                  </button>
                </div>
              </div>
            )}
          </div>
        </section>

        {/* 关于区块 */}
        <section className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 overflow-hidden">
          <div className="px-5 py-4 border-b border-gray-100 dark:border-gray-800">
            <div className="flex items-center gap-2">
              <Info className="w-4 h-4 text-gray-400 dark:text-gray-500" />
              <h2 className="text-sm font-semibold text-gray-900 dark:text-gray-100">关于</h2>
            </div>
          </div>
          <div className="p-5 space-y-2">
            <div className="flex items-center justify-between">
              <span className="text-sm text-gray-500 dark:text-gray-400">应用名称</span>
              <span className="text-sm text-gray-900 dark:text-gray-100">IM 即时通讯</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-sm text-gray-500 dark:text-gray-400">版本</span>
              <span className="text-sm text-gray-900 dark:text-gray-100">v1.0.0</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-sm text-gray-500">后端协议</span>
              <span className="text-sm font-mono text-gray-600">WebSocket + Protobuf</span>
            </div>
          </div>
        </section>

        {/* 退出登录 */}
        <section className="bg-white dark:bg-gray-900 rounded-xl border border-red-100 dark:border-red-900 overflow-hidden">
          <div className="p-5">
            <button
              onClick={handleLogout}
              className="w-full flex items-center justify-center gap-2 px-4 py-3 bg-red-50 dark:bg-red-900/20 text-red-600 dark:text-red-400 text-sm font-medium rounded-lg hover:bg-red-100 dark:hover:bg-red-900/40 transition-colors"
            >
              <LogOut className="w-4 h-4" />
              退出登录
            </button>
          </div>
        </section>

        <div className="pb-8" />
      </div>
    </div>
  );
}
