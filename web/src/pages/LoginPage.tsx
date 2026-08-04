import { useState, useEffect, type FormEvent } from 'react';
import { useNavigate } from 'react-router-dom';
import { login, register } from '@/lib/api';
import { useAuthStore } from '@/stores/authStore';
import { MessageCircle } from 'lucide-react';

type AuthMode = 'login' | 'register';

export default function LoginPage() {
  const [mode, setMode] = useState<AuthMode>('login');
  const [uid, setUid] = useState('');
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const authLogin = useAuthStore((s) => s.login);
  const isLoggedIn = useAuthStore((s) => s.isLoggedIn);
  const navigate = useNavigate();

  // 挂载时恢复已有会话
  useEffect(() => {
    useAuthStore.getState().restore();
  }, []);

  if (isLoggedIn) {
    navigate('/chat', { replace: true });
    return null;
  }

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setError('');

    if (!uid.trim()) {
      setError('请输入用户 ID');
      return;
    }

    if (mode === 'register') {
      if (!password || password.length < 6) {
        setError('密码至少需要 6 个字符');
        return;
      }
      if (password !== confirmPassword) {
        setError('两次输入的密码不一致');
        return;
      }
      if (!username.trim()) {
        setError('请输入显示名称');
        return;
      }
    }

    setLoading(true);

    try {
      let res;
      if (mode === 'register') {
        res = await register(uid.trim(), username.trim() || uid.trim(), password);
      } else {
        res = await login(uid.trim(), username.trim() || undefined, password || undefined);
      }
      authLogin(res.uid, res.username, res.token);
      navigate('/chat', { replace: true });
    } catch (err) {
      setError(err instanceof Error ? err.message : '操作失败');
    } finally {
      setLoading(false);
    }
  };

  const switchMode = (newMode: AuthMode) => {
    setMode(newMode);
    setError('');
    setPassword('');
    setConfirmPassword('');
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-primary-50 dark:from-gray-950 to-blue-100 dark:to-gray-900">
      <div className="w-full max-w-md mx-4">
        <div className="bg-white dark:bg-gray-900 rounded-2xl shadow-lg overflow-hidden">
          {/* 标签切换 */}
          <div className="flex border-b border-gray-100 dark:border-gray-800">
            <button
              onClick={() => switchMode('login')}
              className={`flex-1 py-4 text-sm font-semibold transition-colors ${
                mode === 'login'
                  ? 'text-primary-600 dark:text-primary-400 border-b-2 border-primary-600 bg-primary-50/50 dark:bg-primary-900/20'
                  : 'text-gray-400 dark:text-gray-500 hover:text-gray-600 dark:hover:text-gray-300'
              }`}
            >
              登录
            </button>
            <button
              onClick={() => switchMode('register')}
              className={`flex-1 py-4 text-sm font-semibold transition-colors ${
                mode === 'register'
                  ? 'text-primary-600 dark:text-primary-400 border-b-2 border-primary-600 bg-primary-50/50 dark:bg-primary-900/20'
                  : 'text-gray-400 dark:text-gray-500 hover:text-gray-600 dark:hover:text-gray-300'
              }`}
            >
              注册
            </button>
          </div>

          <div className="p-8 space-y-6">
            {/* 标志 */}
            <div className="text-center space-y-2">
              <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-primary-100">
                <MessageCircle className="w-8 h-8 text-primary-600" />
              </div>
              <h1 className="text-2xl font-bold text-gray-900 dark:text-gray-100">IM 即时通讯</h1>
              <p className="text-sm text-gray-500 dark:text-gray-400">
                {mode === 'login' ? '欢迎回来，请登录您的账户' : '创建新账户开始聊天'}
              </p>
            </div>

            <form onSubmit={handleSubmit} className="space-y-4">
              <div>
                <label htmlFor="uid" className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  用户 ID
                </label>
                <input
                  id="uid"
                  type="text"
                  value={uid}
                  onChange={(e) => setUid(e.target.value)}
                  placeholder="例如: alice"
                  autoFocus
                  className="w-full px-4 py-3 rounded-lg border border-gray-300 dark:border-gray-700 focus:ring-2 focus:ring-primary-500 focus:border-primary-500 outline-none transition-all bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100"
                />
              </div>

              {mode === 'register' && (
                <div>
                  <label htmlFor="username" className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    显示名称
                  </label>
                  <input
                    id="username"
                    type="text"
                    value={username}
                    onChange={(e) => setUsername(e.target.value)}
                    placeholder="例如: Alice"
                    className="w-full px-4 py-3 rounded-lg border border-gray-300 dark:border-gray-700 focus:ring-2 focus:ring-primary-500 focus:border-primary-500 outline-none transition-all bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100"
                  />
                </div>
              )}

              {mode === 'login' && (
                <div>
                  <label htmlFor="username-login" className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    显示名称 <span className="text-gray-400">(可选)</span>
                  </label>
                  <input
                    id="username-login"
                    type="text"
                    value={username}
                    onChange={(e) => setUsername(e.target.value)}
                    placeholder="例如: Alice"
                    className="w-full px-4 py-3 rounded-lg border border-gray-300 dark:border-gray-700 focus:ring-2 focus:ring-primary-500 focus:border-primary-500 outline-none transition-all bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100"
                  />
                </div>
              )}

              <div>
                <label htmlFor="password" className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  密码
                  {mode === 'login' && <span className="text-gray-400 ml-1">(开发模式下可选)</span>}
                </label>
                <input
                  id="password"
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  placeholder={mode === 'register' ? '输入密码（至少 6 位）' : '输入密码（可选）'}
                  className="w-full px-4 py-3 rounded-lg border border-gray-300 dark:border-gray-700 focus:ring-2 focus:ring-primary-500 focus:border-primary-500 outline-none transition-all bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100"
                />
              </div>

              {mode === 'register' && (
                <div>
                  <label htmlFor="confirmPassword" className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    确认密码
                  </label>
                  <input
                    id="confirmPassword"
                    type="password"
                    value={confirmPassword}
                    onChange={(e) => setConfirmPassword(e.target.value)}
                    placeholder="再次输入密码"
                    className="w-full px-4 py-3 rounded-lg border border-gray-300 dark:border-gray-700 focus:ring-2 focus:ring-primary-500 focus:border-primary-500 outline-none transition-all bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100"
                  />
                </div>
              )}

              {error && (
                <div className="p-3 rounded-lg bg-red-50 dark:bg-red-900/20 text-red-600 dark:text-red-400 text-sm">{error}</div>
              )}

              <button
                type="submit"
                disabled={loading}
                className="w-full py-3 px-4 bg-primary-600 hover:bg-primary-700 text-white font-medium rounded-lg transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {loading ? '处理中...' : mode === 'login' ? '登 录' : '注 册'}
              </button>
            </form>

            <p className="text-center text-xs text-gray-400 dark:text-gray-500">
              {mode === 'login'
                ? '还没有账户？点击上方"注册"创建新账户'
                : '已有账户？点击上方"登录"'}
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
