import { useEffect, useCallback } from 'react';
import { getOnlineUsers } from '@/lib/api';
import { useContactStore } from '@/stores/contactStore';

export function useOnlineUsers() {
  const { onlineUsers, setOnlineUsers } = useContactStore();

  const refresh = useCallback(async () => {
    try {
      const data = await getOnlineUsers();
      setOnlineUsers(data.users);
    } catch {
      // 静默失败 —— 在线列表只是锦上添花
    }
  }, [setOnlineUsers]);

  useEffect(() => {
    refresh();
    const interval = setInterval(refresh, 30_000); // 每 30 秒轮询一次
    return () => clearInterval(interval);
  }, [refresh]);

  return { onlineUsers, refresh };
}
