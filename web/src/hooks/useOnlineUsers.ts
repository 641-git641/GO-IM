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
      // Silently fail — online list is nice-to-have
    }
  }, [setOnlineUsers]);

  useEffect(() => {
    refresh();
    const interval = setInterval(refresh, 30_000); // Poll every 30s
    return () => clearInterval(interval);
  }, [refresh]);

  return { onlineUsers, refresh };
}
