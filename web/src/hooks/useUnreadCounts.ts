import { useEffect, useCallback } from 'react';
import { getUnreadCounts } from '@/lib/api';
import { useAuthStore } from '@/stores/authStore';
import { useChatStore } from '@/stores/chatStore';

export function useUnreadCounts() {
  const { uid } = useAuthStore();
  const conversations = useChatStore((s) => s.conversations);

  const refresh = useCallback(async () => {
    if (!uid) return;
    try {
      const data = await getUnreadCounts();
      if (data.counts) {
        const store = useChatStore.getState();
        store.setUnreadCounts(data.counts);
      }
    } catch {
      // Silently fail
    }
  }, [uid]);

  useEffect(() => {
    refresh();
    const interval = setInterval(refresh, 60_000);
    return () => clearInterval(interval);
  }, [refresh]);

  // Compute total
  let totalUnread = 0;
  for (const conv of conversations.values()) {
    totalUnread += conv.unread;
  }

  return { totalUnread, refresh };
}
