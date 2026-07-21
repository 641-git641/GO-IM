import { useState, useCallback } from 'react';
import { searchMessages } from '@/lib/api';
import { useAuthStore } from '@/stores/authStore';
import type { SearchResponse, SearchParams } from '@/types';

export function useSearch() {
  const { uid, token } = useAuthStore();
  const [results, setResults] = useState<SearchResponse | null>(null);
  const [searching, setSearching] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const search = useCallback(
    async (params: SearchParams) => {
      if (!uid || !token) return;

      setSearching(true);
      setError(null);

      try {
        const data = await searchMessages(params);
        setResults(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : '搜索失败');
        setResults(null);
      } finally {
        setSearching(false);
      }
    },
    [uid, token],
  );

  const loadMore = useCallback(
    async (params: SearchParams) => {
      if (!results?.next_cursor || !uid || !token) return;

      const data = await searchMessages({
        ...params,
        cursor: results.next_cursor,
      });

      setResults({
        ...data,
        messages: [...results.messages, ...data.messages],
      });
    },
    [uid, token, results],
  );

  return { results, searching, error, search, loadMore };
}
