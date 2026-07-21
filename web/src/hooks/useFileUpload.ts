import { useCallback, useState } from 'react';
import { uploadFile as apiUploadFile } from '@/lib/api';
import { useAuthStore } from '@/stores/authStore';
import type { UploadResponse } from '@/types';

export function useFileUpload() {
  const { uid, token } = useAuthStore();
  const [uploading, setUploading] = useState(false);
  const [progress, setProgress] = useState(0);
  const [error, setError] = useState<string | null>(null);

  const upload = useCallback(
    async (file: File): Promise<UploadResponse | null> => {
      if (!uid || !token) {
        setError('Not authenticated');
        return null;
      }

      setUploading(true);
      setProgress(0);
      setError(null);

      try {
        const result = await apiUploadFile(file, uid, token, (pct) => {
          setProgress(pct);
        });
        setProgress(100);
        return result;
      } catch (err) {
        const message = err instanceof Error ? err.message : 'Upload failed';
        setError(message);
        return null;
      } finally {
        setUploading(false);
      }
    },
    [uid, token],
  );

  const reset = useCallback(() => {
    setUploading(false);
    setProgress(0);
    setError(null);
  }, []);

  return { upload, uploading, progress, error, reset };
}
