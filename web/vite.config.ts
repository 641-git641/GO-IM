import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'path';

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    port: 5173,
    proxy: {
      '/login':    'http://localhost:8080',
      '/register': 'http://localhost:8080',
      '/ws':       { target: 'ws://localhost:8080', ws: true },
      '/online':   'http://localhost:8080',
      '/health':   'http://localhost:8080',
      '/upload':   'http://localhost:8080',
      '/file':     'http://localhost:8080',
      '/search':   'http://localhost:8080',
      '/unread':   'http://localhost:8080',
      '/group':    'http://localhost:8080',
    },
  },
});
