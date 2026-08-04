import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { useAuthStore } from '@/stores/authStore';
import './index.css';
import App from './App.tsx';

// 在 React 渲染之前恢复会话。若等到 LoginPage 挂载再恢复,
// AuthGate 会看到 isLoggedIn=false 并在 restore() 运行前重定向。
useAuthStore.getState().restore();

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
