import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { useAuthStore } from '@/stores/authStore';
import './index.css';
import App from './App.tsx';

// Restore session BEFORE React renders. If we wait until LoginPage mounts,
// AuthGate would see isLoggedIn=false and redirect before restore() runs.
useAuthStore.getState().restore();

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
