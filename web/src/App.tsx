import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { useAuthStore } from '@/stores/authStore';
import { useWebSocket } from '@/hooks/useWebSocket';
import AppLayout from '@/components/layout/AppLayout';
import LoginPage from '@/pages/LoginPage';
import ChatPage from '@/pages/ChatPage';
import ContactsPage from '@/pages/ContactsPage';
import SearchPage from '@/pages/SearchPage';
import SettingsPage from '@/pages/SettingsPage';
import UserProfilePage from '@/pages/UserProfilePage';
import AdminPage from '@/pages/AdminPage';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 30_000,
      retry: 1,
    },
  },
});

/** Persists WebSocket connection across route changes */
function WSProvider({ children }: { children: React.ReactNode }) {
  useWebSocket();
  return <>{children}</>;
}

/** Redirects to login if not authenticated */
function AuthGate({ children }: { children: React.ReactNode }) {
  const isLoggedIn = useAuthStore((s) => s.isLoggedIn);

  if (!isLoggedIn) {
    return <Navigate to="/login" replace />;
  }

  return <AppLayout>{children}</AppLayout>;
}

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <WSProvider>
          <Routes>
            <Route path="/login" element={<LoginPage />} />
            <Route
              path="/chat"
              element={
                <AuthGate>
                  <ChatPage />
                </AuthGate>
              }
            />
            <Route
              path="/chat/:peerId"
              element={
                <AuthGate>
                  <ChatPage />
                </AuthGate>
              }
            />
            <Route
              path="/contacts"
              element={
                <AuthGate>
                  <ContactsPage />
                </AuthGate>
              }
            />
            <Route
              path="/search"
              element={
                <AuthGate>
                  <SearchPage />
                </AuthGate>
              }
            />
            <Route
              path="/settings"
              element={
                <AuthGate>
                  <SettingsPage />
                </AuthGate>
              }
            />
            <Route
              path="/user/:uid"
              element={
                <AuthGate>
                  <UserProfilePage />
                </AuthGate>
              }
            />
            <Route
              path="/admin"
              element={
                <AuthGate>
                  <AdminPage />
                </AuthGate>
              }
            />
            <Route path="/" element={<Navigate to="/chat" replace />} />
            <Route path="*" element={<Navigate to="/chat" replace />} />
          </Routes>
        </WSProvider>
      </BrowserRouter>
    </QueryClientProvider>
  );
}
