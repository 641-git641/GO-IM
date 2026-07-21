import { useEffect, useRef } from 'react';
import { wsManager } from '@/lib/ws';
import { useAuthStore } from '@/stores/authStore';
import { useWSStore } from '@/stores/wsStore';
import { useChatStore } from '@/stores/chatStore';
import { useContactStore } from '@/stores/contactStore';
import { useFriendStore } from '@/stores/friendStore';
import { Cmd, ChatType, MsgType } from '@/types';
import type { CmdType, IMMessage, ChatMessage, MsgTypeValue, ChatTypeValue } from '@/types';
import { isRecalled, isGroupNotification, tryParseJSON } from '@/lib/utils';

/**
 * Hook that manages WebSocket lifecycle and message dispatch.
 * Connects when logged in, disconnects on logout, and routes
 * all incoming messages to the appropriate stores.
 */
export function useWebSocket() {
  const { uid, isLoggedIn, token, logout } = useAuthStore();
  const setStatus = useWSStore((s) => s.setStatus);
  const {
    addMessage,
    confirmMessage,
    markRecalled,
    upsertConversation,
    prependMessages,
    setNoMoreHistory,
    setActivePeer,
    activePeer,
    setTyping,
    markMessagesRead,
  } = useChatStore();

  const initialized = useRef(false);

  useEffect(() => {
    if (!isLoggedIn || initialized.current) return;
    initialized.current = true;

    // Set up status callback
    wsManager.setStatusCallback(setStatus);

    // Subscribe to all messages
    const unsubAll = wsManager.subscribeAll((msg: IMMessage) => {
      handleMessage(msg);
    });

    // Connect
    wsManager.connect(token);

    return () => {
      unsubAll();
      wsManager.disconnect();
      initialized.current = false;
    };
  }, [isLoggedIn]);

  function handleMessage(msg: IMMessage) {
    switch (msg.cmd) {
      case Cmd.Chat: {
        const peerId = msg.chatType === ChatType.Group ? msg.to : msg.from;
        upsertConversation(peerId, peerId, msg.chatType as ChatTypeValue);

        // Check for group notification
        const notification = isGroupNotification(msg.content);
        if (notification) {
          // System notice — add as text message with notification type
          const chatMsg: ChatMessage = {
            ...msg,
            chatType: msg.chatType as ChatTypeValue,
            msgType: msg.msgType as MsgTypeValue,
            status: 'sent',
            recalled: false,
          };
          addMessage(peerId, chatMsg, uid);
          return;
        }

        const recalled = isRecalled(msg.content);
        if (recalled) {
          markRecalled(peerId, msg.msgId);
          return;
        }

        const chatMsg: ChatMessage = {
          ...msg,
          chatType: msg.chatType as ChatTypeValue,
          msgType: msg.msgType as MsgTypeValue,
          status: 'sent',
          recalled: false,
        };
        addMessage(peerId, chatMsg, uid);

        // Auto-send read receipt for the active conversation
        if (peerId === activePeer && msg.from !== uid) {
          wsManager.send({
            seq: msg.msgId,
            msgId: '0',
            cmd: Cmd.ReadReceipt,
            from: uid,
            to: msg.from,
            chatType: ChatType.Single,
            msgType: MsgType.Text,
            content: '',
            timestamp: String(Date.now()),
            needAck: false,
          });
        }
        break;
      }

      case Cmd.Ack: {
        // Find the conversation by the target
        // msg.to is our UID, msg.from is the server (empty or our UID)
        // We need to match by seq — search all conversations
        const { conversations } = useChatStore.getState();
        for (const [peerId, conv] of conversations) {
          const pending = conv.messages.find(
            (m) => m.seq === msg.seq && m.status === 'sending',
          );
          if (pending) {
            confirmMessage(peerId, msg.seq, msg.msgId, msg.timestamp);
            break;
          }
        }
        break;
      }

      case Cmd.LoginResp: {
        // Connection confirmed — clear the onopen timeout
        wsManager.clearLoginTimeout();
        setStatus('connected');

        // Request unread counts
        wsManager.send({
          seq: '0',
          msgId: '0',
          cmd: Cmd.UnreadCount,
          from: uid,
          to: '',
          chatType: ChatType.Single,
          msgType: MsgType.Text,
          content: '',
          timestamp: '0',
          needAck: false,
        });

        // Pull offline messages
        wsManager.send({
          seq: '0',
          msgId: '0',
          cmd: Cmd.Offline,
          from: uid,
          to: '',
          chatType: ChatType.Single,
          msgType: MsgType.Text,
          content: '',
          timestamp: '0',
          needAck: false,
        });
        break;
      }

      case Cmd.History: {
        // History completion signal — seq = number of messages sent
        // The actual history messages arrive as CmdChat before this
        break;
      }

      case Cmd.Kick: {
        wsManager.markKicked();
        logout();
        break;
      }

      case Cmd.UnreadCount: {
        try {
          const data = JSON.parse(msg.content);
          if (data.counts) {
            const store = useChatStore.getState();
            store.setUnreadCounts(data.counts as Record<string, number>);
          }
        } catch { /* ignore parse errors */ }
        break;
      }

      case Cmd.Recall: {
        // Recall notification from server
        try {
          const data = JSON.parse(msg.content);
          if (data.msg_id) {
            const peerId = msg.from; // The person who recalled
            markRecalled(peerId, String(data.msg_id));
          }
        } catch { /* ignore */ }
        break;
      }

      case Cmd.File:
      case Cmd.Search:
        // Handled by specific hooks or components
        break;

      // --- Forward notification ---
      case Cmd.Forward: {
        // A forwarded message arrives like a regular chat message
        const peerId = msg.chatType === ChatType.Group ? msg.to : msg.from;
        upsertConversation(peerId, peerId, msg.chatType as ChatTypeValue);
        try {
          const data = JSON.parse(msg.content);
          const chatMsg: ChatMessage = {
            ...msg,
            chatType: msg.chatType as ChatTypeValue,
            msgType: msg.msgType as MsgTypeValue,
            status: 'sent',
            recalled: false,
            // Show the inner text if it's a forwarded message
            content: data.text || msg.content,
          };
          addMessage(peerId, chatMsg, uid);
        } catch {
          const chatMsg: ChatMessage = {
            ...msg,
            chatType: msg.chatType as ChatTypeValue,
            msgType: msg.msgType as MsgTypeValue,
            status: 'sent',
            recalled: false,
          };
          addMessage(peerId, chatMsg, uid);
        }
        break;
      }

      // --- Edit notification ---
      case Cmd.Edit: {
        // Server notifies that a message was edited
        try {
          const data = JSON.parse(msg.content);
          if (data.edited && data.msg_id && data.new_text) {
            const peerId = msg.from; // The person who edited
            const conversations = useChatStore.getState().conversations;
            const conv = conversations.get(peerId);
            if (conv) {
              const updated = {
                ...conv,
                messages: conv.messages.map((m) => {
                  if (m.msgId === String(data.msg_id)) {
                    // Try to preserve reply_to structure if present
                    const parsed = tryParseJSON(m.content);
                    if (parsed && typeof parsed === 'object' && 'text' in parsed) {
                      return { ...m, content: JSON.stringify({ ...parsed, text: data.new_text, edited: true }) };
                    }
                    return { ...m, content: JSON.stringify({ text: data.new_text, edited: true }) };
                  }
                  return m;
                }),
              };
              useChatStore.setState((s) => {
                const conversations = new Map(s.conversations);
                conversations.set(peerId, updated);
                return { conversations };
              });
            }
          }
        } catch { /* ignore parse errors */ }
        break;
      }

      // --- Friend protocol handlers ---

      case Cmd.FriendRequest: {
        // Incoming friend request notification or response to our request
        try {
          const data = JSON.parse(msg.content);
          if (data.error) {
            // Our request failed — ignore silently
            break;
          }
          if (data.from_uid && data.from_uid !== uid) {
            // Incoming friend request
            const friendStore = useFriendStore.getState();
            friendStore.addPendingRequest({
              from_uid: data.from_uid,
              username: data.username || data.from_uid,
              created_at: Date.now(),
            });
            // Show as system notice
            const chatMsg: ChatMessage = {
              ...msg,
              chatType: ChatType.Single as ChatTypeValue,
              msgType: MsgType.Text as MsgTypeValue,
              status: 'sent',
              recalled: false,
              content: JSON.stringify({
                type: 'friend_request',
                from_uid: data.from_uid,
                username: data.username || data.from_uid,
              }),
            };
            addMessage(data.from_uid, chatMsg, uid);
          }
        } catch { /* ignore parse errors */ }
        break;
      }

      case Cmd.FriendResponse: {
        // Someone accepted/rejected our friend request
        try {
          const data = JSON.parse(msg.content);
          const friendStore = useFriendStore.getState();
          if (data.action === 'accept') {
            friendStore.addFriend({
              uid,
              friend_uid: data.from_uid || msg.from,
              status: 1,
              created_at: Date.now(),
            });
            // System notice
            const chatMsg: ChatMessage = {
              ...msg,
              chatType: ChatType.Single as ChatTypeValue,
              msgType: MsgType.Text as MsgTypeValue,
              status: 'sent',
              recalled: false,
              content: JSON.stringify({
                type: 'friend_accepted',
                uid: data.from_uid || msg.from,
              }),
            };
            addMessage(data.from_uid || msg.from, chatMsg, uid);
          } else if (data.action === 'reject') {
            // Rejected — do nothing special
          }
        } catch { /* ignore parse errors */ }
        break;
      }

      // --- Group protocol handlers (Phase 5) ---

      case Cmd.GroupCreate: {
        // Server response to group creation (or notification that someone created a group)
        try {
          const data = JSON.parse(msg.content);
          if (data.id && data.name) {
            const contactStore = useContactStore.getState();
            contactStore.addGroup({
              id: data.id,
              name: data.name,
              owner_uid: data.owner_uid || data.ownerUid,
              member_count: Array.isArray(data.members) ? data.members.length : 0,
              created_at: data.created_at || 0,
            });
            // Add as a conversation
            upsertConversation(data.id, data.name, ChatType.Group as ChatTypeValue);
            // System notice
            const chatMsg: ChatMessage = {
              ...msg,
              chatType: ChatType.Group as ChatTypeValue,
              msgType: MsgType.Text as MsgTypeValue,
              status: 'sent',
              recalled: false,
              content: JSON.stringify({
                type: 'group_created',
                group_id: data.id,
                name: data.name,
              }),
            };
            addMessage(data.id, chatMsg, uid);
          }
        } catch { /* ignore parse errors */ }
        break;
      }

      case Cmd.GroupJoin: {
        // Response: {"group_id":"g_123","uid":"bob","members":["alice","bob"]}
        try {
          const data = JSON.parse(msg.content);
          if (data.group_id) {
            // If we are the joiner, add this group to our contact store and conversations
            if (data.uid === uid) {
              const contactStore = useContactStore.getState();
              const existingGroup = contactStore.groups.find(g => g.id === data.group_id);
              if (!existingGroup) {
                // Fetch group info to get name
                wsManager.send({
                  seq: '0',
                  msgId: '0',
                  cmd: Cmd.GroupInfo,
                  from: uid,
                  to: data.group_id,
                  chatType: ChatType.Group,
                  msgType: MsgType.Text,
                  content: '',
                  timestamp: '0',
                  needAck: false,
                });
              }
            }
            // System notice for member_joined (if not already handled by CmdChat notification)
            if (data.uid && data.uid !== uid) {
              const notification: ChatMessage = {
                ...msg,
                chatType: ChatType.Group as ChatTypeValue,
                msgType: MsgType.Text as MsgTypeValue,
                status: 'sent',
                recalled: false,
                content: JSON.stringify({
                  type: 'member_joined',
                  group_id: data.group_id,
                  uid: data.uid,
                }),
              };
              addMessage(data.group_id, notification, uid);
            }
          }
        } catch { /* ignore parse errors */ }
        break;
      }

      case Cmd.GroupLeave: {
        // Response: {"group_id":"g_123","uid":"bob","deleted":false}
        try {
          const data = JSON.parse(msg.content);
          if (data.group_id) {
            if (data.uid === uid) {
              // We left the group — remove from contact store
              const contactStore = useContactStore.getState();
              contactStore.removeGroup(data.group_id);
            }
            // System notice for member_left
            if (data.uid && data.uid !== uid) {
              const notification: ChatMessage = {
                ...msg,
                chatType: ChatType.Group as ChatTypeValue,
                msgType: MsgType.Text as MsgTypeValue,
                status: 'sent',
                recalled: false,
                content: JSON.stringify({
                  type: 'member_left',
                  group_id: data.group_id,
                  uid: data.uid,
                }),
              };
              addMessage(data.group_id, notification, uid);
            }
          }
        } catch { /* ignore parse errors */ }
        break;
      }

      case Cmd.GroupInfo: {
        // Response: {"id":"g_123","name":"My Group","owner_uid":"alice","members":["alice","bob"],"created_at":123}
        try {
          const data = JSON.parse(msg.content);
          if (data.id && data.name) {
            const contactStore = useContactStore.getState();
            contactStore.addGroup({
              id: data.id,
              name: data.name,
              owner_uid: data.owner_uid || data.ownerUid,
              member_count: Array.isArray(data.members) ? data.members.length : 0,
              created_at: data.created_at || 0,
            });
            // Also store detailed info for GroupInfoPanel use
            contactStore.setGroupDetail({
              id: data.id,
              name: data.name,
              ownerUid: data.owner_uid || data.ownerUid,
              members: data.members || [],
              createdAt: data.created_at || 0,
            });
            // Ensure conversation exists
            upsertConversation(data.id, data.name, ChatType.Group as ChatTypeValue);
          }
        } catch { /* ignore parse errors */ }
        break;
      }

      case Cmd.GroupList: {
        // Response: {"uid":"alice","groups":[{"id":"g_1","name":"...","owner_uid":"...","member_count":2,"created_at":123}]}
        try {
          const data = JSON.parse(msg.content);
          if (data.groups && Array.isArray(data.groups)) {
            const contactStore = useContactStore.getState();
            contactStore.setGroups(data.groups);
            // Upsert conversations for all groups
            for (const g of data.groups) {
              upsertConversation(g.id, g.name, ChatType.Group as ChatTypeValue);
            }
          }
        } catch { /* ignore parse errors */ }
        break;
      }

      // --- Typing indicator ---

      case Cmd.Typing: {
        // Peer is typing in a conversation
        const peerId = msg.chatType === ChatType.Group ? msg.to : msg.from;
        setTyping(peerId, msg.from);
        break;
      }

      // --- Read receipt notification ---

      case Cmd.ReadReceipt: {
        // Server notifies us that someone read our messages
        // msg.From = who read, msg.To = us (the original sender)
        const peerId = msg.from; // the reader's UID is our conversation peer
        markMessagesRead(peerId, msg.from);
        break;
      }
    }
  }
}
