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
 * 管理 WebSocket 生命周期和消息分发的 Hook。
 * 登录时连接,登出时断开,并将所有收到的消息路由到对应的 store。
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
    if (!isLoggedIn || !token || initialized.current) return;
    initialized.current = true;

    // 设置状态回调
    wsManager.setStatusCallback(setStatus);

    // 订阅所有消息
    const unsubAll = wsManager.subscribeAll((msg: IMMessage) => {
      handleMessage(msg);
    });

    // 连接
    wsManager.connect(token);

    return () => {
      unsubAll();
      wsManager.disconnect();
      initialized.current = false;
    };
  }, [isLoggedIn, token]);

  function handleMessage(msg: IMMessage) {
    switch (msg.cmd) {
      case Cmd.Chat: {
        // 单聊:若消息是我们发出的(例如来自历史记录),peerId 为 msg.to。
        // 若是他人实时消息,msg.from 是发送者(即我们的聊天对象)。
        const isGroup = msg.chatType === ChatType.Group;
        const peerId = isGroup ? msg.to : (msg.from === uid ? msg.to : msg.from);
        upsertConversation(peerId, peerId, msg.chatType as ChatTypeValue);

        // 检查是否为群组通知
        const notification = isGroupNotification(msg.content);
        if (notification) {
          // 系统通知 —— 以带通知类型的文本消息形式添加
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

        // 为当前活跃会话自动发送已读回执
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
        // 按目标查找会话
        // msg.to 是我们的 UID,msg.from 是服务器(空或我们的 UID)
        // 需要按 seq 匹配 —— 搜索所有会话
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
        // 连接已确认 —— 清除 onopen 超时
        wsManager.clearLoginTimeout();
        setStatus('connected');

        // 请求未读数量
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

        // 拉取离线消息
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
        // 历史记录完成信号 —— seq = 已发送的消息数量
        // 实际的历史消息会在此之前以 CmdChat 到达
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
        } catch { /* 忽略解析错误 */ }
        break;
      }

      case Cmd.Recall: {
        // 服务器发来的撤回通知
        try {
          const data = JSON.parse(msg.content);
          if (data.msg_id) {
            const peerId = msg.from; // 撤回消息的人
            markRecalled(peerId, String(data.msg_id));
          }
        } catch { /* 忽略 */ }
        break;
      }

      case Cmd.File:
      case Cmd.Search:
        // 由特定的 hook 或组件处理
        break;

      // --- 转发通知 ---
      case Cmd.Forward: {
        // 转发的消息与普通聊天消息一样到达
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
            // 若是转发消息,展示内部文本
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

      // --- 编辑通知 ---
      case Cmd.Edit: {
        // 服务器通知某条消息已被编辑
        try {
          const data = JSON.parse(msg.content);
          if (data.edited && data.msg_id && data.new_text) {
            const peerId = msg.from; // 编辑消息的人
            const conversations = useChatStore.getState().conversations;
            const conv = conversations.get(peerId);
            if (conv) {
              const updated = {
                ...conv,
                messages: conv.messages.map((m) => {
                  if (m.msgId === String(data.msg_id)) {
                    // 若存在 reply_to 结构,尝试保留
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
        } catch { /* 忽略解析错误 */ }
        break;
      }

      // --- 好友协议处理 ---

      case Cmd.FriendRequest: {
        // 收到好友请求通知,或我们请求的响应
        try {
          const data = JSON.parse(msg.content);
          if (data.error) {
            // 我们的请求失败 —— 静默忽略
            break;
          }
          if (data.from_uid && data.from_uid !== uid) {
            // 收到好友请求
            const friendStore = useFriendStore.getState();
            friendStore.addPendingRequest({
              from_uid: data.from_uid,
              username: data.username || data.from_uid,
              created_at: Date.now(),
            });
            // 作为系统通知展示
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
        } catch { /* 忽略解析错误 */ }
        break;
      }

      case Cmd.FriendResponse: {
        // 有人接受/拒绝了我们的好友请求
        try {
          const data = JSON.parse(msg.content);
          const friendStore = useFriendStore.getState();
          if (data.action === 'accept') {
            friendStore.addFriend({
              uid: data.from_uid || msg.from,
              friend_uid: uid,
              status: 1,
              created_at: Date.now(),
            });
            // 系统通知
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
            // 被拒绝 —— 不做特殊处理
          }
        } catch { /* 忽略解析错误 */ }
        break;
      }

      // --- 群组协议处理 (Phase 5) ---

      case Cmd.GroupCreate: {
        // 服务器对创建群组的响应(或某人创建了群组的通知)
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
            // 作为会话添加
            upsertConversation(data.id, data.name, ChatType.Group as ChatTypeValue);
            // 系统通知
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
        } catch { /* 忽略解析错误 */ }
        break;
      }

      case Cmd.GroupJoin: {
        // 响应:{"group_id":"g_123","uid":"bob","members":["alice","bob"]}
        try {
          const data = JSON.parse(msg.content);
          if (data.group_id) {
            // 若我们是加入者,将群组加入联系人与会话
            if (data.uid === uid) {
              const contactStore = useContactStore.getState();
              const existingGroup = contactStore.groups.find(g => g.id === data.group_id);
              if (!existingGroup) {
                // 获取群组信息以得到名称
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
            // member_joined 的系统通知(若尚未由 CmdChat 通知处理)
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
        } catch { /* 忽略解析错误 */ }
        break;
      }

      case Cmd.GroupInviteMember: {
        // 响应:{"group_id":"g_123","target_uid":"bob","inviter_uid":"alice","members":[...]}
        try {
          const data = JSON.parse(msg.content);
          if (data.group_id) {
            // 刷新群组信息以获取最新的成员列表
            wsManager.send({
              seq: '0', msgId: '0', cmd: Cmd.GroupInfo, from: uid, to: data.group_id,
              chatType: ChatType.Group, msgType: MsgType.Text, content: '', timestamp: '0', needAck: false,
            });
            // 若我们是被邀请的用户,将群组加入联系人
            if (data.target_uid === uid) {
              const contactStore = useContactStore.getState();
              const existingGroup = contactStore.groups.find(g => g.id === data.group_id);
              if (!existingGroup) {
                // 请求群组列表以获取新群组
                wsManager.send({
                  seq: '0', msgId: '0', cmd: Cmd.GroupList, from: uid, to: '',
                  chatType: ChatType.Group, msgType: MsgType.Text, content: '', timestamp: '0', needAck: false,
                });
              }
            }
            // member_joined 的系统通知
            if (data.target_uid && data.target_uid !== uid) {
              const notification: ChatMessage = {
                ...msg,
                chatType: ChatType.Group as ChatTypeValue,
                msgType: MsgType.Text as MsgTypeValue,
                status: 'sent',
                recalled: false,
                content: JSON.stringify({
                  type: 'member_joined',
                  group_id: data.group_id,
                  uid: data.target_uid,
                }),
              };
              addMessage(data.group_id, notification, uid);
            }
          }
        } catch { /* 忽略解析错误 */ }
        break;
      }

      case Cmd.GroupLeave: {
        // 响应:{"group_id":"g_123","uid":"bob","deleted":false}
        try {
          const data = JSON.parse(msg.content);
          if (data.group_id) {
            if (data.uid === uid) {
              // 我们已退出群组 —— 从联系人中移除
              const contactStore = useContactStore.getState();
              contactStore.removeGroup(data.group_id);
            }
            // member_left 的系统通知
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
        } catch { /* 忽略解析错误 */ }
        break;
      }

      case Cmd.GroupInfo: {
        // 响应:{"id":"g_123","name":"My Group","owner_uid":"alice","members":["alice","bob"],"created_at":123}
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
            // 同时存储详细信息供 GroupInfoPanel 使用
            contactStore.setGroupDetail({
              id: data.id,
              name: data.name,
              owner_uid: data.owner_uid || data.ownerUid,
              members: data.members || [],
              created_at: data.created_at || 0,
            });
            // 确保会话存在
            upsertConversation(data.id, data.name, ChatType.Group as ChatTypeValue);
          }
        } catch { /* 忽略解析错误 */ }
        break;
      }

      case Cmd.GroupList: {
        // 响应:{"uid":"alice","groups":[{"id":"g_1","name":"...","owner_uid":"...","member_count":2,"created_at":123}]}
        try {
          const data = JSON.parse(msg.content);
          if (data.groups && Array.isArray(data.groups)) {
            const contactStore = useContactStore.getState();
            contactStore.setGroups(data.groups);
            // 为所有群组创建/更新会话
            for (const g of data.groups) {
              upsertConversation(g.id, g.name, ChatType.Group as ChatTypeValue);
            }
          }
        } catch { /* 忽略解析错误 */ }
        break;
      }

      // --- 输入中指示器 ---

      case Cmd.Typing: {
        // 对方正在某个会话中输入
        const peerId = msg.chatType === ChatType.Group ? msg.to : msg.from;
        setTyping(peerId, msg.from);
        break;
      }

      // --- 已读回执通知 ---

      case Cmd.ReadReceipt: {
        // 服务器通知我们有人已读我们的消息
        // msg.From = 谁已读,msg.To = 我们(原始发送者)
        const peerId = msg.from; // 已读者的 UID 即我们的会话对象
        markMessagesRead(peerId, msg.from);
        break;
      }
    }
  }
}
