import { useState, useRef, useEffect } from 'react';
import type { ReactNode } from 'react';
import { MoreVertical, Trash2, Reply, Edit3, Forward, X, Check } from 'lucide-react';
import type { ChatMessage, ReplyTo } from '@/types';
import { MsgType } from '@/types';
import { formatTime, tryParseJSON, getAvatarLetters, getFileURL } from '@/lib/utils';
import { useAuthStore } from '@/stores/authStore';
import { RECALL_WINDOW_MS, EDIT_WINDOW_MS } from '@/lib/constants';
import FilePreview from './FilePreview';

interface MessageBubbleProps {
  message: ChatMessage;
  isMine: boolean;
  showAvatar: boolean;
  onRecall?: (msgId: string) => void;
  onReply?: (message: ChatMessage) => void;
  onForward?: (message: ChatMessage) => void;
  onEdit?: (msgId: string, newText: string) => void;
  onDelete?: (msgId: string) => void;
  /** Search term for in-conversation highlighting */
  highlight?: string;
  /** Whether this message is the current search match (for distinct styling) */
  isCurrentMatch?: boolean;
}

export default function MessageBubble({ message, isMine, showAvatar, onRecall, onReply, onForward, onEdit, onDelete, highlight, isCurrentMatch }: MessageBubbleProps) {
  const { uid, token } = useAuthStore();
  const [showMenu, setShowMenu] = useState(false);
  const [editing, setEditing] = useState(false);
  const [editText, setEditText] = useState('');
  const editInputRef = useRef<HTMLTextAreaElement>(null);

  const { content, msgType, recalled, status, timestamp } = message;
  const msgId = message.msgId;
  const time = formatTime(timestamp);

  // Check if message can be recalled
  const canRecall =
    isMine &&
    !recalled &&
    onRecall &&
    msgId !== '0' &&
    Date.now() - Number(timestamp) < RECALL_WINDOW_MS;

  // Check if message can be replied to
  const canReply = !recalled && onReply && msgId !== '0';

  // Check if message can be forwarded
  const canForward = !recalled && onForward && msgId !== '0';

  // Check if message can be edited (own text messages within time window)
  const canEdit =
    isMine &&
    !recalled &&
    onEdit &&
    msgId !== '0' &&
    msgType === MsgType.Text &&
    Date.now() - Number(timestamp) < EDIT_WINDOW_MS;

  // Check if message can be deleted locally
  const canDelete = isMine && !recalled && onDelete && msgId !== '0';

  const avatarLetter = getAvatarLetters(message.from);

  // Focus edit input when entering edit mode
  useEffect(() => {
    if (editing && editInputRef.current) {
      editInputRef.current.focus();
      editInputRef.current.setSelectionRange(editInputRef.current.value.length, editInputRef.current.value.length);
    }
  }, [editing]);

  // Handle save/cancel for inline edit
  const handleSaveEdit = () => {
    if (editText.trim() && editText !== displayText) {
      onEdit?.(msgId, editText.trim());
    }
    setEditing(false);
  };

  const handleCancelEdit = () => {
    setEditing(false);
  };

  const handleStartEdit = () => {
    setEditText(displayText);
    setEditing(true);
    setShowMenu(false);
  };

  // Parse reply metadata from content
  const parsedContent = tryParseJSON<{ text?: string; reply_to?: ReplyTo; edited?: boolean; type?: string; username?: string; from_uid?: string; uid?: string; name?: string }>(content);
  const replyTo: ReplyTo | null = parsedContent?.reply_to || null;

  // Compute display text: handle JSON system messages gracefully
  let displayText: string;
  if (parsedContent?.text) {
    displayText = parsedContent.text;
  } else if (parsedContent?.type === 'friend_request') {
    displayText = `${parsedContent.username || parsedContent.from_uid || '用户'} 请求添加好友`;
  } else if (parsedContent?.type === 'friend_accepted') {
    displayText = `${parsedContent.uid || '用户'} 已同意好友请求`;
  } else if (parsedContent?.type === 'group_created') {
    displayText = `群组 "${parsedContent.name || '未命名'}" 已创建`;
  } else {
    displayText = content;
  }

  // Check if current user is mentioned in the message
  const isMentioned =
    !recalled &&
    !isMine &&
    uid &&
    (content.includes('@' + uid) || displayText.includes('@' + uid));

  // Render text with highlighted mentions and search highlights
  const renderText = (text: string): ReactNode => {
    if (!uid && !highlight) return <span className="whitespace-pre-wrap break-words">{text}</span>;

    // If we have a search highlight, split by the search term (case-insensitive)
    if (highlight && highlight.trim()) {
      const escaped = highlight.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const regex = new RegExp(`(${escaped})`, 'gi');
      const parts = text.split(regex);
      return (
        <span className="whitespace-pre-wrap break-words">
          {parts.map((part, i) => {
            if (part.toLowerCase() === highlight.toLowerCase()) {
              return (
                <mark
                  key={i}
                  className={`rounded px-0.5 ${
                    isCurrentMatch
                      ? 'bg-orange-400 text-white'
                      : 'bg-yellow-200 text-gray-900'
                  }`}
                >
                  {part}
                </mark>
              );
            }
            // Also apply mention styling within parts
            if (part.startsWith('@') && part.slice(1) === uid) {
              return (
                <span key={i} className="bg-primary-200 text-primary-800 px-0.5 rounded font-medium">
                  {part}
                </span>
              );
            }
            if (part.startsWith('@')) {
              return (
                <span key={i} className="text-primary-600 font-medium">
                  {part}
                </span>
              );
            }
            return <span key={i}>{part}</span>;
          })}
        </span>
      );
    }

    // Mention highlighting only (no search)
    const parts = text.split(/(@\S+)/g);
    return (
      <span className="whitespace-pre-wrap break-words">
        {parts.map((part, i) => {
          if (part.startsWith('@') && part.slice(1) === uid) {
            return (
              <span
                key={i}
                className="bg-primary-200 text-primary-800 px-0.5 rounded font-medium"
              >
                {part}
              </span>
            );
          }
          if (part.startsWith('@')) {
            return (
              <span key={i} className="text-primary-600 font-medium">
                {part}
              </span>
            );
          }
          return <span key={i}>{part}</span>;
        })}
      </span>
    );
  };

  // Render content based on type
  const renderContent = () => {
    if (recalled) {
      return (
        <span className="text-gray-400 dark:text-gray-500 italic text-sm">
          消息已撤回
        </span>
      );
    }

    switch (msgType) {
      case MsgType.Text:
        // Show edit input when in edit mode
        if (editing) {
          return (
            <div className="min-w-[200px]">
              <textarea
                ref={editInputRef}
                value={editText}
                onChange={(e) => setEditText(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault();
                    handleSaveEdit();
                  } else if (e.key === 'Escape') {
                    handleCancelEdit();
                  }
                }}
                className="w-full px-2 py-1.5 text-sm border border-primary-300 rounded-lg focus:outline-none focus:border-primary-400 resize-none bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100"
                rows={2}
              />
              <div className="flex items-center justify-end gap-1 mt-1">
                <span className="text-[10px] text-gray-400 dark:text-gray-500">Esc 取消 · Enter 保存</span>
                <button
                  onClick={handleSaveEdit}
                  className="p-1 rounded hover:bg-primary-100 text-primary-600"
                >
                  <Check className="w-3.5 h-3.5" />
                </button>
                <button
                  onClick={handleCancelEdit}
                  className="p-1 rounded hover:bg-gray-100 text-gray-400"
                >
                  <X className="w-3.5 h-3.5" />
                </button>
              </div>
            </div>
          );
        }
        return (
          <div>
            {/* Reply quote */}
            {replyTo && (
              <div className={`mb-1.5 px-2 py-1 rounded text-xs border-l-2 ${
                isMine
                  ? 'bg-primary-400/20 border-primary-300 text-primary-100'
                  : 'bg-gray-50 dark:bg-gray-700 border-primary-400 text-gray-500 dark:text-gray-400'
              }`}>
                <span className="font-medium">{replyTo.from}</span>: {replyTo.content.length > 60 ? replyTo.content.slice(0, 60) + '...' : replyTo.content}
              </div>
            )}
            {renderText(displayText)}
            {/* Edited indicator */}
            {parsedContent?.edited && (
              <span className="text-[10px] text-gray-400 dark:text-gray-500 ml-1">(已编辑)</span>
            )}
          </div>
        );

      case MsgType.Image:
      case MsgType.Video:
      case MsgType.Voice:
      case MsgType.File: {
        const metadata = tryParseJSON<{
          file_id: string;
          name: string;
          size: number;
          mime: string;
          width?: number;
          height?: number;
          text?: string;
        }>(content);

        if (!metadata?.file_id) {
          return <span className="text-gray-500">[文件数据异常]</span>;
        }

        return (
          <FilePreview
            fileId={metadata.file_id}
            fileName={metadata.name}
            mime={metadata.mime}
            size={metadata.size}
            width={metadata.width}
            height={metadata.height}
            caption={metadata.text}
            uid={uid}
            token={token}
          />
        );
      }

      default:
        return renderText(content);
    }
  };

  return (
    <div
      data-msg-id={msgId || message.seq}
      className={`message-enter flex gap-2 ${isMine ? 'flex-row-reverse' : 'flex-row'} ${showAvatar ? 'mt-3' : 'mt-0.5'} ${isCurrentMatch ? 'ring-2 ring-orange-400 rounded-lg p-1 -m-1' : ''}`}
    >
      {/* Avatar */}
      {showAvatar ? (
        <div className="w-8 h-8 rounded-full bg-primary-500 flex items-center justify-center text-white text-xs font-semibold flex-shrink-0">
          {avatarLetter}
        </div>
      ) : (
        <div className="w-8 flex-shrink-0" />
      )}

      {/* Bubble */}
      <div className={`group relative max-w-[75%] ${isMine ? 'items-end' : 'items-start'}`}>
        {/* Sender name */}
        {showAvatar && !isMine && (
          <p className="text-xs text-gray-500 dark:text-gray-400 mb-1 ml-1">{message.from}</p>
        )}

        <div className="flex items-end gap-1">
          {/* Context menu trigger */}
          {(canRecall || canReply || canForward || canEdit || canDelete) && (
            <div className="relative opacity-0 group-hover:opacity-100 transition-opacity">
              <button
                onClick={() => setShowMenu(!showMenu)}
                className="p-1 rounded hover:bg-gray-200 dark:hover:bg-gray-700"
              >
                <MoreVertical className="w-3 h-3 text-gray-400" />
              </button>
              {showMenu && (
                <div
                  className="absolute bottom-full left-0 mb-1 bg-white dark:bg-gray-800 rounded-lg shadow-lg border dark:border-gray-700 py-1 z-10"
                  onMouseLeave={() => setShowMenu(false)}
                >
                  {canReply && (
                    <button
                      onClick={() => {
                        onReply?.(message);
                        setShowMenu(false);
                      }}
                      className="flex items-center gap-2 px-3 py-1.5 text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 w-full whitespace-nowrap"
                    >
                      <Reply className="w-3.5 h-3.5" />
                      回复
                    </button>
                  )}
                  {canForward && (
                    <button
                      onClick={() => {
                        onForward?.(message);
                        setShowMenu(false);
                      }}
                      className="flex items-center gap-2 px-3 py-1.5 text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 w-full whitespace-nowrap"
                    >
                      <Forward className="w-3.5 h-3.5" />
                      转发
                    </button>
                  )}
                  {canEdit && (
                    <button
                      onClick={handleStartEdit}
                      className="flex items-center gap-2 px-3 py-1.5 text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 w-full whitespace-nowrap"
                    >
                      <Edit3 className="w-3.5 h-3.5" />
                      编辑
                    </button>
                  )}
                  {canRecall && (
                    <button
                      onClick={() => {
                        onRecall?.(msgId);
                        setShowMenu(false);
                      }}
                      className="flex items-center gap-2 px-3 py-1.5 text-sm text-red-600 dark:text-red-400 hover:bg-red-50 dark:hover:bg-red-900/20 w-full whitespace-nowrap"
                    >
                      <Trash2 className="w-3.5 h-3.5" />
                      撤回消息
                    </button>
                  )}
                  {canDelete && (
                    <button
                      onClick={() => {
                        onDelete?.(msgId);
                        setShowMenu(false);
                      }}
                      className="flex items-center gap-2 px-3 py-1.5 text-sm text-red-500 dark:text-red-400 hover:bg-red-50 dark:hover:bg-red-900/20 w-full whitespace-nowrap"
                    >
                      <X className="w-3.5 h-3.5" />
                      删除
                    </button>
                  )}
                </div>
              )}
            </div>
          )}

          {/* Bubble body */}
          <div
            className={`px-3 py-2 rounded-2xl text-sm leading-relaxed ${
              recalled
                ? 'bg-gray-100 dark:bg-gray-800'
                : isMentioned
                  ? 'bg-blue-100 dark:bg-blue-900/30 border border-blue-200 dark:border-blue-800 rounded-bl-md shadow-sm'
                  : isMine
                    ? 'bg-primary-500 text-white rounded-br-md'
                    : 'bg-white dark:bg-gray-800 border border-gray-100 dark:border-gray-700 rounded-bl-md shadow-sm'
            }`}
          >
            {renderContent()}
          </div>
        </div>

        {/* Time and status */}
        <div className={`flex items-center gap-1 mt-0.5 ${isMine ? 'justify-end mr-1' : 'ml-1'}`}>
          <span className="text-[10px] text-gray-400 dark:text-gray-500">{time}</span>
          {isMine && status === 'sending' && (
            <span className="text-[10px] text-gray-400 dark:text-gray-500">发送中...</span>
          )}
          {isMine && status === 'sent' && !recalled && (
            <svg className="w-3 h-3 text-gray-400" viewBox="0 0 16 16" fill="currentColor" aria-label="已发送">
              <path d="M4 8l3 3 5-6" stroke="currentColor" strokeWidth="1.5" fill="none" strokeLinecap="round" strokeLinejoin="round"/>
            </svg>
          )}
          {isMine && status === 'read' && (
            <span className="text-[10px] text-blue-500">已读</span>
          )}
          {isMine && status === 'failed' && (
            <span className="text-[10px] text-red-500">失败</span>
          )}
        </div>
      </div>
    </div>
  );
}
