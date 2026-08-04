import { useState, useRef, useCallback, type KeyboardEvent, type ChangeEvent } from 'react';
import { Send, Paperclip, X, Smile, CornerUpLeft } from 'lucide-react';
import type { ChatMessage } from '@/types';
import { Cmd, ChatType, MsgType } from '@/types';
import type { ChatTypeValue } from '@/types';
import { useAuthStore } from '@/stores/authStore';
import { wsManager } from '@/lib/ws';
import MentionSuggestions from './MentionSuggestions';
import EmojiPicker from './EmojiPicker';

interface ChatInputProps {
  onSend: (text: string) => void;
  onSendFile?: (file: File) => void;
  uploading?: boolean;
  uploadProgress?: number;
  disabled?: boolean;

  /** 目标会话对象/群组 ID(用于输入中指示器) */
  peerId: string;
  /** 聊天类型(用于输入中指示器) */
  chatType: number;

  /** 用于 @提及 自动补全的群组成员(为空/未定义 = 非群聊) */
  groupMembers?: string[];

  /** 当前正在回复某条消息 */
  replyTo?: ChatMessage | null;
  /** 取消当前回复 */
  onCancelReply?: () => void;
}

const MAX_FILE_SIZE = 10 * 1024 * 1024; // 10MB

export default function ChatInput({
  onSend,
  onSendFile,
  uploading = false,
  uploadProgress = 0,
  disabled = false,
  peerId,
  chatType,
  groupMembers,
  replyTo,
  onCancelReply,
}: ChatInputProps) {
  const { uid } = useAuthStore();
  const [text, setText] = useState('');
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  // @提及 状态
  const [mentionQuery, setMentionQuery] = useState<string | null>(null);
  const [mentionIdx, setMentionIdx] = useState(0); // 输入 @ 时光标在文本域中的位置

  // 表情选择器状态
  const [showEmoji, setShowEmoji] = useState(false);

  // 输入中指示器(防抖,输入时每 3 秒发送一次)
  const typingTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const lastTypingSentRef = useRef(0);

  const sendTyping = useCallback(() => {
    const now = Date.now();
    if (now - lastTypingSentRef.current < 3000) return; // 限流:每 3 秒一次
    lastTypingSentRef.current = now;
    wsManager.send({
      seq: '0',
      msgId: '0',
      cmd: Cmd.Typing,
      from: uid,
      to: peerId,
      chatType: chatType as ChatTypeValue,
      msgType: MsgType.Text,
      content: 'typing',
      timestamp: String(now),
      needAck: false,
    });
  }, [uid, peerId, chatType]);

  const handleTyping = useCallback(() => {
    if (typingTimerRef.current) return; // 已排定
    sendTyping();
    typingTimerRef.current = setTimeout(() => {
      typingTimerRef.current = null;
    }, 3000);
  }, [sendTyping]);

  // 卸载时清理输入定时器
  const cleanupTyping = () => {
    if (typingTimerRef.current) {
      clearTimeout(typingTimerRef.current);
      typingTimerRef.current = null;
    }
  };

  const handleSend = () => {
    if (!text.trim()) return;

    onSend(text.trim());
    setText('');
    setSelectedFile(null);
  };

  const handleKeyDown = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    // 若 @提及 下拉已打开,由 MentionSuggestions 处理导航
    if (mentionQuery !== null && (e.key === 'ArrowDown' || e.key === 'ArrowUp' || e.key === 'Enter')) {
      // Enter/Escape/方向键由 MentionSuggestions 处理
      if (e.key === 'Enter') {
        e.preventDefault();
        return; // 交由 MentionSuggestions 处理
      }
      return;
    }

    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  const handleChange = (e: ChangeEvent<HTMLTextAreaElement>) => {
    const value = e.target.value;
    setText(value);

    // 触发输入中指示器
    handleTyping();

    // 检测 @提及 触发
    if (groupMembers && groupMembers.length > 0) {
      const cursorPos = e.target.selectionStart || 0;
      const textBeforeCursor = value.slice(0, cursorPos);
      const atMatch = textBeforeCursor.match(/@(\S*)$/);

      if (atMatch) {
        setMentionQuery(atMatch[1]);
        setMentionIdx(cursorPos);
      } else {
        setMentionQuery(null);
      }
    }
  };

  /** 在光标位置插入表情 */
  const handleEmojiSelect = (emoji: string) => {
    const ta = textareaRef.current;
    if (!ta) {
      setText((prev) => prev + emoji);
      return;
    }
    const start = ta.selectionStart;
    const end = ta.selectionEnd;
    const newText = text.slice(0, start) + emoji + text.slice(end);
    setText(newText);
    setShowEmoji(false);
    // 插入表情后恢复光标位置
    setTimeout(() => {
      ta.focus();
      const pos = start + emoji.length;
      ta.setSelectionRange(pos, pos);
    }, 0);
  };

  const handleMentionSelect = (memberUid: string) => {
    if (mentionQuery === null) return;

    // 在光标位置将 @query 替换为 @uid
    const before = text.slice(0, mentionIdx - mentionQuery.length - 1); // 移除 "@query"
    const after = text.slice(mentionIdx);
    const newText = before + '@' + memberUid + ' ' + after;
    setText(newText);
    setMentionQuery(null);

    // 重新聚焦文本域
    textareaRef.current?.focus();
  };

  const closeMention = () => {
    setMentionQuery(null);
  };

  const handleFileSelect = (e: ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    if (file.size > MAX_FILE_SIZE) {
      alert('文件大小超过限制（最大 10MB）');
      return;
    }

    setSelectedFile(file);

    // 若提供了 onSendFile,则立即上传
    if (onSendFile) {
      onSendFile(file);
      setSelectedFile(null);
    }
  };

  // 截断回复内容用于展示
  const replyPreview = replyTo
    ? replyTo.content.length > 80
      ? replyTo.content.slice(0, 80) + '...'
      : replyTo.content
    : '';

  return (
    <div className="border-t border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900">
      {/* 回复栏 */}
      {replyTo && (
        <div className="flex items-center gap-2 px-4 py-2 bg-primary-50 dark:bg-primary-900/20 border-b border-primary-100 dark:border-primary-800">
          <CornerUpLeft className="w-4 h-4 text-primary-500 flex-shrink-0" />
          <div className="flex-1 min-w-0">
            <span className="text-xs font-medium text-primary-700">回复 {replyTo.from}:</span>
            <span className="text-xs text-primary-600 ml-1 truncate">{replyPreview}</span>
          </div>
          <button
            onClick={onCancelReply}
            className="p-1 rounded hover:bg-primary-100 transition-colors"
          >
            <X className="w-4 h-4 text-primary-500" />
          </button>
        </div>
      )}

      {/* 上传进度 */}
      {uploading && (
        <div className="px-4 pt-2">
          <div className="flex items-center gap-2 text-xs text-gray-500 mb-1">
            <span>上传中...</span>
            <span>{uploadProgress}%</span>
          </div>
          <div className="w-full h-1.5 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
            <div
              className="h-full bg-primary-500 rounded-full transition-all duration-300"
              style={{ width: `${uploadProgress}%` }}
            />
          </div>
        </div>
      )}

      {/* 已选文件预览 */}
      {selectedFile && !onSendFile && (
        <div className="px-4 pt-2 flex items-center gap-2">
          <div className="flex-1 p-2 bg-gray-50 dark:bg-gray-800 rounded-lg flex items-center gap-2">
            <span className="text-sm text-gray-600 truncate flex-1">{selectedFile.name}</span>
            <button
              onClick={() => setSelectedFile(null)}
              className="p-1 hover:bg-gray-200 rounded"
            >
              <X className="w-4 h-4 text-gray-400" />
            </button>
          </div>
        </div>
      )}

      <div className="px-4 py-3">
        <div className="flex items-end gap-2 relative">
          {/* @提及 建议 */}
          {mentionQuery !== null && groupMembers && (
            <div className="absolute bottom-full left-4 mb-1 z-10">
              <MentionSuggestions
                query={mentionQuery}
                members={groupMembers}
                onSelect={handleMentionSelect}
                onClose={closeMention}
              />
            </div>
          )}

          {/* 表情选择器 */}
          <div className="relative">
            <button
              onClick={() => setShowEmoji(!showEmoji)}
              disabled={disabled}
              className="p-2 rounded-lg text-gray-400 hover:text-primary-500 hover:bg-gray-100 transition-colors disabled:opacity-50"
              title="表情"
            >
              <Smile className="w-5 h-5" />
            </button>
            {showEmoji && (
              <div className="absolute bottom-full left-0 mb-1 z-20">
                <EmojiPicker
                  onSelect={handleEmojiSelect}
                  onClose={() => setShowEmoji(false)}
                />
              </div>
            )}
          </div>

          {/* 文件附件按钮 */}
          <button
            onClick={() => fileInputRef.current?.click()}
            disabled={disabled}
            className="p-2 rounded-lg text-gray-400 hover:text-primary-500 hover:bg-gray-100 transition-colors disabled:opacity-50"
            title="发送文件"
          >
            <Paperclip className="w-5 h-5" />
          </button>
          <input
            ref={fileInputRef}
            type="file"
            onChange={handleFileSelect}
            className="hidden"
            accept="image/*,video/*,audio/*,.pdf,.doc,.docx,.txt,.zip"
          />

          {/* 文本输入框 */}
          <textarea
            ref={textareaRef}
            value={text}
            onChange={handleChange}
            onKeyDown={handleKeyDown}
            placeholder="输入消息... (Enter 发送)"
            rows={1}
            disabled={disabled}
            className="flex-1 resize-none px-3 py-2 max-h-32 border border-gray-300 dark:border-gray-700 rounded-lg text-sm focus:ring-2 focus:ring-primary-500 focus:border-primary-500 outline-none transition-all disabled:opacity-50 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 placeholder:text-gray-400 dark:placeholder:text-gray-500"
          />

          {/* 发送按钮 */}
          <button
            onClick={handleSend}
            disabled={!text.trim()}
            className="p-2 rounded-lg bg-primary-500 text-white hover:bg-primary-600 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            title="发送"
          >
            <Send className="w-5 h-5" />
          </button>
        </div>
      </div>
    </div>
  );
}
