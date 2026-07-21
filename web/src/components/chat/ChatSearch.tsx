import { useEffect, useRef } from 'react';
import { Search, X, ChevronUp, ChevronDown } from 'lucide-react';

interface ChatSearchProps {
  query: string;
  onQueryChange: (query: string) => void;
  matchCount: number;
  currentMatch: number;
  onNext: () => void;
  onPrev: () => void;
  onClose: () => void;
}

export default function ChatSearch({
  query,
  onQueryChange,
  matchCount,
  currentMatch,
  onNext,
  onPrev,
  onClose,
}: ChatSearchProps) {
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      if (e.shiftKey) {
        onPrev();
      } else {
        onNext();
      }
    } else if (e.key === 'Escape') {
      onClose();
    }
  };

  return (
    <div className="flex items-center gap-2 px-3 py-2 bg-white dark:bg-gray-900 border-b border-gray-200 dark:border-gray-800">
      <Search className="w-4 h-4 text-gray-400 flex-shrink-0" />
      <input
        ref={inputRef}
        type="text"
        value={query}
        onChange={(e) => onQueryChange(e.target.value)}
        onKeyDown={handleKeyDown}
        placeholder="在会话中搜索..."
        className="flex-1 text-sm border-none outline-none bg-transparent text-gray-900 dark:text-gray-100 placeholder:text-gray-400 dark:placeholder:text-gray-500"
      />
      {query && (
        <>
          <span className="text-xs text-gray-400 flex-shrink-0">
            {matchCount > 0 ? `${currentMatch + 1}/${matchCount}` : '无匹配'}
          </span>
          <button
            onClick={onPrev}
            disabled={matchCount === 0}
            className="p-0.5 rounded hover:bg-gray-100 dark:hover:bg-gray-800 text-gray-500 dark:text-gray-400 disabled:opacity-30"
            title="上一个 (Shift+Enter)"
          >
            <ChevronUp className="w-4 h-4" />
          </button>
          <button
            onClick={onNext}
            disabled={matchCount === 0}
            className="p-0.5 rounded hover:bg-gray-100 dark:hover:bg-gray-800 text-gray-500 dark:text-gray-400 disabled:opacity-30"
            title="下一个 (Enter)"
          >
            <ChevronDown className="w-4 h-4" />
          </button>
        </>
      )}
      <button
        onClick={onClose}
        className="p-0.5 rounded hover:bg-gray-100 dark:hover:bg-gray-800 text-gray-400 dark:text-gray-500"
        title="关闭 (Esc)"
      >
        <X className="w-4 h-4" />
      </button>
    </div>
  );
}
