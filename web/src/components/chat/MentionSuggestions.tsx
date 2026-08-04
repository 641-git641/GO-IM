import { useEffect, useRef } from 'react';

interface MentionSuggestionsProps {
  query: string;
  members: string[];
  onSelect: (uid: string) => void;
  onClose: () => void;
}

export default function MentionSuggestions({
  query,
  members,
  onSelect,
  onClose,
}: MentionSuggestionsProps) {
  const ref = useRef<HTMLDivElement>(null);

  // 按查询词过滤成员(不区分大小写)
  const filtered = members
    .filter((m) => m.toLowerCase().includes(query.toLowerCase()))
    .slice(0, 8); // 最多 8 条建议

  // 点击外部时关闭
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        onClose();
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [onClose]);

  // 按 Escape 时关闭
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        onClose();
      }
    };
    document.addEventListener('keydown', handler);
    return () => document.removeEventListener('keydown', handler);
  }, [onClose]);

  if (filtered.length === 0) {
    return (
      <div className="bg-white rounded-lg shadow-lg border p-2 min-w-[160px]">
        <p className="text-xs text-gray-400 px-2 py-1">无匹配成员</p>
      </div>
    );
  }

  return (
    <div ref={ref} className="bg-white rounded-lg shadow-lg border py-1 min-w-[160px] max-h-52 overflow-y-auto">
      {filtered.map((member) => (
        <button
          key={member}
          onClick={() => onSelect(member)}
          className="w-full px-3 py-1.5 text-left text-sm text-gray-700 hover:bg-primary-50 hover:text-primary-700 transition-colors flex items-center gap-2"
        >
          <span className="w-6 h-6 rounded-full bg-primary-500 flex items-center justify-center text-white text-[10px] font-semibold flex-shrink-0">
            {member.slice(0, 2).toUpperCase()}
          </span>
          <span>@{member}</span>
        </button>
      ))}
    </div>
  );
}
