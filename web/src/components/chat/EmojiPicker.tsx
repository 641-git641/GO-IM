import { useEffect, useRef } from 'react';

// Common emoji groups for quick access
const EMOJIS = [
  // Smileys
  '😀', '😃', '😄', '😁', '😅', '😂', '🤣', '😊', '😇', '🙂', '😉', '😌', '😍', '🥰', '😘', '😗', '😋', '😛', '😜', '🤪',
  '😎', '🤩', '🥳', '😏', '😒', '😞', '😔', '😟', '😕', '🙁', '😣', '😖', '😫', '😩', '🥺', '😢', '😭', '😤', '😡', '🤬',
  // Gestures
  '👍', '👎', '👌', '✌️', '🤞', '🤟', '🤘', '🤙', '👋', '🤚', '🖐', '✋', '🖖', '👏', '🙌', '🫶', '🤝', '💪', '🦾', '🙏',
  // Hearts & love
  '❤️', '🧡', '💛', '💚', '💙', '💜', '🖤', '🤍', '🤎', '💔', '❣️', '💕', '💞', '💓', '💗', '💖', '💘', '💝', '💟', '♥️',
  // Objects
  '🎉', '🎊', '🎈', '🎂', '🍰', '🎁', '💡', '🔥', '⭐', '🌟', '✨', '💫', '💥', '💯', '💢', '💦', '💤', '🕶', '👓', '💍',
  '💎', '🎵', '🎶', '🎤', '🎧', '📱', '💻', '🖥', '⌨️', '🖱', '📷', '📹', '🎬', '📚', '📝', '✏️', '📌', '📍', '🗑', '🔒',
  // Nature
  '🌈', '☀️', '🌤', '⛅', '🌧', '⛈', '🌩', '❄️', '☃️', '⛄', '🌊', '🍀', '🌸', '🌺', '🌻', '🌹', '💐', '🌴', '🎄', '🌵',
  // Food
  '🍕', '🍔', '🍟', '🌭', '🍿', '🧁', '🍩', '🍪', '🎂', '🍰', '🍫', '🍬', '🍭', '☕', '🍵', '🍺', '🍻', '🥂', '🍷', '🥤',
  // Animals
  '🐶', '🐱', '🐭', '🐹', '🐰', '🦊', '🐻', '🐼', '🐨', '🐯', '🦁', '🐮', '🐷', '🐸', '🐵', '🐔', '🐧', '🐦', '🐤', '🦄',
  // Activities
  '⚽', '🏀', '🏈', '⚾', '🎾', '🏐', '🎱', '🏓', '🏸', '🥅', '🏒', '🏹', '🎣', '🥊', '🎯', '🪁', '🎮', '🎲', '♟', '🧩',
];

interface EmojiPickerProps {
  onSelect: (emoji: string) => void;
  onClose: () => void;
}

export default function EmojiPicker({ onSelect, onClose }: EmojiPickerProps) {
  const ref = useRef<HTMLDivElement>(null);

  // Close on click outside
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        onClose();
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [onClose]);

  // Close on escape
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    document.addEventListener('keydown', handler);
    return () => document.removeEventListener('keydown', handler);
  }, [onClose]);

  return (
    <div
      ref={ref}
      className="bg-white rounded-xl shadow-xl border border-gray-200 p-3 w-[320px]"
    >
      <div className="grid grid-cols-10 gap-1 max-h-[240px] overflow-y-auto">
        {EMOJIS.map((emoji, i) => (
          <button
            key={i}
            onClick={() => onSelect(emoji)}
            className="w-7 h-7 flex items-center justify-center text-lg rounded hover:bg-gray-100 active:bg-gray-200 transition-colors"
            title={emoji}
          >
            {emoji}
          </button>
        ))}
      </div>
    </div>
  );
}
