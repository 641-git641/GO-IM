import { getAvatarLetters } from '@/lib/utils';

interface MemberListProps {
  members: string[];
  ownerUid: string;
}

export default function MemberList({ members, ownerUid }: MemberListProps) {
  return (
    <div className="space-y-1">
      {members.map((member) => (
        <div
          key={member}
          className="flex items-center gap-2 px-2 py-1.5 rounded-lg hover:bg-gray-50 transition-colors"
        >
          <div className="w-8 h-8 rounded-full bg-primary-500 flex items-center justify-center text-white text-xs font-semibold flex-shrink-0">
            {getAvatarLetters(member)}
          </div>
          <span className="text-sm text-gray-900 flex-1">{member}</span>
          {member === ownerUid && (
            <span className="text-[10px] px-1.5 py-0.5 bg-primary-100 text-primary-700 rounded-full">
              群主
            </span>
          )}
        </div>
      ))}
    </div>
  );
}
