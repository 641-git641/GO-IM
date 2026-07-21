import { useState } from 'react';
import { X, UserPlus, UserMinus, Check, AlertCircle, Edit3, UserCheck } from 'lucide-react';
import { useAuthStore } from '@/stores/authStore';
import { wsManager } from '@/lib/ws';
import { kickGroupMember, renameGroup, transferGroup } from '@/lib/api';
import { Cmd, ChatType, MsgType } from '@/types';
import type { GroupInfo } from '@/types';

interface GroupInfoPanelProps {
  group: GroupInfo;
  open: boolean;
  onClose: () => void;
}

export default function GroupInfoPanel({ group, open, onClose }: GroupInfoPanelProps) {
  const { uid } = useAuthStore();
  const [showAddMember, setShowAddMember] = useState(false);
  const [newMemberUid, setNewMemberUid] = useState('');
  const [addStatus, setAddStatus] = useState<{ type: 'success' | 'error'; text: string } | null>(null);
  const [adding, setAdding] = useState(false);
  const [kicking, setKicking] = useState<string | null>(null); // uid being kicked
  const [renaming, setRenaming] = useState(false);
  const [newGroupName, setNewGroupName] = useState(group.name);
  const [renamingLoading, setRenamingLoading] = useState(false);
  const [transferring, setTransferring] = useState<string | null>(null); // uid being transferred to

  if (!open) return null;

  const isOwner = uid === group.ownerUid;

  const handleAddMember = async () => {
    if (!newMemberUid.trim()) return;
    setAddStatus(null);
    setAdding(true);

    try {
      // Use HTTP API to add member
      const res = await fetch('/group/join', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({ uid: newMemberUid.trim(), group_id: group.id }),
      });

      if (res.ok) {
        setAddStatus({ type: 'success', text: `${newMemberUid} 已加入群组` });
        setNewMemberUid('');
        // Refresh group info via WebSocket
        wsManager.send({
          seq: '0',
          msgId: '0',
          cmd: Cmd.GroupInfo,
          from: uid,
          to: group.id,
          chatType: ChatType.Group,
          msgType: MsgType.Text,
          content: '',
          timestamp: '0',
          needAck: false,
        });
      } else {
        const text = await res.text();
        setAddStatus({ type: 'error', text: text || '添加失败' });
      }
    } catch {
      setAddStatus({ type: 'error', text: '网络错误' });
    } finally {
      setAdding(false);
    }
  };

  const handleKick = async (targetUid: string) => {
    if (!confirm(`确定要将 ${targetUid} 移出群组吗？`)) return;
    setKicking(targetUid);

    try {
      await kickGroupMember(uid, group.id, targetUid);
      // Refresh group info
      wsManager.send({
        seq: '0',
        msgId: '0',
        cmd: Cmd.GroupInfo,
        from: uid,
        to: group.id,
        chatType: ChatType.Group,
        msgType: MsgType.Text,
        content: '',
        timestamp: '0',
        needAck: false,
      });
    } catch (err) {
      const msg = err instanceof Error ? err.message : '移出失败';
      alert(msg);
    } finally {
      setKicking(null);
    }
  };

  const handleRename = async () => {
    if (!newGroupName.trim() || newGroupName === group.name) {
      setRenaming(false);
      setNewGroupName(group.name);
      return;
    }
    setRenamingLoading(true);
    try {
      await renameGroup(uid, group.id, newGroupName.trim());
      // Refresh group info
      wsManager.send({
        seq: '0', msgId: '0', cmd: Cmd.GroupInfo, from: uid, to: group.id,
        chatType: ChatType.Group, msgType: MsgType.Text, content: '', timestamp: '0', needAck: false,
      });
      setRenaming(false);
    } catch (err) {
      const msg = err instanceof Error ? err.message : '重命名失败';
      alert(msg);
    } finally {
      setRenamingLoading(false);
    }
  };

  const handleTransfer = async (toUid: string) => {
    if (!confirm(`确定要将群主转让给 ${toUid} 吗？此操作不可撤销。`)) return;
    setTransferring(toUid);
    try {
      await transferGroup(uid, group.id, toUid);
      // Refresh group info
      wsManager.send({
        seq: '0', msgId: '0', cmd: Cmd.GroupInfo, from: uid, to: group.id,
        chatType: ChatType.Group, msgType: MsgType.Text, content: '', timestamp: '0', needAck: false,
      });
    } catch (err) {
      const msg = err instanceof Error ? err.message : '转让失败';
      alert(msg);
    } finally {
      setTransferring(null);
    }
  };

  return (
    <div className="fixed inset-y-0 right-0 z-40 w-80 bg-white border-l border-gray-200 shadow-xl">
      <div className="flex flex-col h-full">
        {/* Header */}
        <div className="h-14 px-4 flex items-center justify-between border-b border-gray-200 flex-shrink-0">
          <h3 className="text-base font-semibold text-gray-900">群组信息</h3>
          <button onClick={onClose} className="p-1 rounded-lg hover:bg-gray-100">
            <X className="w-5 h-5 text-gray-500" />
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-4 space-y-4">
          <div className="text-center">
            <div className="w-16 h-16 rounded-full bg-primary-500 flex items-center justify-center text-white text-xl font-bold mx-auto">
              {group.name.slice(0, 2).toUpperCase()}
            </div>
            {/* Editable group name */}
            {isOwner && renaming ? (
              <div className="mt-2 flex items-center justify-center gap-1">
                <input
                  type="text"
                  value={newGroupName}
                  onChange={(e) => setNewGroupName(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') handleRename();
                    if (e.key === 'Escape') {
                      setRenaming(false);
                      setNewGroupName(group.name);
                    }
                  }}
                  className="px-2 py-0.5 border border-primary-300 rounded text-sm text-center focus:outline-none focus:border-primary-400"
                  autoFocus
                />
                <button
                  onClick={handleRename}
                  disabled={renamingLoading}
                  className="p-1 rounded hover:bg-primary-100 text-primary-600"
                >
                  <Check className="w-4 h-4" />
                </button>
                <button
                  onClick={() => { setRenaming(false); setNewGroupName(group.name); }}
                  className="p-1 rounded hover:bg-gray-100 text-gray-400"
                >
                  <X className="w-4 h-4" />
                </button>
              </div>
            ) : (
              <div className="flex items-center justify-center gap-1 mt-1">
                <h2 className="text-lg font-bold text-gray-900">{group.name}</h2>
                {isOwner && (
                  <button
                    onClick={() => { setNewGroupName(group.name); setRenaming(true); }}
                    className="p-0.5 rounded hover:bg-gray-100 text-gray-400 hover:text-gray-600"
                    title="重命名群组"
                  >
                    <Edit3 className="w-3.5 h-3.5" />
                  </button>
                )}
              </div>
            )}
            <p className="text-xs text-gray-500">{group.id}</p>
          </div>

          <div className="space-y-2">
            <div className="flex justify-between text-sm">
              <span className="text-gray-500">群主</span>
              <span className="font-medium">{group.ownerUid}</span>
            </div>
            <div className="flex justify-between text-sm">
              <span className="text-gray-500">成员</span>
              <span className="font-medium">{group.members.length} 人</span>
            </div>
            <div className="flex justify-between text-sm">
              <span className="text-gray-500">创建时间</span>
              <span className="font-medium">
                {new Date(group.createdAt).toLocaleDateString('zh-CN')}
              </span>
            </div>
          </div>

          {/* Members */}
          <div>
            <div className="flex items-center justify-between mb-2">
              <h4 className="text-sm font-semibold text-gray-900">
                成员 ({group.members.length})
              </h4>
              <button
                onClick={() => setShowAddMember(!showAddMember)}
                className="flex items-center gap-1 text-xs text-primary-600 hover:text-primary-700 font-medium"
              >
                <UserPlus className="w-3.5 h-3.5" />
                添加
              </button>
            </div>

            {/* Add member form */}
            {showAddMember && (
              <div className="mb-3 p-3 bg-gray-50 rounded-lg space-y-2">
                <div className="flex gap-2">
                  <input
                    type="text"
                    value={newMemberUid}
                    onChange={(e) => setNewMemberUid(e.target.value)}
                    placeholder="输入用户 UID"
                    className="flex-1 px-2.5 py-1.5 text-sm border border-gray-200 rounded-lg focus:outline-none focus:border-primary-400"
                    onKeyDown={(e) => e.key === 'Enter' && handleAddMember()}
                  />
                  <button
                    onClick={handleAddMember}
                    disabled={adding || !newMemberUid.trim()}
                    className="px-3 py-1.5 bg-primary-500 text-white text-sm font-medium rounded-lg hover:bg-primary-600 disabled:opacity-50 transition-colors"
                  >
                    {adding ? '...' : '添加'}
                  </button>
                </div>
                {addStatus && (
                  <div
                    className={`flex items-center gap-1 text-xs ${
                      addStatus.type === 'success' ? 'text-green-600' : 'text-red-500'
                    }`}
                  >
                    {addStatus.type === 'success' ? (
                      <Check className="w-3 h-3" />
                    ) : (
                      <AlertCircle className="w-3 h-3" />
                    )}
                    {addStatus.text}
                  </div>
                )}
              </div>
            )}

            <div className="space-y-1">
              {group.members.map((member) => (
                <div
                  key={member}
                  className="flex items-center gap-2 px-2 py-1.5 rounded-lg hover:bg-gray-50 group/member"
                >
                  <div className="w-7 h-7 rounded-full bg-primary-500 flex items-center justify-center text-white text-xs font-semibold">
                    {member.slice(0, 2).toUpperCase()}
                  </div>
                  <span className="text-sm text-gray-900 flex-1">{member}</span>
                  {member === group.ownerUid && (
                    <span className="text-[10px] px-1.5 py-0.5 bg-primary-100 text-primary-700 rounded-full">
                      群主
                    </span>
                  )}
                  {/* Transfer button: visible to owner only, for non-owner members */}
                  {isOwner && member !== uid && member !== group.ownerUid && (
                    <button
                      onClick={() => handleTransfer(member)}
                      disabled={transferring === member}
                      className="p-1 rounded text-gray-400 hover:text-blue-500 hover:bg-blue-50 opacity-0 group-hover/member:opacity-100 transition-all disabled:opacity-50"
                      title="转让群主"
                    >
                      <UserCheck className="w-3.5 h-3.5" />
                    </button>
                  )}
                  {/* Kick button: visible to owner only, not for self */}
                  {isOwner && member !== uid && (
                    <button
                      onClick={() => handleKick(member)}
                      disabled={kicking === member}
                      className="p-1 rounded text-gray-400 hover:text-red-500 hover:bg-red-50 opacity-0 group-hover/member:opacity-100 transition-all disabled:opacity-50"
                      title="移出群组"
                    >
                      <UserMinus className="w-3.5 h-3.5" />
                    </button>
                  )}
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
