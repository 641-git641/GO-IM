// Package proto 定义 IM 消息协议的常量与校验逻辑。
// Message 结构体由 message.proto 生成于 message.pb.go 中。
package proto

import "errors"

// 命令类型。
const (
	CmdNone        int32 = 0  // 哨兵值：未设置命令（未初始化的消息）
	CmdChat        int32 = 1  // 聊天消息
	CmdAck         int32 = 2  // 确认应答
	CmdLogin       int32 = 3  // 登录请求
	CmdLoginResp   int32 = 4  // 登录响应
	CmdOffline     int32 = 5  // 请求离线消息
	CmdHeartbeat   int32 = 6  // 心跳 ping/pong
	CmdKick        int32 = 7  // 服务器主动踢下线（多设备场景）
	CmdHistory     int32 = 8  // 请求消息历史记录
	CmdReadReceipt int32 = 9  // 已读回执：客户端确认已读消息
	CmdUnreadCount int32 = 10 // 未读数量的请求/响应
	CmdSearch      int32 = 11 // 对消息内容进行全文搜索

	// 群组管理命令。
	CmdGroupCreate int32 = 12 // 创建群组
	CmdGroupJoin   int32 = 13 // 加入群组
	CmdGroupLeave  int32 = 14 // 退出群组
	CmdGroupInfo   int32 = 15 // 获取群组信息与成员
	CmdGroupList   int32 = 16 // 列出用户所属的群组

	// 文件消息。
	CmdFile int32 = 17 // 文件引用消息（走完整聊天管道）

	// 群组邀请（群主邀请第三方加入已有群组）。
	CmdGroupInviteMember int32 = 18 // 邀请用户加入群组（仅限群主）

	// 消息撤回。
	CmdRecall int32 = 19 // 撤回之前发送的消息（在配置的撤回时间窗口内）

	// 好友管理命令。
	CmdFriendRequest  int32 = 20 // 发送/接收好友请求
	CmdFriendResponse int32 = 21 // 接受/拒绝好友请求的响应

	// 输入状态提示。
	CmdTyping int32 = 22 // 输入状态提示（临时的，不持久化）

	// 消息转发。
	CmdForward int32 = 23 // 将消息转发到另一个会话

	// 消息编辑。
	CmdEdit int32 = 24 // 编辑之前发送的消息
)

// 聊天类型。
const (
	ChatTypeSingle int32 = 1
	ChatTypeGroup  int32 = 2
)

// 消息类型。
const (
	MsgTypeText  int32 = 1
	MsgTypeImage int32 = 2
	MsgTypeVoice int32 = 3
	MsgTypeVideo int32 = 4
	MsgTypeFile  int32 = 5
)

// 校验错误。
var (
	ErrInvalidCmd    = errors.New("invalid command")
	ErrMissingTarget = errors.New("chat message missing target")
)

// Validate 检查消息的命令类型所对应的字段是否有效。
func (m *Message) Validate() error {
	if m.Cmd <= CmdNone || m.Cmd > CmdEdit {
		return errors.New("invalid command")
	}
	if (m.Cmd == CmdChat || m.Cmd == CmdHistory || m.Cmd == CmdReadReceipt || m.Cmd == CmdFile || m.Cmd == CmdRecall || m.Cmd == CmdFriendRequest || m.Cmd == CmdFriendResponse || m.Cmd == CmdForward || m.Cmd == CmdEdit) && m.To == "" {
		return ErrMissingTarget
	}
	return nil
}
