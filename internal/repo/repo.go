// Package repo 定义数据持久化接口与 MySQL 实现。
package repo

import (
	"context"

	"github.com/im/api/proto"
)

// User 表示一个已注册的用户。
type User struct {
	UID          string `json:"uid"`
	Username     string `json:"username"`
	PasswordHash string `json:"-"`
	Role         string `json:"role"` // "user" 或 "admin"
	IsDisabled   bool   `json:"is_disabled"`
	CreatedAt    int64  `json:"created_at"` // 毫秒级 Unix 时间戳
}

// UserStore 管理用户持久化。
type UserStore interface {
	Create(ctx context.Context, u *User) error
	GetByUID(ctx context.Context, uid string) (*User, error)
	UpdatePassword(ctx context.Context, uid, newPasswordHash string) error
	// 管理员方法
	ListUsers(ctx context.Context, offset, limit int) ([]*User, int, error)
	DeleteUser(ctx context.Context, uid string) error
	UpdateUserRole(ctx context.Context, uid, role string) error
	UpdateUserDisabled(ctx context.Context, uid string, disabled bool) error
	CountUsers(ctx context.Context) (int, error)
}

// SearchParams 保存全文搜索参数。
type SearchParams struct {
	UID      string // 搜索用户（访问控制 —— 必须是参与者）
	Query    string // 搜索词
	Peer     string // 可选：限定到单个会话
	ChatType int32  // 可选过滤：0=全部，1=单聊，2=群聊
	MsgType  int32  // 可选过滤：0=全部
	Before   int64  // 可选：最大时间戳（毫秒），0 = 当前时间
	After    int64  // 可选：最小时间戳（毫秒）
	Limit    int    // 最大结果数，默认 20，最大 50
	Cursor   int64  // 键集分页：最后一条结果的 msg_id
}

// SearchResult 保存全文搜索的结果。
type SearchResult struct {
	Messages   []*proto.Message
	Count      int   // 本页结果数量
	NextCursor int64 // 0 = 没有更多页
}

// FriendStatus 表示好友请求的状态。
type FriendStatus int32

const (
	FriendStatusPending  FriendStatus = 0 // 请求已发送，等待响应
	FriendStatusAccepted FriendStatus = 1 // 请求已接受，已成为好友
	FriendStatusRejected FriendStatus = 2 // 请求已被拒绝
)

// Friend 表示一条好友关系。
type Friend struct {
	UID       string       `json:"uid"`
	FriendUID string       `json:"friend_uid"`
	Status    FriendStatus `json:"status"`
	CreatedAt int64        `json:"created_at"` // 毫秒级 Unix 时间戳
}

// FriendRequest 表示一条收到的好友请求（从接收方的角度）。
type FriendRequest struct {
	FromUID   string `json:"from_uid"`
	Username  string `json:"username"`   // 请求方的显示名称（如果已知）
	CreatedAt int64  `json:"created_at"` // 毫秒级 Unix 时间戳
}

// FriendStore 管理好友关系与请求。
type FriendStore interface {
	// SendRequest 发送一条从 fromUID 到 toUID 的好友请求。
	// 如果两者之间任何方向已存在请求，则返回错误。
	SendRequest(ctx context.Context, fromUID, toUID string) error
	// AcceptRequest 接受一条待处理的好友请求。
	AcceptRequest(ctx context.Context, uid, fromUID string) error
	// RejectRequest 拒绝一条待处理的好友请求。
	RejectRequest(ctx context.Context, uid, fromUID string) error
	// RemoveFriend 移除一条好友关系（任意方向）。
	RemoveFriend(ctx context.Context, uid, friendUID string) error
	// GetFriends 返回一个用户的所有已接受好友。
	GetFriends(ctx context.Context, uid string) ([]*Friend, error)
	// GetPendingRequests 返回一个用户收到的所有待处理好友请求。
	GetPendingRequests(ctx context.Context, uid string) ([]*FriendRequest, error)
}

// MessageStore 管理消息历史记录的持久化。
type MessageStore interface {
	Save(ctx context.Context, msg *proto.Message) error
	QueryHistory(ctx context.Context, uid1, uid2 string, before int64, limit int) ([]*proto.Message, error)
	QueryGroupHistory(ctx context.Context, groupID string, before int64, limit int) ([]*proto.Message, error)
	SearchMessages(ctx context.Context, params *SearchParams) (*SearchResult, error)
	RecallMessage(ctx context.Context, msgID int64, fromUID string, recallWindowMs int64) error
	UpdateMessageContent(ctx context.Context, msgID int64, fromUID, newContent string) error
	// 管理员方法
	BrowseMessages(ctx context.Context, before int64, limit int) ([]*proto.Message, error)
	DeleteMessage(ctx context.Context, msgID int64) error
	CountMessages(ctx context.Context) (int, error)
}
