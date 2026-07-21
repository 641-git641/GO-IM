// Package repo defines data persistence interfaces and MySQL implementations.
package repo

import (
	"context"

	"github.com/im/api/proto"
)

// User represents a registered user.
type User struct {
	UID          string `json:"uid"`
	Username     string `json:"username"`
	PasswordHash string `json:"-"`
	Role         string `json:"role"`         // "user" or "admin"
	IsDisabled   bool   `json:"is_disabled"`
	CreatedAt    int64  `json:"created_at"`   // unix millis
}

// UserStore manages user persistence.
type UserStore interface {
	Create(ctx context.Context, u *User) error
	GetByUID(ctx context.Context, uid string) (*User, error)
	UpdatePassword(ctx context.Context, uid, newPasswordHash string) error
	// Admin methods
	ListUsers(ctx context.Context, offset, limit int) ([]*User, int, error)
	DeleteUser(ctx context.Context, uid string) error
	UpdateUserRole(ctx context.Context, uid, role string) error
	UpdateUserDisabled(ctx context.Context, uid string, disabled bool) error
	CountUsers(ctx context.Context) (int, error)
}

// SearchParams holds fulltext search parameters.
type SearchParams struct {
	UID      string // searching user (access control — must be participant)
	Query    string // search terms
	Peer     string // optional: scope to one conversation
	ChatType int32  // optional filter: 0=all, 1=single, 2=group
	MsgType  int32  // optional filter: 0=all
	Before   int64  // optional: max timestamp (ms), 0 = now
	After    int64  // optional: min timestamp (ms)
	Limit    int    // max results, default 20, max 50
	Cursor   int64  // keyset pagination: msg_id of last result
}

// SearchResult holds the results of a fulltext search.
type SearchResult struct {
	Messages   []*proto.Message
	Count      int   // number of results in this page
	NextCursor int64 // 0 = no more pages
}

// FriendStatus represents the state of a friendship request.
type FriendStatus int32

const (
	FriendStatusPending  FriendStatus = 0 // request sent, awaiting response
	FriendStatusAccepted FriendStatus = 1 // request accepted, now friends
	FriendStatusRejected FriendStatus = 2 // request rejected
)

// Friend represents a friendship relationship.
type Friend struct {
	UID       string       `json:"uid"`
	FriendUID string       `json:"friend_uid"`
	Status    FriendStatus `json:"status"`
	CreatedAt int64        `json:"created_at"` // unix millis
}

// FriendRequest represents an incoming friend request (from the receiver's perspective).
type FriendRequest struct {
	FromUID   string `json:"from_uid"`
	Username  string `json:"username"`  // display name of the requester (if known)
	CreatedAt int64  `json:"created_at"` // unix millis
}

// FriendStore manages friend relationships and requests.
type FriendStore interface {
	// SendRequest sends a friend request from fromUID to toUID.
	// Returns an error if a request already exists between them in any direction.
	SendRequest(ctx context.Context, fromUID, toUID string) error
	// AcceptRequest accepts a pending friend request.
	AcceptRequest(ctx context.Context, uid, fromUID string) error
	// RejectRequest rejects a pending friend request.
	RejectRequest(ctx context.Context, uid, fromUID string) error
	// RemoveFriend removes a friend relationship (either direction).
	RemoveFriend(ctx context.Context, uid, friendUID string) error
	// GetFriends returns all accepted friends for a user.
	GetFriends(ctx context.Context, uid string) ([]*Friend, error)
	// GetPendingRequests returns all incoming pending friend requests for a user.
	GetPendingRequests(ctx context.Context, uid string) ([]*FriendRequest, error)
}

// MessageStore manages message history persistence.
type MessageStore interface {
	Save(ctx context.Context, msg *proto.Message) error
	QueryHistory(ctx context.Context, uid1, uid2 string, before int64, limit int) ([]*proto.Message, error)
	QueryGroupHistory(ctx context.Context, groupID string, before int64, limit int) ([]*proto.Message, error)
	SearchMessages(ctx context.Context, params *SearchParams) (*SearchResult, error)
	RecallMessage(ctx context.Context, msgID int64, fromUID string, recallWindowMs int64) error
	UpdateMessageContent(ctx context.Context, msgID int64, fromUID, newContent string) error
	// Admin methods
	BrowseMessages(ctx context.Context, before int64, limit int) ([]*proto.Message, error)
	DeleteMessage(ctx context.Context, msgID int64) error
	CountMessages(ctx context.Context) (int, error)
}
