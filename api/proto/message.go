// Package proto defines the IM message protocol constants and validation.
// The Message struct is generated in message.pb.go from message.proto.
package proto

import "errors"

// Command types.
const (
	CmdNone      int32 = 0 // sentinel: no command set (uninitialized message)
	CmdChat      int32 = 1 // chat message
	CmdAck       int32 = 2 // acknowledgement
	CmdLogin     int32 = 3 // login request
	CmdLoginResp int32 = 4 // login response
	CmdOffline   int32 = 5 // request offline messages
	CmdHeartbeat int32 = 6 // heartbeat ping/pong
	CmdKick      int32 = 7 // server-initiated kick (multi-device)
	CmdHistory     int32 = 8  // request message history
	CmdReadReceipt int32 = 9  // read receipt: client acknowledges reading messages
	CmdUnreadCount int32 = 10 // request/response for unread counts
	CmdSearch      int32 = 11 // fulltext search on message content

	// Group management commands.
	CmdGroupCreate int32 = 12 // create a group
	CmdGroupJoin   int32 = 13 // join a group
	CmdGroupLeave  int32 = 14 // leave a group
	CmdGroupInfo   int32 = 15 // get group info + members
	CmdGroupList   int32 = 16 // list groups the user belongs to

	// File message.
	CmdFile int32 = 17 // file reference message (goes through full chat pipeline)

	// Group invite (owner invites a third party to an existing group).
	CmdGroupInviteMember int32 = 18 // invite a user to a group (owner only)

	// Message recall.
	CmdRecall int32 = 19 // recall a previously sent message (within configured recall window)

	// Friend management commands.
	CmdFriendRequest  int32 = 20 // send/receive friend request
	CmdFriendResponse int32 = 21 // accept/reject friend request response

	// Typing indicator.
	CmdTyping int32 = 22 // typing indicator (ephemeral, not persisted)

	// Message forwarding.
	CmdForward int32 = 23 // forward a message to another conversation

	// Message editing.
	CmdEdit int32 = 24 // edit a previously sent message
)

// Chat types.
const (
	ChatTypeSingle int32 = 1
	ChatTypeGroup  int32 = 2
)

// Message types.
const (
	MsgTypeText  int32 = 1
	MsgTypeImage int32 = 2
	MsgTypeVoice int32 = 3
	MsgTypeVideo int32 = 4
	MsgTypeFile  int32 = 5
)

// Validation errors.
var (
	ErrInvalidCmd    = errors.New("invalid command")
	ErrMissingTarget = errors.New("chat message missing target")
)

// Validate checks that the message has valid fields for its command type.
func (m *Message) Validate() error {
	if m.Cmd <= CmdNone || m.Cmd > CmdEdit {
		return errors.New("invalid command")
	}
	if (m.Cmd == CmdChat || m.Cmd == CmdHistory || m.Cmd == CmdReadReceipt || m.Cmd == CmdFile || m.Cmd == CmdRecall || m.Cmd == CmdFriendRequest || m.Cmd == CmdFriendResponse || m.Cmd == CmdForward || m.Cmd == CmdEdit) && m.To == "" {
		return ErrMissingTarget
	}
	return nil
}
