package gateway

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/im/api/proto"
	"github.com/im/internal/repo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// LogicClient wraps the gRPC client for the Logic service.
// It provides synchronous query methods (history, user lookup)
// that transparently call the remote Logic service.
type LogicClient struct {
	conn   *grpc.ClientConn
	client proto.LogicClient
}

// NewLogicClient dials the Logic gRPC server and returns a client.
// Returns nil if addr is empty (gRPC disabled).
func NewLogicClient(addr string) (*LogicClient, error) {
	if addr == "" {
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		log.Printf("[grpc] failed to dial logic service at %s: %v", addr, err)
		return nil, err
	}

	client := proto.NewLogicClient(conn)
	log.Printf("[grpc] logic client connected to %s", addr)
	return &LogicClient{conn: conn, client: client}, nil
}

// QueryHistory returns paginated conversation history from the Logic service.
// Returns nil, nil if the client is nil (gRPC disabled).
func (c *LogicClient) QueryHistory(ctx context.Context, uid, peer string, before int64, limit int) ([]*proto.Message, error) {
	if c == nil {
		return nil, nil
	}

	req := &proto.HistoryRequest{
		Uid:    uid,
		Peer:   peer,
		Before: before,
		Limit:  int32(limit),
	}

	resp, err := c.client.QueryHistory(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.Messages, nil
}

// QueryGroupHistory returns paginated group history from the Logic service.
// Returns nil, nil if the client is nil (gRPC disabled).
func (c *LogicClient) QueryGroupHistory(ctx context.Context, groupID string, before int64, limit int) ([]*proto.Message, error) {
	if c == nil {
		return nil, nil
	}

	// Reuse HistoryRequest — "peer" field carries the group ID when ChatTypeGroup is used.
	// The Logic service distinguishes group history by the ChatType field on stored messages.
	req := &proto.HistoryRequest{
		Uid:    groupID, // group ID as the conversation identifier
		Peer:   "",
		Before: before,
		Limit:  int32(limit),
	}

	resp, err := c.client.QueryHistory(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.Messages, nil
}

// SearchMessages performs a fulltext search via the Logic service.
// Returns nil, nil if the client is nil (gRPC disabled).
func (c *LogicClient) SearchMessages(ctx context.Context, params *repo.SearchParams) (*repo.SearchResult, error) {
	if c == nil {
		return nil, nil
	}

	req := &proto.SearchRequest{
		Uid:      params.UID,
		Query:    params.Query,
		Peer:     params.Peer,
		ChatType: params.ChatType,
		MsgType:  params.MsgType,
		Before:   params.Before,
		After:    params.After,
		Limit:    int32(params.Limit),
		Cursor:   params.Cursor,
	}

	resp, err := c.client.SearchMessages(ctx, req)
	if err != nil {
		return nil, err
	}

	return &repo.SearchResult{
		Messages:   resp.Messages,
		Count:      int(resp.Count),
		NextCursor: resp.NextCursor,
	}, nil
}

// GetUser looks up a user by UID via the Logic service.
// Returns nil, nil if the client is nil (gRPC disabled).
func (c *LogicClient) GetUser(ctx context.Context, uid string) (*repo.User, error) {
	if c == nil {
		return nil, nil
	}

	resp, err := c.client.GetUser(ctx, &proto.UserRequest{Uid: uid})
	if err != nil {
		return nil, err
	}
	if !resp.Found {
		return nil, nil
	}
	return &repo.User{
		UID:      resp.Uid,
		Username: resp.Username,
	}, nil
}

// ---------- Group RPCs ----------

// CreateGroupClient creates a new group via the Logic service.
// Returns nil, nil if the client is nil (gRPC disabled).
func (c *LogicClient) CreateGroupClient(ctx context.Context, name, ownerUID string) (*proto.GroupInfo, error) {
	if c == nil {
		return nil, nil
	}
	resp, err := c.client.CreateGroup(ctx, &proto.CreateGroupRequest{Name: name, OwnerUid: ownerUID})
	if err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, errors.New(resp.Error)
	}
	return resp.Group, nil
}

// JoinGroupClient adds a user to a group via the Logic service.
// Returns an error string (empty = success), or nil if gRPC disabled.
func (c *LogicClient) JoinGroupClient(ctx context.Context, groupID, uid string) error {
	if c == nil {
		return nil
	}
	resp, err := c.client.JoinGroup(ctx, &proto.JoinGroupRequest{GroupId: groupID, Uid: uid})
	if err != nil {
		return err
	}
	if resp.Error != "" {
		return errors.New(resp.Error)
	}
	return nil
}

// LeaveGroupClient removes a user from a group via the Logic service.
// Returns whether the group was deleted, or nil if gRPC disabled.
func (c *LogicClient) LeaveGroupClient(ctx context.Context, groupID, uid string) (deleted bool, err error) {
	if c == nil {
		return false, nil
	}
	resp, err := c.client.LeaveGroup(ctx, &proto.LeaveGroupRequest{GroupId: groupID, Uid: uid})
	if err != nil {
		return false, err
	}
	if resp.Error != "" {
		return false, errors.New(resp.Error)
	}
	return resp.Deleted, nil
}

// GetGroupClient returns group info with members via the Logic service.
// Returns nil, nil if not found or gRPC disabled.
func (c *LogicClient) GetGroupClient(ctx context.Context, groupID string) (*proto.GroupInfo, error) {
	if c == nil {
		return nil, nil
	}
	resp, err := c.client.GetGroup(ctx, &proto.GetGroupRequest{GroupId: groupID})
	if err != nil {
		return nil, err
	}
	if !resp.Found {
		return nil, nil
	}
	return resp.Group, nil
}

// ListGroupsClient returns the groups a user belongs to via the Logic service.
// Returns nil, nil if gRPC disabled.
func (c *LogicClient) ListGroupsClient(ctx context.Context, uid string) ([]*proto.GroupInfo, error) {
	if c == nil {
		return nil, nil
	}
	resp, err := c.client.ListGroups(ctx, &proto.ListGroupsRequest{Uid: uid})
	if err != nil {
		return nil, err
	}
	return resp.Groups, nil
}

// ---------- Unread RPCs ----------

// IncrementUnreadClient increments the unread count for (uid, peer) via the Logic service.
// Returns the new count, or 0 if gRPC disabled.
func (c *LogicClient) IncrementUnreadClient(ctx context.Context, uid, peer string) (int64, error) {
	if c == nil {
		return 0, nil
	}
	resp, err := c.client.IncrementUnread(ctx, &proto.IncrementUnreadRequest{Uid: uid, Peer: peer})
	if err != nil {
		return 0, err
	}
	return resp.NewCount, nil
}

// MarkReadClient clears the unread count for (reader, peer) via the Logic service.
// Returns nil if gRPC disabled.
func (c *LogicClient) MarkReadClient(ctx context.Context, reader, peer string) error {
	if c == nil {
		return nil
	}
	resp, err := c.client.MarkRead(ctx, &proto.MarkReadRequest{Reader: reader, Peer: peer})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return errors.New("mark read failed")
	}
	return nil
}

// GetUnreadCountsClient returns all unread counts for a user via the Logic service.
// Returns nil, nil if gRPC disabled.
func (c *LogicClient) GetUnreadCountsClient(ctx context.Context, uid string) (map[string]int64, error) {
	if c == nil {
		return nil, nil
	}
	resp, err := c.client.GetUnreadCounts(ctx, &proto.GetUnreadCountsRequest{Uid: uid})
	if err != nil {
		return nil, err
	}
	counts := make(map[string]int64, len(resp.Counts))
	for _, uc := range resp.Counts {
		counts[uc.Peer] = uc.Count
	}
	return counts, nil
}

// Close shuts down the gRPC connection.
func (c *LogicClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
