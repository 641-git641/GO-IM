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

// LogicClient 封装指向 Logic 服务的 gRPC 客户端。
// 它提供同步查询方法(历史记录、用户查询),
// 这些方法透明地调用远程 Logic 服务。
type LogicClient struct {
	conn   *grpc.ClientConn
	client proto.LogicClient
}

// NewLogicClient 拨号 Logic gRPC 服务器并返回客户端。
// 如果 addr 为空(gRPC 禁用)则返回 nil。
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

// QueryHistory 从 Logic 服务返回分页的会话历史记录。
// 如果客户端为 nil(gRPC 禁用)则返回 nil, nil。
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

// QueryGroupHistory 从 Logic 服务返回分页的群聊历史记录。
// 如果客户端为 nil(gRPC 禁用)则返回 nil, nil。
func (c *LogicClient) QueryGroupHistory(ctx context.Context, groupID string, before int64, limit int) ([]*proto.Message, error) {
	if c == nil {
		return nil, nil
	}

	// 复用 HistoryRequest —— 使用 ChatTypeGroup 时 "peer" 字段携带群 ID。
	// Logic 服务通过已存消息上的 ChatType 字段区分群聊历史。
	req := &proto.HistoryRequest{
		Uid:    groupID, // 群 ID 作为会话标识符
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

// SearchMessages 通过 Logic 服务执行全文搜索。
// 如果客户端为 nil(gRPC 禁用)则返回 nil, nil。
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

// GetUser 通过 Logic 服务按 UID 查询用户。
// 如果客户端为 nil(gRPC 禁用)则返回 nil, nil。
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

// ---------- 群相关 RPC ----------

// CreateGroupClient 通过 Logic 服务创建新群。
// 如果客户端为 nil(gRPC 禁用)则返回 nil, nil。
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

// JoinGroupClient 通过 Logic 服务将用户加入群。
// 返回错误字符串(空串 = 成功),gRPC 禁用时返回 nil。
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

// LeaveGroupClient 通过 Logic 服务将用户移出群。
// 返回群是否已被删除,gRPC 禁用时返回 nil。
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

// GetGroupClient 通过 Logic 服务返回群信息(含成员)。
// 未找到或 gRPC 禁用时返回 nil, nil。
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

// ListGroupsClient 通过 Logic 服务返回用户所属的群列表。
// gRPC 禁用时返回 nil, nil。
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

// ---------- 未读相关 RPC ----------

// IncrementUnreadClient 通过 Logic 服务增加 (uid, peer) 的未读计数。
// 返回新的计数,gRPC 禁用时返回 0。
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

// MarkReadClient 通过 Logic 服务清除 (reader, peer) 的未读计数。
// gRPC 禁用时返回 nil。
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

// GetUnreadCountsClient 通过 Logic 服务返回用户的所有未读计数。
// gRPC 禁用时返回 nil, nil。
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

// Close 关闭 gRPC 连接。
func (c *LogicClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
