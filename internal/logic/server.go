package logic

import (
	"context"
	"log"
	"time"

	"github.com/im/api/proto"
	"github.com/im/internal/pkg/snowflake"
	"github.com/im/internal/repo"
)

// Server 实现 Logic gRPC 服务（proto.LogicServer）。
// 它提供基于 MySQL 的同步查询 RPC。
type Server struct {
	proto.UnimplementedLogicServer
	mysql       *repo.MySQLStore
	userRepo    repo.UserStore        // 用于 GetUser
	groupStore  *repo.MySQLGroupStore  // 用于群组 RPC
	unreadStore *repo.MySQLUnreadStore // 用于未读 RPC
}

// NewServer 创建一个 Logic gRPC 服务器。
func NewServer(mysql *repo.MySQLStore, workerID int64) *Server {
	s := &Server{
		mysql:    mysql,
		userRepo: mysql,
	}
	if mysql != nil {
		// 为群组 ID 创建 snowflake 生成器。worker ID 应与
		// 网关的 worker ID 不同，以避免 ID 冲突。
		if workerID <= 0 {
			workerID = 2 // 默认回退值
		}
		sf, err := snowflake.New(workerID)
		if err != nil {
			log.Printf("[logic] WARNING: snowflake init failed: %v — group ID generation may not be unique", err)
		} else {
			s.groupStore = repo.NewMySQLGroupStore(mysql.DB(), func() int64 { return sf.Next() })
		}
		s.unreadStore = repo.NewMySQLUnreadStore(mysql.DB())
	}
	return s
}

// QueryHistory 返回两个用户之间的分页会话历史记录。
func (s *Server) QueryHistory(ctx context.Context, req *proto.HistoryRequest) (*proto.HistoryResponse, error) {
	if s.mysql == nil {
		return &proto.HistoryResponse{Messages: nil, Delivered: 0}, nil
	}

	limit := int(req.Limit)
	if limit <= 0 {
		limit = 30
	}
	if limit > 100 {
		limit = 100
	}

	before := req.Before
	if before <= 0 {
		before = time.Now().UnixMilli()
	}

	msgs, err := s.mysql.QueryHistory(ctx, req.Uid, req.Peer, before, limit)
	if err != nil {
		log.Printf("[logic] QueryHistory error (%s<->%s): %v", req.Uid, req.Peer, err)
		return &proto.HistoryResponse{
			Messages:  nil,
			Delivered: 0,
		}, nil // 返回空值而非 gRPC 错误 —— 客户端能容忍空值
	}

	return &proto.HistoryResponse{
		Messages:  msgs,
		Delivered: int32(len(msgs)),
	}, nil
}

// SearchMessages 对消息内容执行全文搜索。
func (s *Server) SearchMessages(ctx context.Context, req *proto.SearchRequest) (*proto.SearchResponse, error) {
	if s.mysql == nil {
		return &proto.SearchResponse{}, nil
	}

	params := &repo.SearchParams{
		UID:      req.Uid,
		Query:    req.Query,
		Peer:     req.Peer,
		ChatType: req.ChatType,
		MsgType:  req.MsgType,
		Before:   req.Before,
		After:    req.After,
		Limit:    int(req.Limit),
		Cursor:   req.Cursor,
	}

	result, err := s.mysql.SearchMessages(ctx, params)
	if err != nil {
		log.Printf("[logic] SearchMessages error (uid=%s, q=%q): %v", req.Uid, req.Query, err)
		return &proto.SearchResponse{}, nil // 返回空值而非 gRPC 错误 —— 客户端能容忍空值
	}

	if result == nil {
		return &proto.SearchResponse{}, nil
	}

	return &proto.SearchResponse{
		Messages:   result.Messages,
		Count:      int32(result.Count),
		NextCursor: result.NextCursor,
	}, nil
}

// ---------- 群组 RPC ----------

func groupInfoFromRow(g *repo.GroupRow, members []string) *proto.GroupInfo {
	return &proto.GroupInfo{
		Id:        g.ID,
		Name:      g.Name,
		OwnerUid:  g.OwnerUID,
		Members:   members,
		CreatedAt: g.CreatedAt,
	}
}

// CreateGroup 创建一个新群组并将群主添加为第一位成员。
func (s *Server) CreateGroup(ctx context.Context, req *proto.CreateGroupRequest) (*proto.CreateGroupResponse, error) {
	if s.groupStore == nil {
		return &proto.CreateGroupResponse{Error: "group store not available"}, nil
	}

	g, err := s.groupStore.CreateGroup(ctx, req.Name, req.OwnerUid, nil) // 成员由网关通过 JoinGroup 添加
	if err != nil {
		log.Printf("[logic] CreateGroup error (%s): %v", req.Name, err)
		return &proto.CreateGroupResponse{Error: err.Error()}, nil
	}

	members, _ := s.groupStore.GetMembers(ctx, g.ID)
	return &proto.CreateGroupResponse{Group: groupInfoFromRow(g, members)}, nil
}

// JoinGroup 将用户添加到群组。
func (s *Server) JoinGroup(ctx context.Context, req *proto.JoinGroupRequest) (*proto.JoinGroupResponse, error) {
	if s.groupStore == nil {
		return &proto.JoinGroupResponse{Ok: false, Error: "group store not available"}, nil
	}

	if err := s.groupStore.AddMember(ctx, req.GroupId, req.Uid); err != nil {
		log.Printf("[logic] JoinGroup error (%s -> %s): %v", req.Uid, req.GroupId, err)
		return &proto.JoinGroupResponse{Ok: false, Error: err.Error()}, nil
	}

	return &proto.JoinGroupResponse{Ok: true}, nil
}

// LeaveGroup 将用户从群组中移除。如果群组为空则删除。
func (s *Server) LeaveGroup(ctx context.Context, req *proto.LeaveGroupRequest) (*proto.LeaveGroupResponse, error) {
	if s.groupStore == nil {
		return &proto.LeaveGroupResponse{Ok: false, Error: "group store not available"}, nil
	}

	deleted, err := s.groupStore.RemoveMember(ctx, req.GroupId, req.Uid)
	if err != nil {
		log.Printf("[logic] LeaveGroup error (%s -> %s): %v", req.Uid, req.GroupId, err)
		return &proto.LeaveGroupResponse{Ok: false, Error: err.Error()}, nil
	}

	return &proto.LeaveGroupResponse{Ok: true, Deleted: deleted}, nil
}

// GetGroup 返回群组详情（包括成员）。
func (s *Server) GetGroup(ctx context.Context, req *proto.GetGroupRequest) (*proto.GetGroupResponse, error) {
	if s.groupStore == nil {
		return &proto.GetGroupResponse{Found: false}, nil
	}

	g, err := s.groupStore.GetGroup(ctx, req.GroupId)
	if err != nil {
		log.Printf("[logic] GetGroup error (%s): %v", req.GroupId, err)
		return &proto.GetGroupResponse{Found: false}, nil
	}

	members, _ := s.groupStore.GetMembers(ctx, g.ID)
	return &proto.GetGroupResponse{Group: groupInfoFromRow(g, members), Found: true}, nil
}

// ListGroups 返回用户所属的全部群组。
func (s *Server) ListGroups(ctx context.Context, req *proto.ListGroupsRequest) (*proto.ListGroupsResponse, error) {
	if s.groupStore == nil {
		return &proto.ListGroupsResponse{}, nil
	}

	groups, err := s.groupStore.ListGroups(ctx, req.Uid)
	if err != nil {
		log.Printf("[logic] ListGroups error (%s): %v", req.Uid, err)
		return &proto.ListGroupsResponse{}, nil
	}

	out := make([]*proto.GroupInfo, 0, len(groups))
	for _, g := range groups {
		members, _ := s.groupStore.GetMembers(ctx, g.ID)
		out = append(out, groupInfoFromRow(g, members))
	}
	return &proto.ListGroupsResponse{Groups: out}, nil
}

// GetUser 按 UID 查找用户。
func (s *Server) GetUser(ctx context.Context, req *proto.UserRequest) (*proto.UserResponse, error) {
	if s.userRepo == nil {
		return &proto.UserResponse{Found: false}, nil
	}

	u, err := s.userRepo.GetByUID(ctx, req.Uid)
	if err != nil {
		log.Printf("[logic] GetUser error (%s): %v", req.Uid, err)
		return &proto.UserResponse{Found: false}, nil
	}
	if u == nil {
		return &proto.UserResponse{Found: false}, nil
	}

	return &proto.UserResponse{
		Uid:      u.UID,
		Username: u.Username,
		Found:    true,
	}, nil
}

// ---------- 未读 RPC ----------

// IncrementUnread 增加 (uid, peer) 的未读计数并返回新计数。
func (s *Server) IncrementUnread(ctx context.Context, req *proto.IncrementUnreadRequest) (*proto.IncrementUnreadResponse, error) {
	if s.unreadStore == nil {
		return &proto.IncrementUnreadResponse{}, nil
	}
	count, err := s.unreadStore.Increment(ctx, req.Uid, req.Peer)
	if err != nil {
		log.Printf("[logic] IncrementUnread error (%s<-%s): %v", req.Uid, req.Peer, err)
		return &proto.IncrementUnreadResponse{}, nil
	}
	return &proto.IncrementUnreadResponse{NewCount: count}, nil
}

// MarkRead 清除 (reader, peer) 的未读计数。
func (s *Server) MarkRead(ctx context.Context, req *proto.MarkReadRequest) (*proto.MarkReadResponse, error) {
	if s.unreadStore == nil {
		return &proto.MarkReadResponse{Ok: false}, nil
	}
	if err := s.unreadStore.MarkRead(ctx, req.Reader, req.Peer); err != nil {
		log.Printf("[logic] MarkRead error (%s<-%s): %v", req.Reader, req.Peer, err)
		return &proto.MarkReadResponse{Ok: false}, nil
	}
	return &proto.MarkReadResponse{Ok: true}, nil
}

// GetUnreadCounts 返回一个用户的全部未读计数。
func (s *Server) GetUnreadCounts(ctx context.Context, req *proto.GetUnreadCountsRequest) (*proto.GetUnreadCountsResponse, error) {
	if s.unreadStore == nil {
		return &proto.GetUnreadCountsResponse{}, nil
	}
	counts, err := s.unreadStore.GetCounts(ctx, req.Uid)
	if err != nil {
		log.Printf("[logic] GetUnreadCounts error (%s): %v", req.Uid, err)
		return &proto.GetUnreadCountsResponse{}, nil
	}
	out := make([]*proto.UnreadCount, 0, len(counts))
	for peer, count := range counts {
		out = append(out, &proto.UnreadCount{Peer: peer, Count: count})
	}
	return &proto.GetUnreadCountsResponse{Counts: out}, nil
}
