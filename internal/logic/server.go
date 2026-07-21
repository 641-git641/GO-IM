package logic

import (
	"context"
	"log"
	"time"

	"github.com/im/api/proto"
	"github.com/im/internal/pkg/snowflake"
	"github.com/im/internal/repo"
)

// Server implements the Logic gRPC service (proto.LogicServer).
// It provides synchronous query RPCs backed by MySQL.
type Server struct {
	proto.UnimplementedLogicServer
	mysql       *repo.MySQLStore
	userRepo    repo.UserStore        // for GetUser
	groupStore  *repo.MySQLGroupStore  // for Group RPCs
	unreadStore *repo.MySQLUnreadStore // for Unread RPCs
}

// NewServer creates a Logic gRPC server.
func NewServer(mysql *repo.MySQLStore, workerID int64) *Server {
	s := &Server{
		mysql:    mysql,
		userRepo: mysql,
	}
	if mysql != nil {
		// Create a snowflake generator for group IDs. The worker ID should be distinct
		// from the gateway's worker ID to avoid ID collisions.
		if workerID <= 0 {
			workerID = 2 // default fallback
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

// QueryHistory returns paginated conversation history between two users.
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
		}, nil // return empty, not gRPC error — client tolerates empty
	}

	return &proto.HistoryResponse{
		Messages:  msgs,
		Delivered: int32(len(msgs)),
	}, nil
}

// SearchMessages performs a fulltext search on message content.
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
		return &proto.SearchResponse{}, nil // return empty, not gRPC error — client tolerates empty
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

// ---------- Group RPCs ----------

func groupInfoFromRow(g *repo.GroupRow, members []string) *proto.GroupInfo {
	return &proto.GroupInfo{
		Id:        g.ID,
		Name:      g.Name,
		OwnerUid:  g.OwnerUID,
		Members:   members,
		CreatedAt: g.CreatedAt,
	}
}

// CreateGroup creates a new group and adds the owner as the first member.
func (s *Server) CreateGroup(ctx context.Context, req *proto.CreateGroupRequest) (*proto.CreateGroupResponse, error) {
	if s.groupStore == nil {
		return &proto.CreateGroupResponse{Error: "group store not available"}, nil
	}

	g, err := s.groupStore.CreateGroup(ctx, req.Name, req.OwnerUid, nil) // members added by gateway via JoinGroup
	if err != nil {
		log.Printf("[logic] CreateGroup error (%s): %v", req.Name, err)
		return &proto.CreateGroupResponse{Error: err.Error()}, nil
	}

	members, _ := s.groupStore.GetMembers(ctx, g.ID)
	return &proto.CreateGroupResponse{Group: groupInfoFromRow(g, members)}, nil
}

// JoinGroup adds a user to a group.
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

// LeaveGroup removes a user from a group. Deletes the group if empty.
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

// GetGroup returns group details including members.
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

// ListGroups returns all groups the user belongs to.
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

// GetUser looks up a user by UID.
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

// ---------- Unread RPCs ----------

// IncrementUnread increments the unread count for (uid, peer) and returns the new count.
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

// MarkRead clears the unread count for (reader, peer).
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

// GetUnreadCounts returns all unread counts for a user.
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
