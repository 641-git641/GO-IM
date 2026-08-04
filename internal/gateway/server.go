package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/im/configs"
	"github.com/im/internal/pkg/jwt"
	"github.com/im/internal/pkg/snowflake"
	"github.com/im/internal/repo"
	"github.com/panjf2000/gnet/v2"
	"golang.org/x/crypto/bcrypt"
)

// Server 是支持 WebSocket 和/或 gnet TCP 传输的网关服务器。
type Server struct {
	clients    ClientRegistry
	router     *Router
	jwtMgr     *jwt.Manager
	userStore  repo.UserStore    // MySQL 禁用时为 nil
	msgStore   repo.MessageStore // MySQL 禁用时为 nil(管理员 API)
	groupStore GroupStore        // 群聊禁用时为 nil
	authCfg    configs.AuthConfig
	adminUIDs  map[string]bool // 具有管理员权限的 UID(来自配置)
	heartbeat  time.Duration
	heartFail  int
	connCfg    configs.GatewayConnConfig

	snow        *snowflake.Generator // 用于生成文件 ID
	objectStore ObjectStore          // 文件上传禁用时为 nil
	maxUpload   int64                // 最大上传大小(字节)

	// WebSocket
	upgrader websocket.Upgrader

	// gnet
	gnetHandler *GnetHandler
}

// NewServer 创建一个网关 Server。
func NewServer(clients ClientRegistry, router *Router, jwtMgr *jwt.Manager,
	userStore repo.UserStore, msgStore repo.MessageStore, groupStore GroupStore,
	authCfg configs.AuthConfig,
	heartbeat time.Duration, heartFail int,
	connCfg configs.GatewayConnConfig, allowedOrigins []string,
	snow *snowflake.Generator, objectStore ObjectStore, maxUpload int64,
	adminUIDs []string) *Server {

	adminSet := make(map[string]bool, len(adminUIDs))
	for _, uid := range adminUIDs {
		adminSet[uid] = true
	}

	s := &Server{
		clients:     clients,
		router:      router,
		jwtMgr:      jwtMgr,
		userStore:   userStore,
		msgStore:    msgStore,
		groupStore:  groupStore,
		authCfg:     authCfg,
		adminUIDs:   adminSet,
		heartbeat:   heartbeat,
		heartFail:   heartFail,
		connCfg:     connCfg,
		snow:        snow,
		objectStore: objectStore,
		maxUpload:   maxUpload,
	}
	s.upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     buildOriginChecker(allowedOrigins),
	}
	return s
}

// buildOriginChecker 返回一个 Origin 检查函数。
// 允许列表为空表示允许所有来源(开发模式)。
func buildOriginChecker(allowed []string) func(r *http.Request) bool {
	if len(allowed) == 0 {
		return func(r *http.Request) bool { return true }
	}
	allowedSet := make(map[string]bool, len(allowed))
	for _, origin := range allowed {
		allowedSet[origin] = true
	}
	return func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		if origin == "" {
			return true // 同源请求没有 Origin 头
		}
		return allowedSet[origin]
	}
}

// Recovery 用 panic 恢复包装一个 http.Handler。
func Recovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("[panic] %s %s: %v", r.Method, r.URL.Path, err)
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// HandleLogin 是用户登录的 HTTP 端点(返回 JWT)。
// 开发模式(默认)下,仅需 uid+username。
// 生产模式下,需要 uid+password,并与用户存储进行校验。
func (s *Server) HandleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form data: "+err.Error(), http.StatusBadRequest)
		return
	}

	uid := r.FormValue("uid")
	username := r.FormValue("username")
	password := r.FormValue("password")

	if uid == "" {
		uid = username
	}
	if uid == "" {
		http.Error(w, "uid required", http.StatusBadRequest)
		return
	}

	// 密码认证路径。
	role := "user"
	if password != "" {
		if s.userStore == nil {
			http.Error(w, "user store not available", http.StatusServiceUnavailable)
			return
		}
		u, err := s.userStore.GetByUID(r.Context(), uid)
		if err != nil {
			log.Printf("[server] login db error for %s: %v", uid, err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		if u == nil {
			http.Error(w, "user not found", http.StatusUnauthorized)
			return
		}
		if err := bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(password)); err != nil {
			http.Error(w, "invalid password", http.StatusUnauthorized)
			return
		}
		username = u.Username
		role = u.Role
	} else if !s.authCfg.DevMode {
		// 生产模式要求密码。
		http.Error(w, "password required", http.StatusUnauthorized)
		return
	}

	// 开发模式:允许通过表单字段覆盖角色,用于测试管理员功能。
	if s.authCfg.DevMode && r.FormValue("role") == "admin" {
		role = "admin"
	}

	// 从配置引导管理员 UID(优先级最高)。
	if s.adminUIDs[uid] {
		role = "admin"
	}

	// 开发模式回退:无密码 → 使用提供的 username(或 uid)。
	if username == "" {
		username = uid
	}

	token, err := s.jwtMgr.Generate(uid, username, role)
	if err != nil {
		http.Error(w, "generate token: "+err.Error(), http.StatusInternalServerError)
		return
	}

	data, _ := json.Marshal(map[string]string{
		"uid":      uid,
		"username": username,
		"token":    token,
	})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleRegister 是用户注册的 HTTP 端点。
// 需要 uid、username 和 password。保存密码的 bcrypt 哈希。
func (s *Server) HandleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form data: "+err.Error(), http.StatusBadRequest)
		return
	}

	uid := r.FormValue("uid")
	username := r.FormValue("username")
	password := r.FormValue("password")

	if uid == "" || password == "" {
		http.Error(w, "uid and password required", http.StatusBadRequest)
		return
	}
	if username == "" {
		username = uid
	}

	if s.userStore == nil {
		http.Error(w, "user store not available", http.StatusServiceUnavailable)
		return
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		log.Printf("[server] bcrypt error: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	u := &repo.User{
		UID:          uid,
		Username:     username,
		PasswordHash: string(hash),
		CreatedAt:    time.Now().UnixMilli(),
	}
	if err := s.userStore.Create(r.Context(), u); err != nil {
		log.Printf("[server] register error for %s: %v", uid, err)
		// MySQL 错误 1062 = 重复条目;其他错误为内部错误。
		if strings.Contains(err.Error(), "Duplicate entry") || strings.Contains(err.Error(), "Error 1062") {
			http.Error(w, "uid already exists", http.StatusConflict)
		} else {
			http.Error(w, "registration failed", http.StatusInternalServerError)
		}
		return
	}

	role := "user"
	if s.adminUIDs[uid] {
		role = "admin"
	}
	token, err := s.jwtMgr.Generate(uid, username, role)
	if err != nil {
		http.Error(w, "generate token: "+err.Error(), http.StatusInternalServerError)
		return
	}

	data, _ := json.Marshal(map[string]string{
		"uid":      uid,
		"username": username,
		"token":    token,
	})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleChangePassword 校验当前密码并更新为新密码。
// POST /change-password  (body: uid, token, old_password, new_password)
func (s *Server) HandleChangePassword(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 校验 JWT —— 修改密码必须通过认证。
	claims := s.authenticateRequest(w, r)
	if claims == nil {
		return
	}

	oldPassword := r.FormValue("old_password")
	newPassword := r.FormValue("new_password")

	if oldPassword == "" || newPassword == "" {
		http.Error(w, "old_password and new_password required", http.StatusBadRequest)
		return
	}
	if len(newPassword) < 6 {
		http.Error(w, "new password must be at least 6 characters", http.StatusBadRequest)
		return
	}

	if s.userStore == nil {
		http.Error(w, "user store not available", http.StatusServiceUnavailable)
		return
	}

	u, err := s.userStore.GetByUID(r.Context(), claims.UID)
	if err != nil {
		log.Printf("[server] change-password db error for %s: %v", claims.UID, err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if u == nil {
		http.Error(w, "user not found", http.StatusNotFound)
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(oldPassword)); err != nil {
		http.Error(w, "invalid current password", http.StatusUnauthorized)
		return
	}

	newHash, err := bcrypt.GenerateFromPassword([]byte(newPassword), bcrypt.DefaultCost)
	if err != nil {
		log.Printf("[server] bcrypt error: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	if err := s.userStore.UpdatePassword(r.Context(), claims.UID, string(newHash)); err != nil {
		log.Printf("[server] change-password update error for %s: %v", claims.UID, err)
		http.Error(w, "failed to update password", http.StatusInternalServerError)
		return
	}

	data, _ := json.Marshal(map[string]string{"status": "ok"})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// authenticateRequest 从表单(POST)或查询(GET)参数中提取 uid+token,
// 校验 JWT,并返回认证后的 claims。失败时写入
// HTTP 错误响应并返回 nil。调用方在继续前必须检查 nil。
func (s *Server) authenticateRequest(w http.ResponseWriter, r *http.Request) *jwt.Claims {
	var uid, token string
	if r.Method == http.MethodPost {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "invalid form data", http.StatusBadRequest)
			return nil
		}
		uid = r.FormValue("uid")
		token = r.FormValue("token")
	} else {
		uid = r.URL.Query().Get("uid")
		token = r.URL.Query().Get("token")
	}

	if uid == "" || token == "" {
		http.Error(w, "uid and token required", http.StatusUnauthorized)
		return nil
	}

	claims, err := s.jwtMgr.Validate(token)
	if err != nil {
		http.Error(w, "invalid token: "+err.Error(), http.StatusUnauthorized)
		return nil
	}
	if claims.UID != uid {
		http.Error(w, "uid does not match token", http.StatusForbidden)
		return nil
	}
	return claims
}

// adminAuth 认证请求并检查用户是否为管理员。
// 失败时返回 nil(并已写入 HTTP 错误响应)。
func (s *Server) adminAuth(w http.ResponseWriter, r *http.Request) *jwt.Claims {
	claims := s.authenticateRequest(w, r)
	if claims == nil {
		return nil
	}
	if claims.Role != "admin" {
		http.Error(w, "admin access required", http.StatusForbidden)
		return nil
	}
	return claims
}

// HandleOnlineUsers 返回当前在线用户。
func (s *Server) HandleOnlineUsers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	users := s.clients.OnlineUsers(ctx) // 单次调用,下面复用
	data, _ := json.Marshal(map[string]interface{}{
		"count": len(users),
		"users": users,
	})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleGroupCreate 创建新的聊天群。创建者自动成为成员。
// 可选的 'members' 参数(逗号分隔的 UID)用于添加初始成员。
// POST /group/create?uid=alice&token=JWT&name=Dev%20Team&members=bob,charlie
func (s *Server) HandleGroupCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.groupStore == nil {
		http.Error(w, "group store not available", http.StatusServiceUnavailable)
		return
	}

	claims := s.authenticateRequest(w, r)
	if claims == nil {
		return
	}

	name := r.FormValue("name")
	if name == "" {
		http.Error(w, "name required", http.StatusBadRequest)
		return
	}

	// 解析初始成员(逗号分隔的 UID)。
	var members []string
	if membersStr := r.FormValue("members"); membersStr != "" {
		for _, uid := range strings.Split(membersStr, ",") {
			uid = strings.TrimSpace(uid)
			if uid != "" {
				members = append(members, uid)
			}
		}
	}

	g, err := s.groupStore.Create(r.Context(), name, claims.UID, members)
	if err != nil {
		log.Printf("[server] group create error: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	memberList := make([]string, 0, len(g.Members))
	for uid := range g.Members {
		memberList = append(memberList, uid)
	}

	// 通知被邀请的成员(跳过自己)。
	if len(members) > 0 {
		s.router.sendGroupNotificationWithMembers(r.Context(), claims.UID, g.ID, "member_joined", memberList)
	}

	data, _ := json.Marshal(map[string]interface{}{
		"group_id":   g.ID,
		"name":       g.Name,
		"owner_uid":  g.OwnerUID,
		"members":    memberList,
		"created_at": g.CreatedAt,
	})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleGroupJoin 将用户加入群。
// POST /group/join?uid=bob&token=JWT&group_id=g_123456
func (s *Server) HandleGroupJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.groupStore == nil {
		http.Error(w, "group store not available", http.StatusServiceUnavailable)
		return
	}

	claims := s.authenticateRequest(w, r)
	if claims == nil {
		return
	}

	groupID := r.FormValue("group_id")
	if groupID == "" {
		http.Error(w, "group_id required", http.StatusBadRequest)
		return
	}

	if err := s.groupStore.AddMember(r.Context(), groupID, claims.UID); err != nil {
		switch err {
		case ErrGroupNotFound:
			http.Error(w, "group not found", http.StatusNotFound)
		case ErrAlreadyMember:
			http.Error(w, "already a member", http.StatusConflict)
		default:
			log.Printf("[server] group join error: %v", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
		}
		return
	}

	// 通过 WebSocket/TCP 通知在线群成员。
	s.router.sendGroupNotification(r.Context(), claims.UID, groupID, "member_joined")

	data, _ := json.Marshal(map[string]string{"ok": "true"})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleGroupInvite 邀请用户入群。只有群主可以邀请他人。
// POST /group/invite?uid=alice&token=JWT&group_id=g_123&target_uid=bob
func (s *Server) HandleGroupInvite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.groupStore == nil {
		http.Error(w, "group store not available", http.StatusServiceUnavailable)
		return
	}

	claims := s.authenticateRequest(w, r)
	if claims == nil {
		return
	}

	groupID := r.FormValue("group_id")
	if groupID == "" {
		http.Error(w, "group_id required", http.StatusBadRequest)
		return
	}
	targetUID := r.FormValue("target_uid")
	if targetUID == "" {
		http.Error(w, "target_uid required", http.StatusBadRequest)
		return
	}
	if targetUID == claims.UID {
		http.Error(w, "cannot invite yourself", http.StatusBadRequest)
		return
	}

	// 只有群主可以邀请成员。
	g, err := s.groupStore.Get(r.Context(), groupID)
	if err != nil {
		http.Error(w, "group not found", http.StatusNotFound)
		return
	}
	if g.OwnerUID != claims.UID {
		http.Error(w, "only the group owner can invite members", http.StatusForbidden)
		return
	}

	if err := s.groupStore.AddMember(r.Context(), groupID, targetUID); err != nil {
		switch err {
		case ErrGroupNotFound:
			http.Error(w, "group not found", http.StatusNotFound)
		case ErrAlreadyMember:
			http.Error(w, "user is already a member", http.StatusConflict)
		default:
			log.Printf("[server] group invite error: %v", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
		}
		return
	}

	// 通过 WebSocket/TCP 通知在线群成员。
	s.router.sendGroupNotification(r.Context(), targetUID, groupID, "member_joined")

	data, _ := json.Marshal(map[string]string{"ok": "true"})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleGroupLeave 将用户移出群。如果群变为空,则删除该群。
// POST /group/leave?uid=alice&token=JWT&group_id=g_123456
func (s *Server) HandleGroupLeave(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.groupStore == nil {
		http.Error(w, "group store not available", http.StatusServiceUnavailable)
		return
	}

	claims := s.authenticateRequest(w, r)
	if claims == nil {
		return
	}

	groupID := r.FormValue("group_id")
	if groupID == "" {
		http.Error(w, "group_id required", http.StatusBadRequest)
		return
	}

	if err := s.groupStore.RemoveMember(r.Context(), groupID, claims.UID); err != nil {
		switch err {
		case ErrGroupNotFound:
			http.Error(w, "group not found", http.StatusNotFound)
		case ErrNotMember:
			http.Error(w, "not a member", http.StatusConflict)
		default:
			log.Printf("[server] group leave error: %v", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
		}
		return
	}

	// 通过 WebSocket/TCP 通知在线群成员。
	s.router.sendGroupNotification(r.Context(), claims.UID, groupID, "member_left")

	data, _ := json.Marshal(map[string]string{"ok": "true"})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleGroupKick 允许群主移除成员。
// POST /group/kick?uid=owner&token=JWT&group_id=g_123456&target_uid=bad_member
func (s *Server) HandleGroupKick(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.groupStore == nil {
		http.Error(w, "group store not available", http.StatusServiceUnavailable)
		return
	}

	claims := s.authenticateRequest(w, r)
	if claims == nil {
		return
	}

	groupID := r.FormValue("group_id")
	targetUID := r.FormValue("target_uid")
	if groupID == "" || targetUID == "" {
		http.Error(w, "group_id and target_uid required", http.StatusBadRequest)
		return
	}

	// 验证请求者是群主。
	group, err := s.groupStore.Get(r.Context(), groupID)
	if err != nil {
		http.Error(w, "group not found", http.StatusNotFound)
		return
	}
	if group.OwnerUID != claims.UID {
		http.Error(w, "only the group owner can kick members", http.StatusForbidden)
		return
	}

	if err := s.groupStore.RemoveMember(r.Context(), groupID, targetUID); err != nil {
		switch err {
		case ErrGroupNotFound:
			http.Error(w, "group not found", http.StatusNotFound)
		case ErrNotMember:
			http.Error(w, "target user is not a member", http.StatusConflict)
		default:
			log.Printf("[server] group kick error: %v", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
		}
		return
	}

	log.Printf("[server] group %s owner %s kicked %s", groupID, claims.UID, targetUID)

	// 通过 WebSocket/TCP 通知在线群成员。
	s.router.sendGroupNotification(r.Context(), claims.UID, groupID, "member_kicked")

	data, _ := json.Marshal(map[string]string{"ok": "true"})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleGroupRename 允许群主修改群名称。
// POST /group/rename?uid=owner&token=JWT&group_id=g_123456&name=New+Name
func (s *Server) HandleGroupRename(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.groupStore == nil {
		http.Error(w, "group store not available", http.StatusServiceUnavailable)
		return
	}

	claims := s.authenticateRequest(w, r)
	if claims == nil {
		return
	}

	groupID := r.FormValue("group_id")
	newName := r.FormValue("name")
	if groupID == "" || newName == "" {
		http.Error(w, "group_id and name required", http.StatusBadRequest)
		return
	}

	// 验证请求者是群主。
	group, err := s.groupStore.Get(r.Context(), groupID)
	if err != nil {
		http.Error(w, "group not found", http.StatusNotFound)
		return
	}
	if group.OwnerUID != claims.UID {
		http.Error(w, "only the group owner can rename the group", http.StatusForbidden)
		return
	}

	if err := s.groupStore.UpdateName(r.Context(), groupID, newName); err != nil {
		log.Printf("[server] group rename error: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	log.Printf("[server] group %s renamed to %q by %s", groupID, newName, claims.UID)

	// 通过 WebSocket/TCP 通知在线群成员。
	s.router.sendGroupNotification(r.Context(), claims.UID, groupID, "group_renamed")

	data, _ := json.Marshal(map[string]string{"ok": "true", "name": newName})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleGroupTransfer 允许群主将所有权转让给另一成员。
// POST /group/transfer?uid=owner&token=JWT&group_id=g_123456&to_uid=new_owner
func (s *Server) HandleGroupTransfer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.groupStore == nil {
		http.Error(w, "group store not available", http.StatusServiceUnavailable)
		return
	}

	claims := s.authenticateRequest(w, r)
	if claims == nil {
		return
	}

	groupID := r.FormValue("group_id")
	toUID := r.FormValue("to_uid")
	if groupID == "" || toUID == "" {
		http.Error(w, "group_id and to_uid required", http.StatusBadRequest)
		return
	}

	if claims.UID == toUID {
		http.Error(w, "cannot transfer ownership to yourself", http.StatusBadRequest)
		return
	}

	if err := s.groupStore.TransferOwnership(r.Context(), groupID, claims.UID, toUID); err != nil {
		switch err {
		case ErrGroupNotFound:
			http.Error(w, "group not found", http.StatusNotFound)
		case ErrNotOwner:
			http.Error(w, "only the group owner can transfer ownership", http.StatusForbidden)
		case ErrNotMember:
			http.Error(w, "target user is not a member of this group", http.StatusConflict)
		default:
			log.Printf("[server] group transfer error: %v", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
		}
		return
	}

	log.Printf("[server] group %s ownership transferred from %s to %s", groupID, claims.UID, toUID)

	// 通过 WebSocket/TCP 通知在线群成员。
	s.router.sendGroupNotification(r.Context(), claims.UID, groupID, "owner_transferred")

	data, _ := json.Marshal(map[string]string{"ok": "true", "owner_uid": toUID})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleGroupMembers 返回群的成员列表。
// GET /group/members?group_id=g_123456&uid=alice&token=JWT
func (s *Server) HandleGroupMembers(w http.ResponseWriter, r *http.Request) {
	if s.groupStore == nil {
		http.Error(w, "group store not available", http.StatusServiceUnavailable)
		return
	}

	claims := s.authenticateRequest(w, r)
	if claims == nil {
		return
	}
	_ = claims // 已认证;uid 可能用于将来的访问控制

	groupID := r.URL.Query().Get("group_id")
	if groupID == "" {
		http.Error(w, "group_id required", http.StatusBadRequest)
		return
	}

	members, err := s.groupStore.GetMembers(r.Context(), groupID)
	if err != nil {
		if err == ErrGroupNotFound {
			http.Error(w, "group not found", http.StatusNotFound)
		} else {
			http.Error(w, "internal error", http.StatusInternalServerError)
		}
		return
	}

	data, _ := json.Marshal(map[string]interface{}{
		"group_id": groupID,
		"members":  members,
	})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleGroupList 返回用户所属的所有群。
// GET /group/list?uid=alice&token=JWT
func (s *Server) HandleGroupList(w http.ResponseWriter, r *http.Request) {
	if s.groupStore == nil {
		http.Error(w, "group store not available", http.StatusServiceUnavailable)
		return
	}

	claims := s.authenticateRequest(w, r)
	if claims == nil {
		return
	}

	groups, err := s.groupStore.GetUserGroups(r.Context(), claims.UID)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// 只暴露安全字段(不暴露所有群的内部成员映射)。
	type groupInfo struct {
		ID          string `json:"id"`
		Name        string `json:"name"`
		OwnerUID    string `json:"owner_uid"`
		MemberCount int    `json:"member_count"`
		CreatedAt   int64  `json:"created_at"`
	}
	result := make([]groupInfo, 0, len(groups))
	for _, g := range groups {
		result = append(result, groupInfo{
			ID:          g.ID,
			Name:        g.Name,
			OwnerUID:    g.OwnerUID,
			MemberCount: len(g.Members),
			CreatedAt:   g.CreatedAt,
		})
	}

	data, _ := json.Marshal(map[string]interface{}{
		"groups": result,
	})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleUnreadCount 返回用户的各会话未读计数。
// GET /unread?uid=alice&token=JWT
func (s *Server) HandleUnreadCount(w http.ResponseWriter, r *http.Request) {
	claims := s.authenticateRequest(w, r)
	if claims == nil {
		return
	}

	counts := map[string]int64{}
	if s.router.unreadTracker != nil {
		counts = s.router.unreadTracker.GetCounts(r.Context(), claims.UID)
	}

	data, _ := json.Marshal(map[string]interface{}{
		"uid":    claims.UID,
		"counts": counts,
	})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// ---------- 好友管理 HTTP 处理器 ----------

// HandleFriendRequest 发送好友请求。
// POST /friend/request?uid=alice&token=JWT&to_uid=bob
func (s *Server) HandleFriendRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	claims := s.authenticateRequest(w, r)
	if claims == nil {
		return
	}

	toUID := r.FormValue("to_uid")
	if toUID == "" {
		http.Error(w, "to_uid required", http.StatusBadRequest)
		return
	}
	if claims.UID == toUID {
		http.Error(w, "cannot friend yourself", http.StatusBadRequest)
		return
	}

	if s.router.friendStore == nil {
		http.Error(w, "friend system not available", http.StatusServiceUnavailable)
		return
	}

	if err := s.router.friendStore.SendRequest(r.Context(), claims.UID, toUID); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}

	data, _ := json.Marshal(map[string]string{"status": "ok"})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleFriendAccept 接受待处理的好友请求。
// POST /friend/accept?uid=alice&token=JWT&from_uid=bob
func (s *Server) HandleFriendAccept(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	claims := s.authenticateRequest(w, r)
	if claims == nil {
		return
	}

	fromUID := r.FormValue("from_uid")
	if fromUID == "" {
		http.Error(w, "from_uid required", http.StatusBadRequest)
		return
	}

	if s.router.friendStore == nil {
		http.Error(w, "friend system not available", http.StatusServiceUnavailable)
		return
	}

	if err := s.router.friendStore.AcceptRequest(r.Context(), claims.UID, fromUID); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	data, _ := json.Marshal(map[string]string{"status": "ok"})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleFriendReject 拒绝待处理的好友请求。
// POST /friend/reject?uid=alice&token=JWT&from_uid=bob
func (s *Server) HandleFriendReject(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	claims := s.authenticateRequest(w, r)
	if claims == nil {
		return
	}

	fromUID := r.FormValue("from_uid")
	if fromUID == "" {
		http.Error(w, "from_uid required", http.StatusBadRequest)
		return
	}

	if s.router.friendStore == nil {
		http.Error(w, "friend system not available", http.StatusServiceUnavailable)
		return
	}

	if err := s.router.friendStore.RejectRequest(r.Context(), claims.UID, fromUID); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	data, _ := json.Marshal(map[string]string{"status": "ok"})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleFriendRemove 删除好友关系。
// POST /friend/remove?uid=alice&token=JWT&friend_uid=bob
func (s *Server) HandleFriendRemove(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	claims := s.authenticateRequest(w, r)
	if claims == nil {
		return
	}

	friendUID := r.FormValue("friend_uid")
	if friendUID == "" {
		http.Error(w, "friend_uid required", http.StatusBadRequest)
		return
	}

	if s.router.friendStore == nil {
		http.Error(w, "friend system not available", http.StatusServiceUnavailable)
		return
	}

	if err := s.router.friendStore.RemoveFriend(r.Context(), claims.UID, friendUID); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	data, _ := json.Marshal(map[string]string{"status": "ok"})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleFriendList 返回用户的好友和待处理请求。
// GET /friend/list?uid=alice&token=JWT
func (s *Server) HandleFriendList(w http.ResponseWriter, r *http.Request) {
	claims := s.authenticateRequest(w, r)
	if claims == nil {
		return
	}

	if s.router.friendStore == nil {
		http.Error(w, "friend system not available", http.StatusServiceUnavailable)
		return
	}

	friends, err := s.router.friendStore.GetFriends(r.Context(), claims.UID)
	if err != nil {
		log.Printf("[server] friend list error for %s: %v", claims.UID, err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	requests, err := s.router.friendStore.GetPendingRequests(r.Context(), claims.UID)
	if err != nil {
		log.Printf("[server] pending requests error for %s: %v", claims.UID, err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	data, _ := json.Marshal(map[string]interface{}{
		"uid":              claims.UID,
		"friends":          friends,
		"pending_requests": requests,
	})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleUpload 通过 multipart 表单处理文件/图片上传。
// POST /upload 字段:file、uid、token
// 返回包含 snowflake file_id 的文件元数据 JSON。
func (s *Server) HandleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.objectStore == nil {
		http.Error(w, "object store not available", http.StatusServiceUnavailable)
		return
	}

	// 限制请求体大小。
	if s.maxUpload > 0 {
		r.Body = http.MaxBytesReader(w, r.Body, s.maxUpload)
	}

	// 在解析 multipart 表单之前校验 JWT。这可以防止
	// 未认证的攻击者通过大文件上传消耗服务器内存。
	uid := r.FormValue("uid")
	token := r.FormValue("token")
	if uid == "" || token == "" {
		http.Error(w, "uid and token required", http.StatusBadRequest)
		return
	}

	claims, err := s.jwtMgr.Validate(token)
	if err != nil {
		http.Error(w, "invalid token: "+err.Error(), http.StatusUnauthorized)
		return
	}
	if claims.UID != uid {
		http.Error(w, "uid does not match token", http.StatusForbidden)
		return
	}

	// 解析 multipart 表单(最大内存等于配置的上传上限)。
	if err := r.ParseMultipartForm(s.maxUpload); err != nil {
		http.Error(w, "file too large or invalid form: "+err.Error(), http.StatusRequestEntityTooLarge)
		return
	}
	if r.MultipartForm != nil {
		defer r.MultipartForm.RemoveAll()
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "file field required: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	// 读取文件数据。
	data, err := io.ReadAll(file)
	if err != nil {
		log.Printf("[server] upload read error: %v", err)
		http.Error(w, "read error", http.StatusInternalServerError)
		return
	}

	// 拒绝空文件。
	if len(data) == 0 {
		http.Error(w, "file is empty", http.StatusBadRequest)
		return
	}

	// 从内容中检测 MIME 类型。
	mime := http.DetectContentType(data)
	fileName := header.Filename

	// 生成文件 ID。
	fileID := strconv.FormatInt(s.snow.Next(), 10)

	// 获取图片尺寸。
	width, height := ImageDimensions(data)

	// 为图片生成缩略图。如果尺寸过大则跳过
	// (解压炸弹防御 —— image.Decode 会分配 W×H×4 字节)。
	const maxImageDim = 4096
	var thumbW, thumbH int
	if IsImageMIME(mime) {
		if width > maxImageDim || height > maxImageDim {
			log.Printf("[server] thumbnail skipped for %s: image too large (%dx%d, max %d)",
				fileID, width, height, maxImageDim)
		} else {
			thumb, err := Thumbnail(data)
			if err != nil {
				log.Printf("[server] thumbnail generation for %s failed: %v (uploading original only)", fileID, err)
			} else {
				thumbKey := fileID + "_thumb"
				if err := s.objectStore.Put(r.Context(), thumbKey, thumb, "image/jpeg"); err != nil {
					log.Printf("[server] thumbnail upload for %s failed: %v", fileID, err)
				} else {
					thumbW, thumbH = ImageDimensions(thumb)
					log.Printf("[server] thumbnail %s: %dx%d (%d bytes)", fileID, thumbW, thumbH, len(thumb))
				}
			}
		}
	}

	// 将原图上传到对象存储。
	if err := s.objectStore.Put(r.Context(), fileID, data, mime); err != nil {
		log.Printf("[server] upload %s failed: %v", fileID, err)
		// 原图上传失败时清理缩略图。
		if IsImageMIME(mime) {
			_ = s.objectStore.Delete(r.Context(), fileID+"_thumb")
		}
		http.Error(w, "upload failed", http.StatusInternalServerError)
		return
	}

	log.Printf("[server] upload %s: name=%s size=%d mime=%s dims=%dx%d thumb=%dx%d",
		fileID, fileName, len(data), mime, width, height, thumbW, thumbH)

	resp, _ := json.Marshal(map[string]interface{}{
		"file_id":      fileID,
		"name":         fileName,
		"size":         len(data),
		"mime":         mime,
		"width":        width,
		"height":       height,
		"thumb_width":  thumbW,
		"thumb_height": thumbH,
	})
	w.Header().Set("Content-Type", "application/json")
	w.Write(resp)
}

// HandleSearch 对消息内容执行全文搜索。
// GET /search?uid=X&token=Y&q=hello[&peer=Z][&chat_type=1][&msg_type=1][&limit=20]
func (s *Server) HandleSearch(w http.ResponseWriter, r *http.Request) {
	uid := r.URL.Query().Get("uid")
	token := r.URL.Query().Get("token")
	if uid == "" || token == "" {
		http.Error(w, "uid and token required", http.StatusBadRequest)
		return
	}

	claims, err := s.jwtMgr.Validate(token)
	if err != nil {
		http.Error(w, "invalid token: "+err.Error(), http.StatusUnauthorized)
		return
	}
	if claims.UID != uid {
		http.Error(w, "uid does not match token", http.StatusForbidden)
		return
	}

	// 对搜索限流(开销较大的操作)。
	if rl := s.router.checkRateLimit(uid); rl {
		http.Error(w, "rate limited", http.StatusTooManyRequests)
		return
	}

	query := r.URL.Query().Get("q")
	if query == "" {
		http.Error(w, "q (query) required", http.StatusBadRequest)
		return
	}

	limit := 20
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 50 {
			limit = parsed
		}
	}

	params := &repo.SearchParams{
		UID:   uid,
		Query: query,
		Peer:  r.URL.Query().Get("peer"),
		Limit: limit,
	}
	if ct := r.URL.Query().Get("chat_type"); ct != "" {
		if v, err := strconv.Atoi(ct); err == nil {
			params.ChatType = int32(v)
		}
	}
	if mt := r.URL.Query().Get("msg_type"); mt != "" {
		if v, err := strconv.Atoi(mt); err == nil {
			params.MsgType = int32(v)
		}
	}
	if b := r.URL.Query().Get("before"); b != "" {
		if v, err := strconv.ParseInt(b, 10, 64); err == nil {
			params.Before = v
		}
	}
	if a := r.URL.Query().Get("after"); a != "" {
		if v, err := strconv.ParseInt(a, 10, 64); err == nil {
			params.After = v
		}
	}
	if c := r.URL.Query().Get("cursor"); c != "" {
		if v, err := strconv.ParseInt(c, 10, 64); err == nil {
			params.Cursor = v
		}
	}

	result, err := s.router.Search(r.Context(), params)
	if err != nil {
		log.Printf("[server] search error for %s (q=%q): %v", uid, query, err)
		http.Error(w, "search failed", http.StatusInternalServerError)
		return
	}

	if result == nil {
		result = &repo.SearchResult{}
	}

	data, _ := json.Marshal(map[string]interface{}{
		"query":       query,
		"messages":    result.Messages,
		"total":       result.Count,
		"next_cursor": result.NextCursor,
	})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleDownload 从对象存储提供文件/图片数据。
// GET /file?id={file_id}[&thumb=1][&uid=uid&token=token]
func (s *Server) HandleDownload(w http.ResponseWriter, r *http.Request) {
	if s.objectStore == nil {
		http.Error(w, "object store not available", http.StatusServiceUnavailable)
		return
	}

	fileID := r.URL.Query().Get("id")
	if fileID == "" {
		http.Error(w, "id required", http.StatusBadRequest)
		return
	}

	// 校验 JWT 令牌。
	uid := r.URL.Query().Get("uid")
	token := r.URL.Query().Get("token")
	if uid == "" || token == "" {
		http.Error(w, "uid and token required", http.StatusBadRequest)
		return
	}
	claims, err := s.jwtMgr.Validate(token)
	if err != nil {
		http.Error(w, "invalid token: "+err.Error(), http.StatusUnauthorized)
		return
	}
	if claims.UID != uid {
		http.Error(w, "uid does not match token", http.StatusForbidden)
		return
	}

	// 如果请求了缩略图则提供缩略图。
	key := fileID
	if r.URL.Query().Get("thumb") == "1" {
		key = fileID + "_thumb"
	}

	data, contentType, err := s.objectStore.Get(r.Context(), key)
	if err != nil {
		log.Printf("[server] download %s failed: %v", key, err)
		http.Error(w, "file not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Write(data)
}

// GnetHandler 返回 gnet 处理器(gnet 未启用时为 nil)。
func (s *Server) GnetHandler() *GnetHandler {
	return s.gnetHandler
}

// HealthChecker 是检查依赖健康状态的函数。
// 健康时返回 nil,否则返回描述问题的错误。
type HealthChecker func(ctx context.Context) error

// healthCheckers 保存已注册的依赖健康检查。
var healthCheckers = map[string]HealthChecker{}

// RegisterHealthCheck 注册一个具名的健康检查函数。
func RegisterHealthCheck(name string, check HealthChecker) {
	healthCheckers[name] = check
}

// HandleHealth 是 GET /health 的 HTTP 处理器。
// 返回增强状态,包括依赖状态和内存指标。
func (s *Server) HandleHealth(w http.ResponseWriter, r *http.Request) {
	deps := make(map[string]string, len(healthCheckers))
	allHealthy := true
	for name, check := range healthCheckers {
		if err := check(r.Context()); err != nil {
			deps[name] = "unhealthy: " + err.Error()
			allHealthy = false
		} else {
			deps[name] = "ok"
		}
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// 一次性构建响应,然后根据健康状况设置状态。
	status := "ok"
	if !allHealthy {
		status = "degraded"
	}
	data, _ := json.Marshal(map[string]interface{}{
		"status":       status,
		"connections":  s.clients.Count(r.Context()),
		"dependencies": deps,
		"memory": map[string]interface{}{
			"alloc_mb":   int(m.Alloc / 1024 / 1024),
			"goroutines": runtime.NumGoroutine(),
		},
	})

	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// ---------- 管理员处理器 ----------

// HandleAdminStats 返回管理面板所需的系统统计汇总。
// GET /admin/stats?uid=admin&token=JWT
func (s *Server) HandleAdminStats(w http.ResponseWriter, r *http.Request) {
	claims := s.adminAuth(w, r)
	if claims == nil {
		return
	}
	_ = claims

	// 复用现有的健康检查基础设施。
	deps := make(map[string]string, len(healthCheckers))
	allHealthy := true
	for name, check := range healthCheckers {
		if err := check(r.Context()); err != nil {
			deps[name] = "unhealthy: " + err.Error()
			allHealthy = false
		} else {
			deps[name] = "ok"
		}
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	status := "ok"
	if !allHealthy {
		status = "degraded"
	}

	onlineUsers := s.clients.Count(r.Context())
	totalUsers := 0
	totalMessages := 0
	if s.userStore != nil {
		totalUsers, _ = s.userStore.CountUsers(r.Context())
	}
	if s.msgStore != nil {
		totalMessages, _ = s.msgStore.CountMessages(r.Context())
	}

	data, _ := json.Marshal(map[string]interface{}{
		"status":         status,
		"online_users":   onlineUsers,
		"total_users":    totalUsers,
		"total_messages": totalMessages,
		"dependencies":   deps,
		"memory": map[string]interface{}{
			"alloc_mb":   int(m.Alloc / 1024 / 1024),
			"goroutines": runtime.NumGoroutine(),
		},
	})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleAdminUsers 返回所有用户的分页列表。
// GET /admin/users?uid=admin&token=JWT&offset=0&limit=50
func (s *Server) HandleAdminUsers(w http.ResponseWriter, r *http.Request) {
	claims := s.adminAuth(w, r)
	if claims == nil {
		return
	}
	_ = claims

	if s.userStore == nil {
		http.Error(w, "user store not available", http.StatusServiceUnavailable)
		return
	}

	offset := 0
	limit := 50
	if o := r.URL.Query().Get("offset"); o != "" {
		if v, err := strconv.Atoi(o); err == nil && v >= 0 {
			offset = v
		}
	}
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 && v <= 200 {
			limit = v
		}
	}

	users, total, err := s.userStore.ListUsers(r.Context(), offset, limit)
	if err != nil {
		log.Printf("[server] admin list users error: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	data, _ := json.Marshal(map[string]interface{}{
		"users":  users,
		"total":  total,
		"offset": offset,
		"limit":  limit,
	})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleAdminUserDelete 按 UID 删除用户。
// POST /admin/users/delete 表单字段:uid(管理员)、token、target_uid
func (s *Server) HandleAdminUserDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	claims := s.adminAuth(w, r)
	if claims == nil {
		return
	}

	if s.userStore == nil {
		http.Error(w, "user store not available", http.StatusServiceUnavailable)
		return
	}

	targetUID := r.FormValue("target_uid")
	if targetUID == "" {
		http.Error(w, "target_uid required", http.StatusBadRequest)
		return
	}
	if targetUID == claims.UID {
		http.Error(w, "cannot delete yourself", http.StatusBadRequest)
		return
	}

	if err := s.userStore.DeleteUser(r.Context(), targetUID); err != nil {
		log.Printf("[server] admin delete user %s error: %v", targetUID, err)
		http.Error(w, "delete failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	data, _ := json.Marshal(map[string]string{"status": "ok"})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleAdminMessages 返回最近的消息,用于内容审核。
// GET /admin/messages?uid=admin&token=JWT&limit=50&before={ts}
func (s *Server) HandleAdminMessages(w http.ResponseWriter, r *http.Request) {
	claims := s.adminAuth(w, r)
	if claims == nil {
		return
	}
	_ = claims

	if s.msgStore == nil {
		http.Error(w, "message store not available", http.StatusServiceUnavailable)
		return
	}

	before := int64(0)
	limit := 50
	if b := r.URL.Query().Get("before"); b != "" {
		if v, err := strconv.ParseInt(b, 10, 64); err == nil {
			before = v
		}
	}
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 && v <= 100 {
			limit = v
		}
	}

	msgs, err := s.msgStore.BrowseMessages(r.Context(), before, limit)
	if err != nil {
		log.Printf("[server] admin browse messages error: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	data, _ := json.Marshal(map[string]interface{}{
		"messages": msgs,
		"limit":    limit,
	})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleAdminMessageDelete 按 ID 删除消息。
// POST /admin/messages/delete 表单字段:uid(管理员)、token、msg_id
func (s *Server) HandleAdminMessageDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	claims := s.adminAuth(w, r)
	if claims == nil {
		return
	}
	_ = claims

	if s.msgStore == nil {
		http.Error(w, "message store not available", http.StatusServiceUnavailable)
		return
	}

	msgIDStr := r.FormValue("msg_id")
	if msgIDStr == "" {
		http.Error(w, "msg_id required", http.StatusBadRequest)
		return
	}
	msgID, err := strconv.ParseInt(msgIDStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid msg_id", http.StatusBadRequest)
		return
	}

	if err := s.msgStore.DeleteMessage(r.Context(), msgID); err != nil {
		log.Printf("[server] admin delete message %d error: %v", msgID, err)
		http.Error(w, "delete failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	data, _ := json.Marshal(map[string]string{"status": "ok"})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleWS 定义在 server_ws.go(WebSocket 专用代码)。

// StartGNet 初始化 gnet 处理器并启动 TCP 服务器。
func (s *Server) StartGNet(cfg *configs.Config) error {
	gnetCfg := cfg.Gateway.GNet
	numLoops := gnetCfg.NumEventLoops
	if numLoops <= 0 {
		numLoops = 0 // 为 0 时 gnet 使用 runtime.NumCPU()
	}
	workerPoolSize := gnetCfg.WorkerPoolSize
	if workerPoolSize <= 0 {
		workerPoolSize = 0 // 将在 NewGnetHandler 中取默认值
	}

	ctx := context.Background()
	s.gnetHandler = NewGnetHandler(
		ctx,
		s.router,
		s.clients,
		s.jwtMgr,
		cfg.Gateway.Conn.SendBufSize,
		cfg.Gateway.Conn.MaxMsgSize,
		time.Duration(cfg.Gateway.Heartbeat)*time.Duration(cfg.Gateway.HeartbeatFail),
		workerPoolSize,
	)

	log.Printf("[server] gnet TCP server starting on %s (event-loops=%d)", cfg.Gateway.TCPAddr, numLoops)
	return gnet.Run(s.gnetHandler, fmt.Sprintf("tcp://%s", cfg.Gateway.TCPAddr),
		gnet.WithNumEventLoop(numLoops),
	)
}
