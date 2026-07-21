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

// Server is the gateway server supporting WebSocket and/or gnet TCP transports.
type Server struct {
	clients    ClientRegistry
	router     *Router
	jwtMgr     *jwt.Manager
	userStore  repo.UserStore   // nil when MySQL disabled
	msgStore   repo.MessageStore // nil when MySQL disabled (admin APIs)
	groupStore GroupStore       // nil when group chat disabled
	authCfg    configs.AuthConfig
	adminUIDs  map[string]bool // UIDs with admin privileges (from config)
	heartbeat  time.Duration
	heartFail  int
	connCfg    configs.GatewayConnConfig

	snow        *snowflake.Generator // for generating file IDs
	objectStore ObjectStore          // nil when file upload disabled
	maxUpload   int64                // max upload size in bytes

	// WebSocket
	upgrader websocket.Upgrader

	// gnet
	gnetHandler *GnetHandler
}

// NewServer creates a gateway Server.
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

// buildOriginChecker returns an Origin check function.
// An empty allowed list means allow all (development mode).
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
			return true // same-origin requests have no Origin header
		}
		return allowedSet[origin]
	}
}

// Recovery wraps an http.Handler with panic recovery.
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

// HandleLogin is the HTTP endpoint for user login (returns JWT).
// In dev mode (default), uid+username is sufficient.
// In production mode, uid+password is required and validated against the user store.
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

	// Password auth path.
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
		// Production mode requires password.
		http.Error(w, "password required", http.StatusUnauthorized)
		return
	}

	// Dev mode: allow role override via form field for testing admin features.
	if s.authCfg.DevMode && r.FormValue("role") == "admin" {
		role = "admin"
	}

	// Bootstrap admin UIDs from config (highest priority).
	if s.adminUIDs[uid] {
		role = "admin"
	}

	// Dev mode fallback: no password → use provided username (or uid).
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

// HandleRegister is the HTTP endpoint for user registration.
// Requires uid, username, and password. Stores a bcrypt hash of the password.
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
		// MySQL error 1062 = duplicate entry; other errors are internal.
		if strings.Contains(err.Error(), "Duplicate entry") || strings.Contains(err.Error(), "Error 1062") {
			http.Error(w, "uid already exists", http.StatusConflict)
		} else {
			http.Error(w, "registration failed", http.StatusInternalServerError)
		}
		return
	}

	token, err := s.jwtMgr.Generate(uid, username, "user")
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

// HandleChangePassword validates the current password and updates to a new one.
// POST /change-password  (body: uid, token, old_password, new_password)
func (s *Server) HandleChangePassword(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Validate JWT — must be authenticated to change password.
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

// authenticateRequest extracts uid+token from form (POST) or query (GET) parameters,
// validates the JWT, and returns the authenticated claims. On failure, it writes an
// HTTP error response and returns nil. Callers must check for nil before proceeding.
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

// adminAuth authenticates the request and checks that the user is an admin.
// Returns nil (with an HTTP error already written) on failure.
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

// HandleOnlineUsers returns currently online users.
func (s *Server) HandleOnlineUsers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	users := s.clients.OnlineUsers(ctx) // single call, reused below
	data, _ := json.Marshal(map[string]interface{}{
		"count": len(users),
		"users": users,
	})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleGroupCreate creates a new chat group. The creator is auto-added as a member.
// Optional 'members' parameter (comma-separated UIDs) adds initial members.
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

	// Parse initial members (comma-separated UIDs).
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

	// Notify invited members (skip self).
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

// HandleGroupJoin adds a user to a group.
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

	// Notify online group members via WebSocket/TCP.
	s.router.sendGroupNotification(r.Context(), claims.UID, groupID, "member_joined")

	data, _ := json.Marshal(map[string]string{"ok": "true"})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleGroupInvite invites a user to a group. Only the group owner can invite others.
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

	// Only the group owner can invite members.
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

	// Notify online group members via WebSocket/TCP.
	s.router.sendGroupNotification(r.Context(), targetUID, groupID, "member_joined")

	data, _ := json.Marshal(map[string]string{"ok": "true"})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleGroupLeave removes a user from a group. If the group becomes empty, it is deleted.
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

	// Notify online group members via WebSocket/TCP.
	s.router.sendGroupNotification(r.Context(), claims.UID, groupID, "member_left")

	data, _ := json.Marshal(map[string]string{"ok": "true"})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleGroupKick allows the group owner to remove a member.
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

	// Verify requester is the group owner.
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

	// Notify online group members via WebSocket/TCP.
	s.router.sendGroupNotification(r.Context(), claims.UID, groupID, "member_kicked")

	data, _ := json.Marshal(map[string]string{"ok": "true"})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleGroupRename allows the group owner to change the group name.
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

	// Verify requester is the group owner.
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

	// Notify online group members via WebSocket/TCP.
	s.router.sendGroupNotification(r.Context(), claims.UID, groupID, "group_renamed")

	data, _ := json.Marshal(map[string]string{"ok": "true", "name": newName})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleGroupTransfer allows the group owner to transfer ownership to another member.
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

	// Notify online group members via WebSocket/TCP.
	s.router.sendGroupNotification(r.Context(), claims.UID, groupID, "owner_transferred")

	data, _ := json.Marshal(map[string]string{"ok": "true", "owner_uid": toUID})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleGroupMembers returns the member list for a group.
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
	_ = claims // authenticated; uid may be used for future access control

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

// HandleGroupList returns all groups the user is a member of.
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

	// Only expose safe fields (not internal member maps for all groups).
	type groupInfo struct {
		ID          string   `json:"id"`
		Name        string   `json:"name"`
		OwnerUID    string   `json:"owner_uid"`
		MemberCount int      `json:"member_count"`
		CreatedAt   int64    `json:"created_at"`
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

// HandleUnreadCount returns per-peer unread counts for a user.
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

// ---------- Friend management HTTP handlers ----------

// HandleFriendRequest sends a friend request.
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

// HandleFriendAccept accepts a pending friend request.
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

// HandleFriendReject rejects a pending friend request.
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

// HandleFriendRemove removes a friend relationship.
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

// HandleFriendList returns a user's friends and pending requests.
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
		"uid":             claims.UID,
		"friends":         friends,
		"pending_requests": requests,
	})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleUpload processes file/image uploads via multipart form.
// POST /upload with fields: file, uid, token
// Returns JSON with file metadata including a snowflake file_id.
func (s *Server) HandleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.objectStore == nil {
		http.Error(w, "object store not available", http.StatusServiceUnavailable)
		return
	}

	// Limit request body size.
	if s.maxUpload > 0 {
		r.Body = http.MaxBytesReader(w, r.Body, s.maxUpload)
	}

	// Validate JWT BEFORE parsing the multipart form. This prevents
	// unauthenticated attackers from consuming server memory via large uploads.
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

	// Parse multipart form (max memory equal to configured upload limit).
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

	// Read the file data.
	data, err := io.ReadAll(file)
	if err != nil {
		log.Printf("[server] upload read error: %v", err)
		http.Error(w, "read error", http.StatusInternalServerError)
		return
	}

	// Reject empty files.
	if len(data) == 0 {
		http.Error(w, "file is empty", http.StatusBadRequest)
		return
	}

	// Detect MIME type from content.
	mime := http.DetectContentType(data)
	fileName := header.Filename

	// Generate file ID.
	fileID := strconv.FormatInt(s.snow.Next(), 10)

	// Get image dimensions.
	width, height := ImageDimensions(data)

	// Generate thumbnail for images. Skip if dimensions are unreasonably
	// large (decompression bomb defense — image.Decode allocates W×H×4 bytes).
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

	// Upload original to object store.
	if err := s.objectStore.Put(r.Context(), fileID, data, mime); err != nil {
		log.Printf("[server] upload %s failed: %v", fileID, err)
		// Clean up thumbnail if original upload fails.
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

// HandleSearch performs fulltext search on message content.
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

	// Rate limit search (expensive operation).
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

// HandleDownload serves file/image data from the object store.
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

	// Validate JWT token.
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

	// Serve thumbnail if requested.
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

// GnetHandler returns the gnet handler (nil if gnet is not active).
func (s *Server) GnetHandler() *GnetHandler {
	return s.gnetHandler
}

// HealthChecker is a function that checks a dependency's health.
// It returns nil if healthy, or an error describing the issue.
type HealthChecker func(ctx context.Context) error

// healthCheckers stores registered dependency health checks.
var healthCheckers = map[string]HealthChecker{}

// RegisterHealthCheck registers a named health check function.
func RegisterHealthCheck(name string, check HealthChecker) {
	healthCheckers[name] = check
}

// HandleHealth is the HTTP handler for GET /health.
// Returns enhanced status including dependency states and memory metrics.
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

	// Build response once, then set status based on health.
	status := "ok"
	if !allHealthy {
		status = "degraded"
	}
	data, _ := json.Marshal(map[string]interface{}{
		"status":      status,
		"connections": s.clients.Count(r.Context()),
		"dependencies": deps,
		"memory": map[string]interface{}{
			"alloc_mb":   int(m.Alloc / 1024 / 1024),
			"goroutines": runtime.NumGoroutine(),
		},
	})

	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// ---------- Admin handlers ----------

// HandleAdminStats returns aggregated system statistics for the admin dashboard.
// GET /admin/stats?uid=admin&token=JWT
func (s *Server) HandleAdminStats(w http.ResponseWriter, r *http.Request) {
	claims := s.adminAuth(w, r)
	if claims == nil {
		return
	}
	_ = claims

	// Reuse existing health-check infrastructure.
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

// HandleAdminUsers returns a paginated list of all users.
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

// HandleAdminUserDelete deletes a user by UID.
// POST /admin/users/delete with form fields: uid (admin), token, target_uid
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

// HandleAdminMessages returns recent messages for content moderation.
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

// HandleAdminMessageDelete deletes a message by ID.
// POST /admin/messages/delete with form fields: uid (admin), token, msg_id
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

// HandleWS is defined in server_ws.go (WebSocket-specific code).

// StartGNet initializes the gnet handler and starts the TCP server.
func (s *Server) StartGNet(cfg *configs.Config) error {
	gnetCfg := cfg.Gateway.GNet
	numLoops := gnetCfg.NumEventLoops
	if numLoops <= 0 {
		numLoops = 0 // gnet uses runtime.NumCPU() when 0
	}
	workerPoolSize := gnetCfg.WorkerPoolSize
	if workerPoolSize <= 0 {
		workerPoolSize = 0 // will default in NewGnetHandler
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
