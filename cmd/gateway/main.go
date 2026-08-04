package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof" // 在 DefaultServeMux 上注册 pprof 处理器
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/im/configs"
	"github.com/im/internal/gateway"
	"github.com/im/internal/mq"
	"github.com/im/internal/pkg/jwt"
	"github.com/im/internal/pkg/snowflake"
	"github.com/im/internal/repo"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

// App 持有已初始化的应用组件。
type App struct {
	Hub           gateway.ClientRegistry
	Server        *gateway.Server
	Config        *configs.Config
	redisClient   *redis.Client           // Redis 禁用或不可用时为 nil
	mysqlStore    *repo.MySQLStore        // MySQL 禁用时为 nil
	kafkaProducer *mq.Producer            // Kafka 禁用时为 nil
	logicClient   *gateway.LogicClient    // Logic gRPC 禁用时为 nil
	grpcServer    *grpc.Server            // gRPC 禁用（单节点模式）时为 nil
	grpcForwarder *gateway.GrpcForwarder  // 多网关禁用时为 nil
	clusterMgr    *gateway.ClusterManager // 多网关禁用时为 nil
	pprofServer   *http.Server            // pprof 禁用时为 nil
	router        *gateway.Router         // 用于清理（去重缓存、限流器）
}

// NewApp 根据配置初始化所有应用组件。
func NewApp(cfg *configs.Config) (*App, error) {
	snow, err := snowflake.New(cfg.Snow.WorkerID)
	if err != nil {
		return nil, fmt.Errorf("snowflake: %w", err)
	}
	jwtMgr := jwt.New(cfg.JWT.Secret, time.Duration(cfg.JWT.Expiration))
	hub := gateway.NewHub(cfg.Gateway.Conn.OfflineMaxSize)

	// 组装离线存储：先尝试 Redis，失败则回退到内存 Hub。
	var offlineStore gateway.OfflineStore = hub
	var rdb *redis.Client
	if cfg.Gateway.Redis.Addr != "" {
		rdb = redis.NewClient(&redis.Options{
			Addr:     cfg.Gateway.Redis.Addr,
			Password: cfg.Gateway.Redis.Password,
			DB:       cfg.Gateway.Redis.DB,
		})
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := rdb.Ping(ctx).Err(); err != nil {
			log.Printf("[main] Redis ping failed (%v), falling back to in-memory offline store", err)
			rdb.Close()
			rdb = nil
		} else {
			log.Printf("[main] Redis connected at %s, using Redis offline store", cfg.Gateway.Redis.Addr)
			rs := gateway.NewRedisStore(rdb, cfg.Gateway.Conn.OfflineMaxSize)
			rs.WithFallback(hub)
			offlineStore = rs
		}
	} else {
		log.Printf("[main] Redis disabled (empty addr), using in-memory offline store")
	}

	// 如果启用则初始化 MySQL 存储。
	var mysqlStore *repo.MySQLStore
	if cfg.Gateway.MySQL.Enabled {
		var err error
		mysqlStore, err = repo.NewMySQLStore(cfg.Gateway.MySQL.DSN)
		if err != nil {
			log.Printf("[main] MySQL init failed (%v), continuing without persistence", err)
		} else {
			log.Printf("[main] MySQL connected at %s", cfg.Gateway.MySQL.DSN)
		}
		// 从配置中引导初始化管理员用户（幂等 —— 每次启动都会执行）。
		if mysqlStore != nil && len(cfg.AdminUIDs) > 0 {
			for _, adminUID := range cfg.AdminUIDs {
				if err := mysqlStore.UpdateUserRole(context.Background(), adminUID, "admin"); err != nil {
					log.Printf("[main] WARNING: could not promote admin %s: %v", adminUID, err)
				} else {
					log.Printf("[main] admin user %s ensured", adminUID)
				}
			}
		}
	}

	var msgStore repo.MessageStore
	var userStore repo.UserStore
	if mysqlStore != nil {
		msgStore = mysqlStore
		userStore = mysqlStore
	}

	// 如果启用则初始化 Kafka 生产者。
	var kafkaProducer *mq.Producer
	if cfg.Gateway.Kafka.Enabled && len(cfg.Gateway.Kafka.Brokers) > 0 {
		kafkaCfg := mq.ProducerConfig{
			Brokers: cfg.Gateway.Kafka.Brokers,
			Topic:   cfg.Gateway.Kafka.Topic,
		}
		if kafkaCfg.Topic == "" {
			kafkaCfg.Topic = "im.message.persist"
		}
		var err error
		kafkaProducer, err = mq.NewProducer(kafkaCfg)
		if err != nil {
			log.Printf("[main] Kafka producer init failed (%v), falling back to direct MySQL", err)
			kafkaProducer = nil
		}
	}

	// 如果配置了则初始化 Logic gRPC 客户端。
	logicClient, err := gateway.NewLogicClient(cfg.Gateway.LogicGateway.Addr)
	if err != nil {
		log.Printf("[main] Logic gRPC client init failed (%v), will use local MessageStore", err)
	}

	// 根据网关配置构建路由配置。
	routerCfg := gateway.RouterConfig{
		DedupTTL:            time.Duration(cfg.Gateway.DedupTTL),
		PersistConcurrency:  cfg.Gateway.PersistConcurrency,
		RecallWindowMs:      cfg.Gateway.RecallWindowMs,
		HistoryDefaultLimit: cfg.Gateway.HistoryDefaultLimit,
		SearchDefaultLimit:  cfg.Gateway.SearchDefaultLimit,
		RateLimitCleanup:    time.Duration(cfg.Gateway.RateLimit.CleanupInterval),
	}
	router := gateway.NewRouter(hub, offlineStore, snow, msgStore, routerCfg)
	if kafkaProducer != nil {
		router.SetKafkaProducer(kafkaProducer)
	}
	if logicClient != nil {
		router.SetLogicClient(logicClient)
	}

	// 根据配置组装限流。
	if cfg.Gateway.RateLimit.Enabled {
		router.SetRateLimit(cfg.Gateway.RateLimit.Rate, cfg.Gateway.RateLimit.Burst)
	}

	// 在 hub 上设置最大连接数限制。
	if cfg.Stability.MaxConnections > 0 {
		hub.SetMaxConnections(cfg.Stability.MaxConnections)
	}

	// --- 多网关水平扩展 ---
	var grpcForwarder *gateway.GrpcForwarder
	var grpcSrv *grpc.Server
	var clusterMgr *gateway.ClusterManager

	if cfg.Gateway.Grpc.Addr != "" && cfg.Gateway.Grpc.NodeID != "" {
		// 构建包含所有对端节点与自身的哈希环。
		hr := gateway.NewHashRing(150) // 每个物理节点 150 个虚拟节点
		for nodeID := range cfg.Gateway.Grpc.PeerAddrs {
			hr.Add(nodeID)
		}
		hr.Add(cfg.Gateway.Grpc.NodeID) // 包含自身

		// 创建用于跨节点消息投递的 gRPC 转发器。
		grpcForwarder = gateway.NewGrpcForwarder(
			hr,
			cfg.Gateway.Grpc.NodeID,
			cfg.Gateway.Grpc.PeerAddrs,
			time.Duration(cfg.Gateway.Grpc.ForwardDialTimeout),
			time.Duration(cfg.Gateway.Grpc.ForwardRPCTimeout),
		)

		// 注入到路由中。
		router.SetHashRing(hr)
		router.SetForwarder(grpcForwarder)
		router.SetThisNodeID(cfg.Gateway.Grpc.NodeID)

		// 创建处理传入转发消息的 gRPC 服务器处理器。
		grpcHandler := gateway.NewGrpcGatewayServer(hub, offlineStore, cfg.Gateway.Grpc.NodeID)
		grpcSrv, err = gateway.StartGrpcServer(cfg.Gateway.Grpc.Addr, grpcHandler)
		if err != nil {
			log.Printf("[main] gRPC server start failed (%v), continuing without multi-gateway", err)
			grpcSrv = nil
		}

		// 设置动态集群（健康检查 + 可选的 Redis 服务发现）。
		if grpcSrv != nil {
			clusterCfg := gateway.ClusterConfig{
				ThisNodeID: cfg.Gateway.Grpc.NodeID,
				ThisAddr:   cfg.Gateway.Grpc.Addr,
			}
			if cfg.Gateway.Grpc.Discovery.HealthInterval > 0 {
				clusterCfg.HealthInterval = time.Duration(cfg.Gateway.Grpc.Discovery.HealthInterval)
			}

			// 当配置了 Redis 服务发现且 Redis 可用时启用。
			if cfg.Gateway.Grpc.Discovery.Mode == "redis" && rdb != nil {
				clusterCfg.Redis = rdb
				if cfg.Gateway.Grpc.Discovery.RedisKey != "" {
					clusterCfg.RedisPrefix = cfg.Gateway.Grpc.Discovery.RedisKey
				}
				if cfg.Gateway.Grpc.Discovery.TTL > 0 {
					clusterCfg.TTL = time.Duration(cfg.Gateway.Grpc.Discovery.TTL)
				}
				log.Printf("[main] cluster discovery mode: redis (key=%s ttl=%s)",
					clusterCfg.RedisPrefix, clusterCfg.TTL)
			} else {
				log.Printf("[main] cluster discovery mode: static (peer_addrs)")
			}

			clusterMgr = gateway.NewClusterManager(hr, grpcForwarder, clusterCfg)
			clusterMgr.Start(context.Background())
		}

		log.Printf("[main] multi-gateway mode enabled: node=%s gRPC=%s peers=%d",
			cfg.Gateway.Grpc.NodeID, cfg.Gateway.Grpc.Addr, len(cfg.Gateway.Grpc.PeerAddrs))
	}

	// --- 群聊支持 ---
	var groupStore gateway.GroupStore
	if mysqlStore != nil {
		groupStore = gateway.NewMySQLGroupStore(mysqlStore.DB(), snow)
		log.Printf("[main] using MySQL group store")
	} else {
		groupStore = gateway.NewInMemoryGroupStore(snow)
		log.Printf("[main] using in-memory group store")
	}
	router.SetGroupStore(groupStore)

	// --- 去重 Redis 持久化 ---
	if rdb != nil {
		router.SetDedupRedis(rdb)
		log.Printf("[main] dedup: Redis durability enabled")
	}

	// --- 好友关系管理 ---
	if mysqlStore != nil {
		router.SetFriendStore(mysqlStore)
		log.Printf("[main] friend system enabled (MySQL)")
	} else {
		log.Printf("[main] friend system disabled (no MySQL)")
	}

	// --- 已读/未读回执跟踪 ---
	unreadTracker := gateway.NewInMemoryUnreadTracker()
	router.SetUnreadTracker(unreadTracker)

	// --- 文件/图片消息的对象存储 ---
	var objectStore gateway.ObjectStore
	if cfg.Gateway.ObjectStorage.Enabled {
		minioStore, err := gateway.NewMinioStore(context.Background(), cfg.Gateway.ObjectStorage)
		if err != nil {
			log.Printf("[main] MinIO init failed (%v), falling back to in-memory object store", err)
			objectStore = gateway.NewInMemoryObjectStore()
		} else {
			objectStore = minioStore
		}
	} else {
		log.Printf("[main] Object storage disabled (enabled=false), using in-memory object store")
		objectStore = gateway.NewInMemoryObjectStore()
	}

	server := gateway.NewServer(
		hub, router, jwtMgr,
		userStore, msgStore, groupStore, cfg.Gateway.Auth,
		time.Duration(cfg.Gateway.Heartbeat),
		cfg.Gateway.HeartbeatFail,
		cfg.Gateway.Conn,
		cfg.Gateway.CheckOrigin,
		snow, objectStore, cfg.Gateway.ObjectStorage.MaxUpload,
		cfg.AdminUIDs,
	)

	// 为 /health 端点注册依赖健康检查。
	if rdb != nil {
		gateway.RegisterHealthCheck("redis", func(ctx context.Context) error {
			return rdb.Ping(ctx).Err()
		})
	}
	if mysqlStore != nil {
		gateway.RegisterHealthCheck("mysql", func(ctx context.Context) error {
			return mysqlStore.Ping(ctx)
		})
	}
	if kafkaProducer != nil {
		// Kafka 生产者是可选的；健康检查报告为已启用。
		// kafka-go 的 Writer 没有 Ping 方法，因此报告为已启用，
		// 错误通过 Publish 日志暴露。
		gateway.RegisterHealthCheck("kafka", func(ctx context.Context) error {
			return nil // 已启用 = 视为健康；Publish 失败会被记录日志
		})
	}
	if ms, ok := objectStore.(*gateway.MinioStore); ok {
		gateway.RegisterHealthCheck("minio", func(ctx context.Context) error {
			return ms.Ping(ctx)
		})
	}

	return &App{
		Hub:           hub,
		Server:        server,
		Config:        cfg,
		redisClient:   rdb,
		mysqlStore:    mysqlStore,
		kafkaProducer: kafkaProducer,
		logicClient:   logicClient,
		grpcServer:    grpcSrv,
		grpcForwarder: grpcForwarder,
		clusterMgr:    clusterMgr,
		router:        router,
	}, nil
}

// Close 按依赖顺序清理应用资源。
func (app *App) Close() {
	// 1. 停止后台 goroutine（DedupCache、RateLimiter）。
	if app.router != nil {
		app.router.Stop()
	}

	// 2. 停止集群管理器（从 Redis 注销，停止健康检查）。
	if app.clusterMgr != nil {
		app.clusterMgr.Stop()
	}

	// 3. 停止 gRPC 转发器（对端连接）。
	if app.grpcForwarder != nil {
		if err := app.grpcForwarder.Close(); err != nil {
			log.Printf("[main] gRPC forwarder close error: %v", err)
		}
	}

	// 4. 停止 gRPC 服务器。
	if app.grpcServer != nil {
		app.grpcServer.GracefulStop()
		log.Printf("[main] gRPC server stopped")
	}

	// 5. 停止 pprof 服务器。
	if app.pprofServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		app.pprofServer.Shutdown(ctx)
	}

	// 6. Kafka 生产者。
	if app.kafkaProducer != nil {
		if err := app.kafkaProducer.Close(); err != nil {
			log.Printf("[main] Kafka producer close error: %v", err)
		}
	}

	// 7. Logic gRPC 客户端。
	if app.logicClient != nil {
		if err := app.logicClient.Close(); err != nil {
			log.Printf("[main] Logic gRPC client close error: %v", err)
		}
	}

	// 8. Redis。
	if app.redisClient != nil {
		if err := app.redisClient.Close(); err != nil {
			log.Printf("[main] Redis close error: %v", err)
		}
	}

	// 9. MySQL（最后 —— 其他组件可能还在使用它）。
	if app.mysqlStore != nil {
		if err := app.mysqlStore.Close(); err != nil {
			log.Printf("[main] MySQL close error: %v", err)
		}
	}
}

// Run 启动 HTTP 服务器（可选地启动 gnet TCP 服务器），阻塞直到 ctx 被取消。
func (app *App) Run(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/login", app.Server.HandleLogin)
	mux.HandleFunc("/register", app.Server.HandleRegister)
	mux.HandleFunc("/online", app.Server.HandleOnlineUsers)

	// 群聊管理端点。
	mux.HandleFunc("/group/create", app.Server.HandleGroupCreate)
	mux.HandleFunc("/group/join", app.Server.HandleGroupJoin)
	mux.HandleFunc("/group/invite", app.Server.HandleGroupInvite)
	mux.HandleFunc("/group/leave", app.Server.HandleGroupLeave)
	mux.HandleFunc("/group/kick", app.Server.HandleGroupKick)
	mux.HandleFunc("/group/rename", app.Server.HandleGroupRename)
	mux.HandleFunc("/group/transfer", app.Server.HandleGroupTransfer)
	mux.HandleFunc("/group/members", app.Server.HandleGroupMembers)
	mux.HandleFunc("/group/list", app.Server.HandleGroupList)

	// 未读数量查询。
	mux.HandleFunc("/unread", app.Server.HandleUnreadCount)

	// 文件/图片上传与下载。
	mux.HandleFunc("/upload", app.Server.HandleUpload)
	mux.HandleFunc("/file", app.Server.HandleDownload)

	// 消息全文搜索。
	mux.HandleFunc("/search", app.Server.HandleSearch)

	// 修改密码。
	mux.HandleFunc("/change-password", app.Server.HandleChangePassword)

	// 好友管理。
	mux.HandleFunc("/friend/request", app.Server.HandleFriendRequest)
	mux.HandleFunc("/friend/accept", app.Server.HandleFriendAccept)
	mux.HandleFunc("/friend/reject", app.Server.HandleFriendReject)
	mux.HandleFunc("/friend/remove", app.Server.HandleFriendRemove)
	mux.HandleFunc("/friend/list", app.Server.HandleFriendList)

	transport := app.Config.Gateway.Transport
	if transport == "" {
		transport = "websocket" // 为旧配置保持向后兼容
	}

	// 仅当 WebSocket 传输启用时挂载 /ws。
	if transport == "websocket" || transport == "both" {
		mux.HandleFunc("/ws", app.Server.HandleWS)
	}

	mux.HandleFunc("/health", app.Server.HandleHealth)

	// 管理员监控与管理端点。
	mux.HandleFunc("/admin/stats", app.Server.HandleAdminStats)
	mux.HandleFunc("/admin/users", app.Server.HandleAdminUsers)
	mux.HandleFunc("/admin/users/delete", app.Server.HandleAdminUserDelete)
	mux.HandleFunc("/admin/messages", app.Server.HandleAdminMessages)
	mux.HandleFunc("/admin/messages/delete", app.Server.HandleAdminMessageDelete)

	// 使用 panic 恢复中间件包装
	handler := gateway.Recovery(mux)

	// 如果启用则启动 pprof 服务器。
	st := app.Config.Stability
	if st.PprofEnabled && st.PprofAddr != "" {
		pprofSrv := &http.Server{
			Addr:    st.PprofAddr,
			Handler: http.DefaultServeMux, // net/http/pprof 注册在 DefaultServeMux 上
		}
		app.pprofServer = pprofSrv
		go func() {
			log.Printf("[main] pprof server listening on %s", st.PprofAddr)
			if err := pprofSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("[main] pprof server error: %v", err)
			}
		}()
	}

	// 如果配置了则启动 gnet TCP 服务器。
	if transport == "gnet" || transport == "both" {
		go func() {
			log.Printf("[main] gnet TCP server starting on %s", app.Config.Gateway.TCPAddr)
			if err := app.Server.StartGNet(app.Config); err != nil {
				log.Printf("[main] gnet error: %v", err)
			}
		}()
	}

	// 使用配置的超时时间构建 HTTP 服务器。
	shutdownTimeout := time.Duration(st.ShutdownTimeout)
	if shutdownTimeout <= 0 {
		shutdownTimeout = 30 * time.Second
	}
	// 应用 HTTP 超时，带零值保护（配置缺失 = 使用默认值）。
	readTimeout := time.Duration(st.HTTPReadTimeout)
	if readTimeout <= 0 {
		readTimeout = 10 * time.Second
	}
	writeTimeout := time.Duration(st.HTTPWriteTimeout)
	if writeTimeout <= 0 {
		writeTimeout = 10 * time.Second
	}
	idleTimeout := time.Duration(st.HTTPIdleTimeout)
	if idleTimeout <= 0 {
		idleTimeout = 120 * time.Second
	}
	srv := &http.Server{
		Addr:         app.Config.Gateway.HTTPAddr,
		Handler:      handler,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  idleTimeout,
	}

	go func() {
		<-ctx.Done()
		log.Println("[main] shutting down...")

		// 1. 先关闭 HTTP 服务器（停止接受新的 HTTP/WS 连接）。
		httpCtx, httpCancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer httpCancel()
		if err := srv.Shutdown(httpCtx); err != nil {
			log.Printf("[main] HTTP shutdown error: %v", err)
		}

		// 2. 关闭 gnet TCP 服务器（停止接受新 TCP 连接，排空现有连接）。
		if gh := app.Server.GnetHandler(); gh != nil {
			gnetCtx, gnetCancel := context.WithTimeout(context.Background(), shutdownTimeout)
			defer gnetCancel()
			if err := gh.Shutdown(gnetCtx); err != nil {
				log.Printf("[main] gnet shutdown error: %v", err)
			} else {
				log.Printf("[main] gnet server stopped")
			}
		}
	}()

	log.Printf("[main] IM Gateway starting on %s (transport=%s)", app.Config.Gateway.HTTPAddr, transport)
	return srv.ListenAndServe()
}

func main() {
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "configs/config.json"
	}
	cfg, err := configs.Load(configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	app, err := NewApp(cfg)
	if err != nil {
		log.Fatalf("init app: %v", err)
	}
	defer app.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		cancel()
	}()

	if err := app.Run(ctx); err != nil && err != http.ErrServerClosed {
		log.Fatalf("[main] server error: %v", err)
	}
	log.Println("[main] server stopped")
}
