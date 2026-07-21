package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof" // register pprof handlers on DefaultServeMux
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

// App holds the initialized application components.
type App struct {
	Hub           gateway.ClientRegistry
	Server        *gateway.Server
	Config        *configs.Config
	redisClient   *redis.Client           // nil if Redis disabled or unavailable
	mysqlStore    *repo.MySQLStore        // nil if MySQL disabled
	kafkaProducer *mq.Producer           // nil if Kafka disabled
	logicClient   *gateway.LogicClient    // nil if Logic gRPC disabled
	grpcServer    *grpc.Server            // nil when gRPC disabled (single-node mode)
	grpcForwarder *gateway.GrpcForwarder  // nil when multi-gateway disabled
	clusterMgr    *gateway.ClusterManager // nil when multi-gateway disabled
	pprofServer   *http.Server            // nil when pprof disabled
	router        *gateway.Router         // for cleanup (dedup cache, rate limiter)
}

// NewApp initializes all app components from configuration.
func NewApp(cfg *configs.Config) (*App, error) {
	snow, err := snowflake.New(cfg.Snow.WorkerID)
	if err != nil {
		return nil, fmt.Errorf("snowflake: %w", err)
	}
	jwtMgr := jwt.New(cfg.JWT.Secret, time.Duration(cfg.JWT.Expiration))
	hub := gateway.NewHub(cfg.Gateway.Conn.OfflineMaxSize)

	// Wire offline store: try Redis, fall back to in-memory Hub.
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

	// Initialize MySQL store if enabled.
	var mysqlStore *repo.MySQLStore
	if cfg.Gateway.MySQL.Enabled {
		var err error
		mysqlStore, err = repo.NewMySQLStore(cfg.Gateway.MySQL.DSN)
		if err != nil {
			log.Printf("[main] MySQL init failed (%v), continuing without persistence", err)
		} else {
			log.Printf("[main] MySQL connected at %s", cfg.Gateway.MySQL.DSN)
		}
		// Bootstrap admin users from config (idempotent — runs on every startup).
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

	// Initialize Kafka producer if enabled.
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

	// Initialize Logic gRPC client if configured.
	logicClient, err := gateway.NewLogicClient(cfg.Gateway.LogicGateway.Addr)
	if err != nil {
		log.Printf("[main] Logic gRPC client init failed (%v), will use local MessageStore", err)
	}

	// Build router config from gateway config.
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

	// Wire rate limiting from config.
	if cfg.Gateway.RateLimit.Enabled {
		router.SetRateLimit(cfg.Gateway.RateLimit.Rate, cfg.Gateway.RateLimit.Burst)
	}

	// Set max connections limit on the hub.
	if cfg.Stability.MaxConnections > 0 {
		hub.SetMaxConnections(cfg.Stability.MaxConnections)
	}

	// --- Multi-Gateway horizontal scaling ---
	var grpcForwarder *gateway.GrpcForwarder
	var grpcSrv *grpc.Server
	var clusterMgr *gateway.ClusterManager

	if cfg.Gateway.Grpc.Addr != "" && cfg.Gateway.Grpc.NodeID != "" {
		// Build consistent hash ring with all peers plus self.
		hr := gateway.NewHashRing(150) // 150 virtual nodes per physical node
		for nodeID := range cfg.Gateway.Grpc.PeerAddrs {
			hr.Add(nodeID)
		}
		hr.Add(cfg.Gateway.Grpc.NodeID) // include self

		// Create gRPC forwarder for cross-node message delivery.
		grpcForwarder = gateway.NewGrpcForwarder(
			hr,
			cfg.Gateway.Grpc.NodeID,
			cfg.Gateway.Grpc.PeerAddrs,
			time.Duration(cfg.Gateway.Grpc.ForwardDialTimeout),
			time.Duration(cfg.Gateway.Grpc.ForwardRPCTimeout),
		)

		// Inject into router.
		router.SetHashRing(hr)
		router.SetForwarder(grpcForwarder)
		router.SetThisNodeID(cfg.Gateway.Grpc.NodeID)

		// Create gRPC server handler for incoming forwarded messages.
		grpcHandler := gateway.NewGrpcGatewayServer(hub, offlineStore, cfg.Gateway.Grpc.NodeID)
		grpcSrv, err = gateway.StartGrpcServer(cfg.Gateway.Grpc.Addr, grpcHandler)
		if err != nil {
			log.Printf("[main] gRPC server start failed (%v), continuing without multi-gateway", err)
			grpcSrv = nil
		}

		// Set up dynamic clustering (health checks + optional Redis discovery).
		if grpcSrv != nil {
			clusterCfg := gateway.ClusterConfig{
				ThisNodeID: cfg.Gateway.Grpc.NodeID,
				ThisAddr:   cfg.Gateway.Grpc.Addr,
			}
			if cfg.Gateway.Grpc.Discovery.HealthInterval > 0 {
				clusterCfg.HealthInterval = time.Duration(cfg.Gateway.Grpc.Discovery.HealthInterval)
			}

			// Enable Redis service discovery when configured and Redis is available.
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

	// --- Group chat support ---
	var groupStore gateway.GroupStore
	if mysqlStore != nil {
		groupStore = gateway.NewMySQLGroupStore(mysqlStore.DB(), snow)
		log.Printf("[main] using MySQL group store")
	} else {
		groupStore = gateway.NewInMemoryGroupStore(snow)
		log.Printf("[main] using in-memory group store")
	}
	router.SetGroupStore(groupStore)

	// --- Dedup Redis durability ---
	if rdb != nil {
		router.SetDedupRedis(rdb)
		log.Printf("[main] dedup: Redis durability enabled")
	}

	// --- Friend relationship management ---
	if mysqlStore != nil {
		router.SetFriendStore(mysqlStore)
		log.Printf("[main] friend system enabled (MySQL)")
	} else {
		log.Printf("[main] friend system disabled (no MySQL)")
	}

	// --- Read/unread receipt tracking ---
	unreadTracker := gateway.NewInMemoryUnreadTracker()
	router.SetUnreadTracker(unreadTracker)

	// --- Object storage for file/image messages ---
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

	// Register dependency health checks for the /health endpoint.
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
		// Kafka producer is optional; health check reports enabled.
		// The kafka-go Writer has no Ping method, so we report enabled
		// and surface errors via Publish logs.
		gateway.RegisterHealthCheck("kafka", func(ctx context.Context) error {
			return nil // enabled = presumed healthy; Publish failures are logged
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

// Close cleans up application resources in dependency order.
func (app *App) Close() {
	// 1. Stop background goroutines (DedupCache, RateLimiter).
	if app.router != nil {
		app.router.Stop()
	}

	// 2. Stop cluster manager (deregisters from Redis, stops health checks).
	if app.clusterMgr != nil {
		app.clusterMgr.Stop()
	}

	// 3. Stop gRPC forwarder (peer connections).
	if app.grpcForwarder != nil {
		if err := app.grpcForwarder.Close(); err != nil {
			log.Printf("[main] gRPC forwarder close error: %v", err)
		}
	}

	// 4. Stop gRPC server.
	if app.grpcServer != nil {
		app.grpcServer.GracefulStop()
		log.Printf("[main] gRPC server stopped")
	}

	// 5. Stop pprof server.
	if app.pprofServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		app.pprofServer.Shutdown(ctx)
	}

	// 6. Kafka producer.
	if app.kafkaProducer != nil {
		if err := app.kafkaProducer.Close(); err != nil {
			log.Printf("[main] Kafka producer close error: %v", err)
		}
	}

	// 7. Logic gRPC client.
	if app.logicClient != nil {
		if err := app.logicClient.Close(); err != nil {
			log.Printf("[main] Logic gRPC client close error: %v", err)
		}
	}

	// 8. Redis.
	if app.redisClient != nil {
		if err := app.redisClient.Close(); err != nil {
			log.Printf("[main] Redis close error: %v", err)
		}
	}

	// 9. MySQL (last — other components may have used it).
	if app.mysqlStore != nil {
		if err := app.mysqlStore.Close(); err != nil {
			log.Printf("[main] MySQL close error: %v", err)
		}
	}
}

// Run starts the HTTP server and optionally the gnet TCP server, blocking until ctx is cancelled.
func (app *App) Run(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/login", app.Server.HandleLogin)
	mux.HandleFunc("/register", app.Server.HandleRegister)
	mux.HandleFunc("/online", app.Server.HandleOnlineUsers)

	// Group chat management endpoints.
	mux.HandleFunc("/group/create", app.Server.HandleGroupCreate)
	mux.HandleFunc("/group/join", app.Server.HandleGroupJoin)
	mux.HandleFunc("/group/invite", app.Server.HandleGroupInvite)
	mux.HandleFunc("/group/leave", app.Server.HandleGroupLeave)
	mux.HandleFunc("/group/kick", app.Server.HandleGroupKick)
	mux.HandleFunc("/group/rename", app.Server.HandleGroupRename)
	mux.HandleFunc("/group/transfer", app.Server.HandleGroupTransfer)
	mux.HandleFunc("/group/members", app.Server.HandleGroupMembers)
	mux.HandleFunc("/group/list", app.Server.HandleGroupList)

	// Unread count query.
	mux.HandleFunc("/unread", app.Server.HandleUnreadCount)

	// File/image upload and download.
	mux.HandleFunc("/upload", app.Server.HandleUpload)
	mux.HandleFunc("/file", app.Server.HandleDownload)

	// Message fulltext search.
	mux.HandleFunc("/search", app.Server.HandleSearch)

	// Change password.
	mux.HandleFunc("/change-password", app.Server.HandleChangePassword)

	// Friend management.
	mux.HandleFunc("/friend/request", app.Server.HandleFriendRequest)
	mux.HandleFunc("/friend/accept", app.Server.HandleFriendAccept)
	mux.HandleFunc("/friend/reject", app.Server.HandleFriendReject)
	mux.HandleFunc("/friend/remove", app.Server.HandleFriendRemove)
	mux.HandleFunc("/friend/list", app.Server.HandleFriendList)

	transport := app.Config.Gateway.Transport
	if transport == "" {
		transport = "websocket" // backward compat for old configs
	}

	// Only mount /ws if WebSocket transport is active.
	if transport == "websocket" || transport == "both" {
		mux.HandleFunc("/ws", app.Server.HandleWS)
	}

	mux.HandleFunc("/health", app.Server.HandleHealth)

	// Admin monitoring and management endpoints.
	mux.HandleFunc("/admin/stats", app.Server.HandleAdminStats)
	mux.HandleFunc("/admin/users", app.Server.HandleAdminUsers)
	mux.HandleFunc("/admin/users/delete", app.Server.HandleAdminUserDelete)
	mux.HandleFunc("/admin/messages", app.Server.HandleAdminMessages)
	mux.HandleFunc("/admin/messages/delete", app.Server.HandleAdminMessageDelete)

	// Wrap with panic recovery middleware
	handler := gateway.Recovery(mux)

	// Start pprof server if enabled.
	st := app.Config.Stability
	if st.PprofEnabled && st.PprofAddr != "" {
		pprofSrv := &http.Server{
			Addr:    st.PprofAddr,
			Handler: http.DefaultServeMux, // net/http/pprof registers on DefaultServeMux
		}
		app.pprofServer = pprofSrv
		go func() {
			log.Printf("[main] pprof server listening on %s", st.PprofAddr)
			if err := pprofSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("[main] pprof server error: %v", err)
			}
		}()
	}

	// Start gnet TCP server if configured.
	if transport == "gnet" || transport == "both" {
		go func() {
			log.Printf("[main] gnet TCP server starting on %s", app.Config.Gateway.TCPAddr)
			if err := app.Server.StartGNet(app.Config); err != nil {
				log.Printf("[main] gnet error: %v", err)
			}
		}()
	}

	// Build HTTP server with configured timeouts.
	shutdownTimeout := time.Duration(st.ShutdownTimeout)
	if shutdownTimeout <= 0 {
		shutdownTimeout = 30 * time.Second
	}
	// Apply HTTP timeouts with zero-value protection (missing config = defaults).
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

		// 1. Shutdown HTTP server first (stop accepting new HTTP/WS connections).
		httpCtx, httpCancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer httpCancel()
		if err := srv.Shutdown(httpCtx); err != nil {
			log.Printf("[main] HTTP shutdown error: %v", err)
		}

		// 2. Shutdown gnet TCP server (stop accepting new TCP connections, drain existing).
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
