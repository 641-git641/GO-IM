// Package configs 定义服务器配置。
package configs

import (
	"encoding/json"
	"log"
	"os"
	"time"
)

// Duration 是一个 time.Duration 类型，可序列化为人类可读的字符串（例如 "30s"）。
type Duration time.Duration

// MarshalJSON 实现 json.Marshaler。
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// UnmarshalJSON 实现 json.Unmarshaler。
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	dur, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(dur)
	return nil
}

// StabilityConfig 保存运行稳定性相关设置。
type StabilityConfig struct {
	MaxConnections   int      `json:"max_connections"`   // 0 = 无限制
	HTTPReadTimeout  Duration `json:"http_read_timeout"`  // 例如 "10s"
	HTTPWriteTimeout Duration `json:"http_write_timeout"` // 例如 "10s"
	HTTPIdleTimeout  Duration `json:"http_idle_timeout"`  // 例如 "120s"
	ShutdownTimeout  Duration `json:"shutdown_timeout"`   // 例如 "30s"
	PprofEnabled     bool     `json:"pprof_enabled"`      // false = 不启用调试端点
	PprofAddr        string   `json:"pprof_addr"`         // 例如 "localhost:6060"
}

// Config 汇总所有配置值。
type Config struct {
	Gateway   GatewayConfig   `json:"gateway"`
	Logic     LogicConfig     `json:"logic"`
	JWT       JWTConfig       `json:"jwt"`
	Snow      SnowConfig      `json:"snowflake"`
	Stability StabilityConfig `json:"stability"`
	AdminUIDs []string        `json:"admin_uids"` // 初始管理员用户 ID
}

// GatewayConfig 保存网关服务器设置。
type GatewayConfig struct {
	HTTPAddr      string            `json:"http_addr"`       // 例如 ":8080"
	TCPAddr       string            `json:"tcp_addr"`        // 例如 ":8081" —— gnet TCP 监听地址
	Transport     string            `json:"transport"`       // "websocket"、"gnet" 或 "both"
	Heartbeat     Duration          `json:"heartbeat"`       // 心跳间隔
	HeartbeatFail int               `json:"heartbeat_fail"`  // 最大连续心跳失败次数
	CheckOrigin   []string          `json:"check_origin"`    // 允许的 WebSocket 来源（空 = 允许全部）
	Conn          GatewayConnConfig `json:"conn"`            // 每连接参数
	RateLimit     RateLimitConfig   `json:"rate_limit"`      // 限流
	Redis         RedisConfig       `json:"redis"`           // Redis（Addr 为空 = 禁用）
	GNet          GNetConfig        `json:"gnet"`            // gnet TCP 设置
	MySQL         MySQLConfig       `json:"mysql"`           // MySQL（Enabled=false = 内存模式）
	Auth          AuthConfig        `json:"auth"`            // 认证设置
	Kafka         KafkaConfig       `json:"kafka"`           // Kafka（Enabled=false = 直接写 MySQL）
	LogicGateway  LogicConfig       `json:"logic"`           // Gateway→Logic 的 gRPC 客户端（Addr）；与顶层 Config.Logic 不同
	Grpc          GrpcConfig           `json:"grpc"`            // 网关自身的 gRPC 服务器（用于跨网关，未来扩展）
	ObjectStorage ObjectStorageConfig  `json:"object_storage"`   // 用于文件/图片消息的 MinIO/S3

	// 运行参数（之前为硬编码常量）。
	RecallWindowMs     int64    `json:"recall_window_ms"`      // 消息撤回时间窗口（毫秒），默认 120000
	HistoryDefaultLimit int     `json:"history_default_limit"` // 默认历史记录每页条数，默认 30
	SearchDefaultLimit  int     `json:"search_default_limit"`  // 默认搜索结果条数，默认 20
	DedupTTL            Duration `json:"dedup_ttl"`            // 去重缓存条目 TTL，默认 "5m"
	PersistConcurrency  int      `json:"persist_concurrency"`  // 最大并发异步持久化 goroutine 数，默认 64
}

// GNetConfig 保存 gnet TCP 服务器设置。
type GNetConfig struct {
	NumEventLoops  int `json:"num_event_loops"`  // 0 = 自动（runtime.NumCPU()）
	WorkerPoolSize int `json:"worker_pool_size"` // 0 = 自动（runtime.NumCPU() * 2）
}

// MySQLConfig 保存 MySQL 连接设置。
// Enabled=false 表示 MySQL 被禁用（回退到内存模式）。
type MySQLConfig struct {
	Enabled bool   `json:"enabled"` // false = 内存模式
	DSN     string `json:"dsn"`     // 例如 "im:im-dev@tcp(127.0.0.1:3306)/im?parseTime=true"
}

// AuthConfig 保存认证设置。
type AuthConfig struct {
	DevMode bool `json:"dev_mode"` // true = 跳过密码校验（开发环境）
}

// KafkaConfig 保存 Kafka 生产者设置。
// Enabled=false 表示 Kafka 被禁用（直接写 MySQL）。
type KafkaConfig struct {
	Enabled bool     `json:"enabled"` // false = 直接写 MySQL 或同步写入
	Brokers []string `json:"brokers"` // 例如 ["localhost:9092"]
	Topic   string   `json:"topic"`   // 例如 "im.message.persist"
}

// LogicConfig 保存 Logic 服务的连接与服务器设置。
// 当 MySQL/Kafka 字段为空时，Logic 服务会回退到
// gateway.mysql 和 gateway.kafka 以保持向后兼容。
type LogicConfig struct {
	Addr       string      `json:"addr"`        // Gateway→Logic 的 gRPC 地址；空 = 使用本地 MessageStore
	ListenAddr string      `json:"listen_addr"` // Logic 服务自身的 gRPC 绑定地址；空 = ":50051"
	MySQL      MySQLConfig `json:"mysql"`       // Logic 自身的 MySQL 配置（回退到 gateway.mysql）
	Kafka      KafkaConfig `json:"kafka"`       // Logic 自身的 Kafka 配置（回退到 gateway.kafka）
	WorkerID   int64       `json:"worker_id"`   // 用于群组 ID 的 Snowflake worker ID；默认 2（网关使用 1）
}

// ObjectStorageConfig 保存文件/图片消息的对象存储（MinIO/S3）设置。
// Enabled=false 表示回退到内存模式（不持久化）。
type ObjectStorageConfig struct {
	Enabled   bool   `json:"enabled"`    // false = 回退到内存模式
	Endpoint  string `json:"endpoint"`   // 例如 "localhost:9000"
	AccessKey string `json:"access_key"` // MinIO 访问密钥
	SecretKey string `json:"secret_key"` // MinIO 密钥
	Bucket    string `json:"bucket"`     // 默认 "im-files"
	UseSSL    bool   `json:"use_ssl"`    // 默认 false（开发环境）
	MaxUpload int64  `json:"max_upload"` // 最大上传大小（字节），默认 10 MB
}

// GrpcConfig 保存网关自身的 gRPC 服务器设置，用于跨网关转发。
// 当 Addr 非空且设置了 NodeID 时，网关会启动 gRPC 服务器
// 并加入多节点哈希环，实现跨节点消息投递。
type GrpcConfig struct {
	Addr               string            `json:"addr"`                 // 例如 ":50050" —— 空 = 禁用
	NodeID             string            `json:"node_id"`              // 该网关的唯一 ID，例如 "gw-1"
	PeerAddrs          map[string]string `json:"peer_addrs"`           // 对端节点 nodeID → gRPC 地址，例如 {"gw-2": "localhost:50051"}
	ForwardDialTimeout Duration          `json:"forward_dial_timeout"` // gRPC 转发器拨号超时，默认 "3s"
	ForwardRPCTimeout  Duration          `json:"forward_rpc_timeout"`  // gRPC 转发器 RPC 超时，默认 "2s"
	Discovery          DiscoveryConfig   `json:"discovery"`            // 服务发现与健康检查
}

// DiscoveryConfig 保存多网关集群的服务发现与健康检查设置。
// 当 Mode 为 "redis" 时，网关使用 Redis 进行对端发现，而不是静态 peer_addrs。
// 当 Mode 为空或 "static" 时，直接使用 peer_addrs（静态环）。
type DiscoveryConfig struct {
	Mode           string   `json:"mode"`            // "" 或 "static" = 静态 peer_addrs；"redis" = Redis 发现
	RedisKey       string   `json:"redis_key"`       // 节点注册表的 Redis 哈希键，默认 "im:gateway:nodes"
	TTL            Duration `json:"ttl"`             // 心跳 TTL，默认 "15s"
	HealthInterval Duration `json:"health_interval"` // 健康检查间隔，默认 "5s"
}

// GatewayConnConfig 保存每连接 WebSocket 参数。
type GatewayConnConfig struct {
	PongWait       Duration `json:"pong_wait"`        // pong 超时
	PingPeriod     Duration `json:"ping_period"`      // ping 间隔
	MaxMsgSize     int64    `json:"max_msg_size"`     // 最大入站消息大小
	SendBufSize    int      `json:"send_buf_size"`    // 出站通道容量
	OfflineMaxSize int      `json:"offline_max_size"` // 每个用户的最大离线消息数
}

// RateLimitConfig 保存每用户限流设置。
type RateLimitConfig struct {
	Enabled        bool     `json:"enabled"`         // false = 禁用限流
	Rate           int      `json:"rate"`            // 每秒消息数（令牌补充速率）
	Burst          int      `json:"burst"`           // 最大突发量（桶容量）
	CleanupInterval Duration `json:"cleanup_interval"` // 过期桶清理间隔，默认 "5m"
}

// JWTConfig 保存 JWT 设置。
type JWTConfig struct {
	Secret     string   `json:"secret"`
	Expiration Duration `json:"expiration"`
}

// RedisConfig 保存 Redis 连接设置。
// Addr 为空表示 Redis 被禁用（回退到内存模式）。
type RedisConfig struct {
	Addr     string `json:"addr"`     // 例如 "localhost:6379" —— 空 = 禁用
	Password string `json:"password"` // 无需认证时为空
	DB       int    `json:"db"`       // Redis 数据库编号，默认 0
}

// SnowConfig 保存 snowflake ID 生成器设置。
type SnowConfig struct {
	WorkerID int64 `json:"worker_id"`
}

// Default 返回带有合理默认值的 Config。
func Default() *Config {
	return &Config{
		Gateway: GatewayConfig{
			HTTPAddr:      ":8080",
			TCPAddr:       ":8081",
			Transport:     "websocket", // 默认为 websocket 以保持向后兼容
			Heartbeat:     Duration(30 * time.Second),
			HeartbeatFail: 3,
			Conn: GatewayConnConfig{
				PongWait:       Duration(60 * time.Second),
				PingPeriod:     Duration(54 * time.Second),
				MaxMsgSize:     65536, // protobuf 二进制消息需要更大余量
				SendBufSize:    256,
				OfflineMaxSize: 1000,
			},
			RateLimit: RateLimitConfig{
				Enabled:        true,
				Rate:           10,
				Burst:          20,
				CleanupInterval: Duration(5 * time.Minute),
			},
			GNet: GNetConfig{
				NumEventLoops:  0, // 自动（runtime.NumCPU()）
				WorkerPoolSize: 0, // 自动（runtime.NumCPU() * 2）
			},
			MySQL: MySQLConfig{
				Enabled: false,                                                      // 默认使用内存模式
				DSN:     "im:im-dev@tcp(127.0.0.1:3307)/im?parseTime=true&charset=utf8mb4",
			},
			Auth: AuthConfig{
				DevMode: true, // 开发环境跳过密码
			},
			Kafka: KafkaConfig{
				Enabled: false,                // 默认直接写 MySQL / 同步写入
				Brokers: []string{"localhost:9092"},
				Topic:   "im.message.persist",
			},
			LogicGateway: LogicConfig{
				Addr:       "",            // 空 = 使用本地 MessageStore
				ListenAddr: "",            // 空 = ":50051"
				MySQL:      MySQLConfig{}, // 空 → 回退到 gateway.mysql
				Kafka:      KafkaConfig{}, // 空 → 回退到 gateway.kafka
				WorkerID:   2,             // 群组 ID 的 snowflake worker（网关使用 1）
			},
			Grpc: GrpcConfig{
				Addr:               "",   // 空 = 禁用（单节点模式）
				NodeID:             "",
				PeerAddrs:          nil,
				ForwardDialTimeout: Duration(3 * time.Second),
				ForwardRPCTimeout:  Duration(2 * time.Second),
				Discovery: DiscoveryConfig{
					Mode:           "", // "static" —— 使用 peer_addrs
					RedisKey:       "im:gateway:node:",
					TTL:            Duration(15 * time.Second),
					HealthInterval: Duration(5 * time.Second),
				},
			},
			ObjectStorage: ObjectStorageConfig{
				Enabled:   false,      // 默认使用内存模式（无需 Docker）
				Endpoint:  "localhost:9000",
				AccessKey: "minioadmin",
				SecretKey: "minioadmin",
				Bucket:    "im-files",
				UseSSL:    false,
				MaxUpload: 10 * 1024 * 1024, // 10 MB
			},

			// 运行参数。
			RecallWindowMs:      120_000,
			HistoryDefaultLimit: 30,
			SearchDefaultLimit:  20,
			DedupTTL:            Duration(5 * time.Minute),
			PersistConcurrency:  64,
		},
		JWT: JWTConfig{
			Secret:     "change-me-in-production",
			Expiration: Duration(7 * 24 * time.Hour),
		},
		Snow: SnowConfig{
			WorkerID: 1,
		},
		Stability: StabilityConfig{
			MaxConnections:   0, // 无限制
			HTTPReadTimeout:  Duration(10 * time.Second),
			HTTPWriteTimeout: Duration(10 * time.Second),
			HTTPIdleTimeout:  Duration(120 * time.Second),
			ShutdownTimeout:  Duration(30 * time.Second),
			PprofEnabled:     false,
			PprofAddr:        "localhost:6060",
		},
		AdminUIDs: []string{},
	}
}

// Load 读取 JSON 配置文件，文件不存在时回退到默认值。
func Load(path string) (*Config, error) {
	cfg := Default()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("[config] no config file at %s, using defaults", path)
			return cfg, nil
		}
		return nil, err
	}
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
