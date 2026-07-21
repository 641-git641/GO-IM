// Package configs defines the server configuration.
package configs

import (
	"encoding/json"
	"log"
	"os"
	"time"
)

// Duration is a time.Duration that marshals as a human-readable string (e.g. "30s").
type Duration time.Duration

// MarshalJSON implements json.Marshaler.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// UnmarshalJSON implements json.Unmarshaler.
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

// StabilityConfig holds operational stability settings.
type StabilityConfig struct {
	MaxConnections   int      `json:"max_connections"`   // 0 = unlimited
	HTTPReadTimeout  Duration `json:"http_read_timeout"`  // e.g. "10s"
	HTTPWriteTimeout Duration `json:"http_write_timeout"` // e.g. "10s"
	HTTPIdleTimeout  Duration `json:"http_idle_timeout"`  // e.g. "120s"
	ShutdownTimeout  Duration `json:"shutdown_timeout"`   // e.g. "30s"
	PprofEnabled     bool     `json:"pprof_enabled"`      // false = no debug endpoints
	PprofAddr        string   `json:"pprof_addr"`         // e.g. "localhost:6060"
}

// Config aggregates all configuration values.
type Config struct {
	Gateway   GatewayConfig   `json:"gateway"`
	Logic     LogicConfig     `json:"logic"`
	JWT       JWTConfig       `json:"jwt"`
	Snow      SnowConfig      `json:"snowflake"`
	Stability StabilityConfig `json:"stability"`
	AdminUIDs []string        `json:"admin_uids"` // bootstrap admin user IDs
}

// GatewayConfig holds the gateway server settings.
type GatewayConfig struct {
	HTTPAddr      string            `json:"http_addr"`       // e.g. ":8080"
	TCPAddr       string            `json:"tcp_addr"`        // e.g. ":8081" — gnet TCP listen address
	Transport     string            `json:"transport"`       // "websocket", "gnet", or "both"
	Heartbeat     Duration          `json:"heartbeat"`       // heartbeat interval
	HeartbeatFail int               `json:"heartbeat_fail"`  // max consecutive heartbeat failures
	CheckOrigin   []string          `json:"check_origin"`    // allowed WebSocket origins (empty = allow all)
	Conn          GatewayConnConfig `json:"conn"`            // per-connection parameters
	RateLimit     RateLimitConfig   `json:"rate_limit"`      // rate limiting
	Redis         RedisConfig       `json:"redis"`           // Redis (empty Addr = disabled)
	GNet          GNetConfig        `json:"gnet"`            // gnet TCP settings
	MySQL         MySQLConfig       `json:"mysql"`           // MySQL (Enabled=false = in-memory)
	Auth          AuthConfig        `json:"auth"`            // authentication settings
	Kafka         KafkaConfig       `json:"kafka"`           // Kafka (Enabled=false = direct MySQL)
	LogicGateway  LogicConfig       `json:"logic"`           // Gateway→Logic gRPC client (Addr); distinct from top-level Config.Logic
	Grpc          GrpcConfig           `json:"grpc"`            // Gateway's own gRPC server (for inter-gateway, future)
	ObjectStorage ObjectStorageConfig  `json:"object_storage"`   // MinIO/S3 for file/image messages

	// Operational tunables (previously hardcoded constants).
	RecallWindowMs     int64    `json:"recall_window_ms"`      // message recall window in milliseconds, default 120000
	HistoryDefaultLimit int     `json:"history_default_limit"` // default history page size, default 30
	SearchDefaultLimit  int     `json:"search_default_limit"`  // default search result limit, default 20
	DedupTTL            Duration `json:"dedup_ttl"`            // dedup cache entry TTL, default "5m"
	PersistConcurrency  int      `json:"persist_concurrency"`  // max concurrent async persist goroutines, default 64
}

// GNetConfig holds gnet TCP server settings.
type GNetConfig struct {
	NumEventLoops  int `json:"num_event_loops"`  // 0 = auto (runtime.NumCPU())
	WorkerPoolSize int `json:"worker_pool_size"` // 0 = auto (runtime.NumCPU() * 2)
}

// MySQLConfig holds MySQL connection settings.
// Enabled=false means MySQL is disabled (in-memory fallback).
type MySQLConfig struct {
	Enabled bool   `json:"enabled"` // false = in-memory mode
	DSN     string `json:"dsn"`     // e.g. "im:im-dev@tcp(127.0.0.1:3306)/im?parseTime=true"
}

// AuthConfig holds authentication settings.
type AuthConfig struct {
	DevMode bool `json:"dev_mode"` // true = skip password verification (development)
}

// KafkaConfig holds Kafka producer settings.
// Enabled=false means Kafka is disabled (direct MySQL write).
type KafkaConfig struct {
	Enabled bool     `json:"enabled"` // false = direct MySQL or synchronous
	Brokers []string `json:"brokers"` // e.g. ["localhost:9092"]
	Topic   string   `json:"topic"`   // e.g. "im.message.persist"
}

// LogicConfig holds Logic service connection and server settings.
// When MySQL/Kafka fields are empty, the Logic service falls back to
// gateway.mysql and gateway.kafka for backward compatibility.
type LogicConfig struct {
	Addr       string      `json:"addr"`        // Gateway→Logic gRPC address; empty = use local MessageStore
	ListenAddr string      `json:"listen_addr"` // Logic service's own gRPC bind address; empty = ":50051"
	MySQL      MySQLConfig `json:"mysql"`       // Logic's own MySQL config (falls back to gateway.mysql)
	Kafka      KafkaConfig `json:"kafka"`       // Logic's own Kafka config (falls back to gateway.kafka)
	WorkerID   int64       `json:"worker_id"`   // Snowflake worker ID for group IDs; default 2 (gateway uses 1)
}

// ObjectStorageConfig holds object storage (MinIO/S3) settings for file/image messages.
// Enabled=false means in-memory fallback (no persistence).
type ObjectStorageConfig struct {
	Enabled   bool   `json:"enabled"`    // false = in-memory fallback
	Endpoint  string `json:"endpoint"`   // e.g. "localhost:9000"
	AccessKey string `json:"access_key"` // MinIO access key
	SecretKey string `json:"secret_key"` // MinIO secret key
	Bucket    string `json:"bucket"`     // default "im-files"
	UseSSL    bool   `json:"use_ssl"`    // default false (dev)
	MaxUpload int64  `json:"max_upload"` // max upload size in bytes, default 10 MB
}

// GrpcConfig holds the Gateway's own gRPC server settings for inter-gateway forwarding.
// When Addr is non-empty and NodeID is set, the Gateway starts a gRPC server
// and participates in the multi-node hash ring for cross-node message delivery.
type GrpcConfig struct {
	Addr               string            `json:"addr"`                 // e.g. ":50050" — empty = disabled
	NodeID             string            `json:"node_id"`              // unique ID for this Gateway, e.g. "gw-1"
	PeerAddrs          map[string]string `json:"peer_addrs"`           // peer nodeID → gRPC address, e.g. {"gw-2": "localhost:50051"}
	ForwardDialTimeout Duration          `json:"forward_dial_timeout"` // gRPC forwarder dial timeout, default "3s"
	ForwardRPCTimeout  Duration          `json:"forward_rpc_timeout"`  // gRPC forwarder RPC timeout, default "2s"
	Discovery          DiscoveryConfig   `json:"discovery"`            // service discovery & health checking
}

// DiscoveryConfig holds service discovery and health check settings for multi-gateway clusters.
// When Mode is "redis", the Gateway uses Redis for peer discovery instead of static peer_addrs.
// When Mode is empty or "static", peer_addrs is used as-is (static ring).
type DiscoveryConfig struct {
	Mode           string   `json:"mode"`            // "" or "static" = static peer_addrs; "redis" = Redis discovery
	RedisKey       string   `json:"redis_key"`       // Redis hash key for node registry, default "im:gateway:nodes"
	TTL            Duration `json:"ttl"`             // heartbeat TTL, default "15s"
	HealthInterval Duration `json:"health_interval"` // health check interval, default "5s"
}

// GatewayConnConfig holds per-connection WebSocket parameters.
type GatewayConnConfig struct {
	PongWait       Duration `json:"pong_wait"`        // pong timeout
	PingPeriod     Duration `json:"ping_period"`      // ping interval
	MaxMsgSize     int64    `json:"max_msg_size"`     // max incoming message size
	SendBufSize    int      `json:"send_buf_size"`    // outbound channel capacity
	OfflineMaxSize int      `json:"offline_max_size"` // max offline messages per user
}

// RateLimitConfig holds per-user rate limiting settings.
type RateLimitConfig struct {
	Enabled        bool     `json:"enabled"`         // false to disable rate limiting
	Rate           int      `json:"rate"`            // messages per second (token refill rate)
	Burst          int      `json:"burst"`           // max burst size (bucket capacity)
	CleanupInterval Duration `json:"cleanup_interval"` // stale bucket cleanup interval, default "5m"
}

// JWTConfig holds JWT settings.
type JWTConfig struct {
	Secret     string   `json:"secret"`
	Expiration Duration `json:"expiration"`
}

// RedisConfig holds Redis connection settings.
// An empty Addr means Redis is disabled (in-memory fallback).
type RedisConfig struct {
	Addr     string `json:"addr"`     // e.g. "localhost:6379" — empty = disabled
	Password string `json:"password"` // empty if no auth required
	DB       int    `json:"db"`       // Redis database number, default 0
}

// SnowConfig holds snowflake ID generator settings.
type SnowConfig struct {
	WorkerID int64 `json:"worker_id"`
}

// Default returns a Config with sensible defaults.
func Default() *Config {
	return &Config{
		Gateway: GatewayConfig{
			HTTPAddr:      ":8080",
			TCPAddr:       ":8081",
			Transport:     "websocket", // default to websocket for backward compat
			Heartbeat:     Duration(30 * time.Second),
			HeartbeatFail: 3,
			Conn: GatewayConnConfig{
				PongWait:       Duration(60 * time.Second),
				PingPeriod:     Duration(54 * time.Second),
				MaxMsgSize:     65536, // protobuf binary messages need more headroom
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
				NumEventLoops:  0, // auto (runtime.NumCPU())
				WorkerPoolSize: 0, // auto (runtime.NumCPU() * 2)
			},
			MySQL: MySQLConfig{
				Enabled: false,                                                      // default to in-memory
				DSN:     "im:im-dev@tcp(127.0.0.1:3307)/im?parseTime=true&charset=utf8mb4",
			},
			Auth: AuthConfig{
				DevMode: true, // skip password in development
			},
			Kafka: KafkaConfig{
				Enabled: false,                // default to direct MySQL / synchronous
				Brokers: []string{"localhost:9092"},
				Topic:   "im.message.persist",
			},
			LogicGateway: LogicConfig{
				Addr:       "",            // empty = use local MessageStore
				ListenAddr: "",            // empty = ":50051"
				MySQL:      MySQLConfig{}, // empty → fall back to gateway.mysql
				Kafka:      KafkaConfig{}, // empty → fall back to gateway.kafka
				WorkerID:   2,             // snowflake worker for group IDs (gateway uses 1)
			},
			Grpc: GrpcConfig{
				Addr:               "",   // empty = disabled (single-node mode)
				NodeID:             "",
				PeerAddrs:          nil,
				ForwardDialTimeout: Duration(3 * time.Second),
				ForwardRPCTimeout:  Duration(2 * time.Second),
				Discovery: DiscoveryConfig{
					Mode:           "", // "static" — use peer_addrs
					RedisKey:       "im:gateway:node:",
					TTL:            Duration(15 * time.Second),
					HealthInterval: Duration(5 * time.Second),
				},
			},
			ObjectStorage: ObjectStorageConfig{
				Enabled:   false,      // default to in-memory (no Docker needed)
				Endpoint:  "localhost:9000",
				AccessKey: "minioadmin",
				SecretKey: "minioadmin",
				Bucket:    "im-files",
				UseSSL:    false,
				MaxUpload: 10 * 1024 * 1024, // 10 MB
			},

			// Operational tunables.
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
			MaxConnections:   0, // unlimited
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

// Load reads a JSON config file, falling back to defaults if the file is absent.
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
