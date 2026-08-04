package gateway

import (
	"context"
	"log"
	"strconv"

	"github.com/im/api/proto"
	"github.com/redis/go-redis/v9"
	pb "google.golang.org/protobuf/proto"
)

const defaultKeyPrefix = "im:offline:"

// RedisOfflineStore 实现基于 Redis List 的 OfflineStore。
// 可选的内存回退(Hub)可优雅地处理 Redis 故障。
type RedisOfflineStore struct {
	client    *redis.Client
	maxSize   int
	keyPrefix string
	fallback  OfflineStore
}

// NewRedisStore 创建基于 Redis 的离线消息存储。
// 调用方必须确保客户端已连接(Ping 成功)。
func NewRedisStore(client *redis.Client, maxSize int) *RedisOfflineStore {
	return &RedisOfflineStore{
		client:    client,
		maxSize:   maxSize,
		keyPrefix: defaultKeyPrefix,
	}
}

// WithFallback 设置 Redis 操作失败时使用的内存回退。
// 通常是 Hub,这样即使 Redis 暂时不可用,消息也不会丢失。
func (rs *RedisOfflineStore) WithFallback(fb OfflineStore) *RedisOfflineStore {
	rs.fallback = fb
	return rs
}

// Close 关闭底层的 Redis 客户端。nil 接收者上调用也是安全的。
func (rs *RedisOfflineStore) Close() error {
	if rs == nil || rs.client == nil {
		return nil
	}
	return rs.client.Close()
}

// offlineKey 返回用户离线队列的 Redis 键。
func (rs *RedisOfflineStore) offlineKey(uid string) string {
	return rs.keyPrefix + uid
}

// Lua 脚本:原子地向列表推送并裁剪到 maxSize。
// KEYS[1] = 列表键,ARGV[1] = protobuf 二进制负载,ARGV[2] = maxSize。
var storeScript = redis.NewScript(`
	redis.call('RPUSH', KEYS[1], ARGV[1])
	local len = redis.call('LLEN', KEYS[1])
	local maxSize = tonumber(ARGV[2])
	if len > maxSize then
		redis.call('LTRIM', KEYS[1], -maxSize, -1)
	end
	return len
`)

// Lua 脚本:原子地读取全部元素并删除该键。
// KEYS[1] = 列表键。
var drainScript = redis.NewScript(`
	local msgs = redis.call('LRANGE', KEYS[1], 0, -1)
	if #msgs > 0 then
		redis.call('DEL', KEYS[1])
	end
	return msgs
`)

// StoreOffline 为离线用户排队存储一条消息。
// 如果 Redis 不可用,则回退到内存存储。
func (rs *RedisOfflineStore) StoreOffline(ctx context.Context, uid string, msg *proto.Message) {
	data, err := pb.Marshal(msg)
	if err != nil {
		log.Printf("[redis-store] marshal error for uid=%s: %v", uid, err)
		if rs.fallback != nil {
			rs.fallback.StoreOffline(ctx, uid, msg)
		}
		return
	}

	key := rs.offlineKey(uid)
	_, err = storeScript.Run(ctx, rs.client, []string{key}, string(data), strconv.Itoa(rs.maxSize)).Result()
	if err != nil {
		log.Printf("[redis-store] StoreOffline error for uid=%s: %v", uid, err)
		if rs.fallback != nil {
			rs.fallback.StoreOffline(ctx, uid, msg)
		}
	}
}

// DrainOffline 检索并清除用户的所有离线消息。
// 如果 Redis 不可用,则回退到内存存储。
func (rs *RedisOfflineStore) DrainOffline(ctx context.Context, uid string) []*proto.Message {
	key := rs.offlineKey(uid)
	result, err := drainScript.Run(ctx, rs.client, []string{key}).Result()
	if err != nil {
		log.Printf("[redis-store] DrainOffline error for uid=%s: %v", uid, err)
		if rs.fallback != nil {
			return rs.fallback.DrainOffline(ctx, uid)
		}
		return nil
	}

	items, ok := result.([]interface{})
	if !ok {
		// 没有消息或类型不符
		return nil
	}

	msgs := make([]*proto.Message, 0, len(items))
	for _, item := range items {
		s, ok := item.(string)
		if !ok {
			continue
		}
		msg := &proto.Message{}
		if err := pb.Unmarshal([]byte(s), msg); err != nil {
			log.Printf("[redis-store] unmarshal error during drain for uid=%s: %v", uid, err)
			continue
		}
		msgs = append(msgs, msg)
	}
	return msgs
}

// 确保 RedisOfflineStore 实现了 OfflineStore。
var _ OfflineStore = (*RedisOfflineStore)(nil)
