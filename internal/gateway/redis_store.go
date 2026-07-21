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

// RedisOfflineStore implements OfflineStore backed by Redis Lists.
// An optional in-memory fallback (Hub) handles Redis failures gracefully.
type RedisOfflineStore struct {
	client    *redis.Client
	maxSize   int
	keyPrefix string
	fallback  OfflineStore
}

// NewRedisStore creates a Redis-backed offline message store.
// The caller must ensure the client is connected (Ping succeeded).
func NewRedisStore(client *redis.Client, maxSize int) *RedisOfflineStore {
	return &RedisOfflineStore{
		client:    client,
		maxSize:   maxSize,
		keyPrefix: defaultKeyPrefix,
	}
}

// WithFallback sets an in-memory fallback for when Redis operations fail.
// Typically this is the Hub, so messages are not lost if Redis is temporarily unavailable.
func (rs *RedisOfflineStore) WithFallback(fb OfflineStore) *RedisOfflineStore {
	rs.fallback = fb
	return rs
}

// Close closes the underlying Redis client. Safe to call on nil receiver.
func (rs *RedisOfflineStore) Close() error {
	if rs == nil || rs.client == nil {
		return nil
	}
	return rs.client.Close()
}

// offlineKey returns the Redis key for a user's offline queue.
func (rs *RedisOfflineStore) offlineKey(uid string) string {
	return rs.keyPrefix + uid
}

// Lua script: atomically push to list and trim to maxSize.
// KEYS[1] = list key, ARGV[1] = protobuf binary payload, ARGV[2] = maxSize.
var storeScript = redis.NewScript(`
	redis.call('RPUSH', KEYS[1], ARGV[1])
	local len = redis.call('LLEN', KEYS[1])
	local maxSize = tonumber(ARGV[2])
	if len > maxSize then
		redis.call('LTRIM', KEYS[1], -maxSize, -1)
	end
	return len
`)

// Lua script: atomically read all elements and delete the key.
// KEYS[1] = list key.
var drainScript = redis.NewScript(`
	local msgs = redis.call('LRANGE', KEYS[1], 0, -1)
	if #msgs > 0 then
		redis.call('DEL', KEYS[1])
	end
	return msgs
`)

// StoreOffline queues a message for an offline user.
// Falls back to in-memory store if Redis is unavailable.
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

// DrainOffline retrieves and clears all offline messages for a user.
// Falls back to in-memory store if Redis is unavailable.
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
		// No messages or unexpected type
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

// Ensure RedisOfflineStore implements OfflineStore.
var _ OfflineStore = (*RedisOfflineStore)(nil)
