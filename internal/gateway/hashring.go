package gateway

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

// HashRing 使用带虚拟节点的一致性哈希,在多节点集群中
// 将 UID 确定性地路由到 Gateway 节点。
//
// 每个物理节点由分布在环上的多个虚拟节点(副本)表示。
// 查找键时,环会找到第一个哈希值 >= 键哈希值的虚拟节点(回绕到 0)。
type HashRing struct {
	mu        sync.RWMutex
	ring      []uint32          // 虚拟节点哈希值的有序切片
	nodes     map[uint32]string // 哈希 → 物理节点 ID
	replicas  int               // 每个物理节点的虚拟节点数
	physCount int               // 物理节点数量(缓存以便 Len 以 O(1) 完成)
}

// NewHashRing 创建一个 HashRing,每个物理节点带给定数量的虚拟副本。
// 如果 replicas <= 0,默认为 150。
func NewHashRing(replicas int) *HashRing {
	if replicas <= 0 {
		replicas = 150
	}
	return &HashRing{
		ring:     make([]uint32, 0),
		nodes:    make(map[uint32]string),
		replicas: replicas,
	}
}

// Add 将物理节点及其虚拟副本插入环中。
// 如果节点已存在,先移除其旧副本(幂等)。
func (h *HashRing) Add(nodeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// 移除该 nodeID 之前的所有记录(幂等重新注册)。
	h.removeLocked(nodeID)
	h.physCount++

	for i := 0; i < h.replicas; i++ {
		key := fmt.Sprintf("%s:%d", nodeID, i)
		hash := crc32.ChecksumIEEE([]byte(key))
		h.ring = append(h.ring, hash)
		h.nodes[hash] = nodeID
	}
	sort.Slice(h.ring, func(i, j int) bool { return h.ring[i] < h.ring[j] })
}

// Remove 从环中删除物理节点及其所有虚拟副本。
func (h *HashRing) Remove(nodeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.removeLocked(nodeID)
}

// removeLocked 不加锁地移除节点的副本。
func (h *HashRing) removeLocked(nodeID string) {
	// 收集属于该节点的哈希值。
	var toRemove []uint32
	for hash, id := range h.nodes {
		if id == nodeID {
			toRemove = append(toRemove, hash)
		}
	}
	if len(toRemove) == 0 {
		return
	}

	h.physCount--

	// 从 nodes 映射中删除。
	for _, hash := range toRemove {
		delete(h.nodes, hash)
	}

	// 重建环切片,排除已移除的哈希值。
	filtered := make([]uint32, 0, len(h.ring)-len(toRemove))
	for _, hash := range h.ring {
		if _, exists := h.nodes[hash]; exists {
			filtered = append(filtered, hash)
		}
	}
	h.ring = filtered
}

// Get 返回负责给定键的 nodeID。
// 如果环为空则返回 ""。
func (h *HashRing) Get(key string) string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ring) == 0 {
		return ""
	}

	hash := crc32.ChecksumIEEE([]byte(key))

	// 二分查找第一个 >= hash 的虚拟节点。
	idx := sort.Search(len(h.ring), func(i int) bool {
		return h.ring[i] >= hash
	})

	// 如果已越过环的末尾,则回绕到开头。
	if idx >= len(h.ring) {
		idx = 0
	}

	return h.nodes[h.ring[idx]]
}

// Len 返回环中物理节点的数量。
func (h *HashRing) Len() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.physCount
}
