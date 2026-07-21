package gateway

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

// HashRing implements consistent hashing with virtual nodes for deterministic
// routing of UIDs to Gateway nodes in a multi-node cluster.
//
// Each physical node is represented by multiple virtual nodes (replicas)
// distributed around the ring. When looking up a key, the ring finds the
// first virtual node whose hash is >= the key's hash (wrapping to 0).
type HashRing struct {
	mu        sync.RWMutex
	ring      []uint32          // sorted slice of virtual node hashes
	nodes     map[uint32]string // hash → physical nodeID
	replicas  int               // virtual nodes per physical node
	physCount int               // number of physical nodes (cached for O(1) Len)
}

// NewHashRing creates a HashRing with the given number of virtual replicas
// per physical node. If replicas <= 0, defaults to 150.
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

// Add inserts a physical node into the ring with its virtual replicas.
// If the node already exists, its old replicas are removed first (idempotent).
func (h *HashRing) Add(nodeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Remove any previous entries for this nodeID (idempotent re-registration).
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

// Remove deletes a physical node and all its virtual replicas from the ring.
func (h *HashRing) Remove(nodeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.removeLocked(nodeID)
}

// removeLocked removes a node's replicas without acquiring the lock.
func (h *HashRing) removeLocked(nodeID string) {
	// Collect hashes belonging to this node.
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

	// Delete from the nodes map.
	for _, hash := range toRemove {
		delete(h.nodes, hash)
	}

	// Rebuild the ring slice excluding removed hashes.
	filtered := make([]uint32, 0, len(h.ring)-len(toRemove))
	for _, hash := range h.ring {
		if _, exists := h.nodes[hash]; exists {
			filtered = append(filtered, hash)
		}
	}
	h.ring = filtered
}

// Get returns the nodeID responsible for the given key.
// Returns "" if the ring is empty.
func (h *HashRing) Get(key string) string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ring) == 0 {
		return ""
	}

	hash := crc32.ChecksumIEEE([]byte(key))

	// Binary search for the first virtual node >= hash.
	idx := sort.Search(len(h.ring), func(i int) bool {
		return h.ring[i] >= hash
	})

	// Wrap around if we passed the end of the ring.
	if idx >= len(h.ring) {
		idx = 0
	}

	return h.nodes[h.ring[idx]]
}

// Len returns the number of physical nodes in the ring.
func (h *HashRing) Len() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.physCount
}
