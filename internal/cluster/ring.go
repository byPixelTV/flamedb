package cluster

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

type Node struct {
	ID   string
	Addr string // z.b. "192.168.1.1:7777"
}

type Ring struct {
	mu       sync.RWMutex
	nodes    map[uint64]Node // hash → node
	keys     []uint64        // sorted hashes
	replicas int             // virtual nodes pro physical node
}

func NewRing(replicas int) *Ring {
	return &Ring{
		nodes:    make(map[uint64]Node),
		replicas: replicas,
	}
}

func (r *Ring) Add(node Node) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < r.replicas; i++ {
		hash := hashKey(fmt.Sprintf("%s:%d", node.ID, i))
		r.nodes[hash] = node
		r.keys = append(r.keys, hash)
	}
	sort.Slice(r.keys, func(i, j int) bool {
		return r.keys[i] < r.keys[j]
	})
}

func (r *Ring) Remove(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < r.replicas; i++ {
		hash := hashKey(fmt.Sprintf("%s:%d", nodeID, i))
		delete(r.nodes, hash)
	}

	// rebuild keys
	r.keys = r.keys[:0]
	for k := range r.nodes {
		r.keys = append(r.keys, k)
	}
	sort.Slice(r.keys, func(i, j int) bool {
		return r.keys[i] < r.keys[j]
	})
}

func (r *Ring) Get(metric string) (Node, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.keys) == 0 {
		return Node{}, false
	}

	hash := hashKey(metric)

	// binary search für nächsten node im ring
	idx := sort.Search(len(r.keys), func(i int) bool {
		return r.keys[i] >= hash
	})

	// wrap around
	if idx >= len(r.keys) {
		idx = 0
	}

	return r.nodes[r.keys[idx]], true
}

func hashKey(key string) uint64 {
	h := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint64(h[:8])
}
