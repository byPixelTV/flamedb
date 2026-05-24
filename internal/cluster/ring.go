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

func (r *Ring) Replicas() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.replicas
}

func (r *Ring) Add(node Node) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// check ob node bereits im ring ist
	for _, existing := range r.nodes {
		if existing.ID == node.ID {
			return // bereits drin, nichts tun
		}
	}

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

func (r *Ring) GetN(metric string, n int) []Node {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.keys) == 0 || n <= 0 {
		return nil
	}

	hash := hashKey(metric)
	idx := sort.Search(len(r.keys), func(i int) bool {
		return r.keys[i] >= hash
	})
	if idx >= len(r.keys) {
		idx = 0
	}

	seen := make(map[string]bool)
	var nodes []Node

	// iterate über alle keys, nicht nur n keys
	for i := 0; i < len(r.keys); i++ {
		pos := (idx + i) % len(r.keys)
		node := r.nodes[r.keys[pos]]
		if !seen[node.ID] {
			seen[node.ID] = true
			nodes = append(nodes, node)
		}
		if len(nodes) >= n {
			break
		}
	}

	return nodes
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
