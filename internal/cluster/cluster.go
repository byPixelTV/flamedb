package cluster

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

type ReadPolicy string

const (
	ReadPolicyRoundRobin ReadPolicy = "rr"
	ReadPolicyPrimary    ReadPolicy = "primary"
	ReadPolicyLocal      ReadPolicy = "local"
)

type Cluster struct {
	Self              Node
	Ring              *Ring
	failures          sync.Map
	pool              *ConnPool
	ReplicationFactor int
	readCounter       atomic.Uint64 // für round-robin
	replicationFactor atomic.Int32
	readPolicy        atomic.Value
}

func New(self Node, replicas int, apiKey string, replicationFactor int) *Cluster {
	c := &Cluster{
		Self:              self,
		Ring:              NewRing(replicas),
		pool:              NewConnPool(apiKey),
		ReplicationFactor: replicationFactor,
	}
	c.Ring.Add(self) // self immer zuerst adden
	c.replicationFactor.Store(int32(replicationFactor))
	c.readPolicy.Store(ReadPolicyRoundRobin)
	return c
}

func (c *Cluster) GetReplicationFactor() int {
	return int(c.replicationFactor.Load())
}

func (c *Cluster) SetReplicationFactor(n int) {
	if n < 1 {
		n = 1
	}
	c.replicationFactor.Store(int32(n))
}

func (c *Cluster) SetReadPolicy(p ReadPolicy) {
	if p == "" {
		p = ReadPolicyRoundRobin
	}
	c.readPolicy.Store(p)
}

func (c *Cluster) getReadPolicy() ReadPolicy {
	if v := c.readPolicy.Load(); v != nil {
		return v.(ReadPolicy)
	}
	return ReadPolicyRoundRobin
}

func (c *Cluster) BroadcastConfig(msg DiscoveryMessage) {
	msg.Type = "SET_CONFIG"
	msg.Propagate = false

	payload, _ := json.Marshal(msg)

	c.Ring.mu.RLock()
	nodes := make(map[string]Node)
	for _, n := range c.Ring.nodes {
		nodes[n.ID] = n
	}
	c.Ring.mu.RUnlock()

	for _, node := range nodes {
		if node.ID == c.Self.ID {
			continue
		}
		_, _ = c.pool.Send(node, "CLUSTER "+string(payload))
	}
}

func (c *Cluster) GetReadNode(metric string) Node {
	policy := c.getReadPolicy()
	switch policy {
	case ReadPolicyPrimary:
		nodes := c.Ring.GetN(metric, 1)
		if len(nodes) == 0 {
			return c.Self
		}
		return nodes[0]
	case ReadPolicyLocal:
		if c.IsLocal(metric) {
			return c.Self
		}
		// fallback: primary
		nodes := c.Ring.GetN(metric, 1)
		if len(nodes) == 0 {
			return c.Self
		}
		return nodes[0]
	default: // rr
		nodes := c.Ring.GetN(metric, c.GetReplicationFactor())
		if len(nodes) == 0 {
			return c.Self
		}
		idx := c.readCounter.Add(1) % uint64(len(nodes))
		return nodes[idx]
	}
}

func (c *Cluster) SendToNode(node Node, query string) ([]byte, error) {
	return c.pool.Send(node, query)
}

func (c *Cluster) ForwardToPrimary(metric, apiKey, query string) ([]byte, error) {
	nodes := c.Ring.GetN(metric, 1)
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no primary found for metric: %s", metric)
	}
	primary := nodes[0]
	if primary.ID == c.Self.ID {
		return nil, fmt.Errorf("self is primary, should not forward")
	}
	return c.pool.Send(primary, query)
}

func (c *Cluster) GetAllNodes() []NodeInfo {
	c.Ring.mu.RLock()
	defer c.Ring.mu.RUnlock()
	seen := make(map[string]bool)
	var nodes []NodeInfo
	for _, n := range c.Ring.nodes {
		if !seen[n.ID] {
			seen[n.ID] = true
			nodes = append(nodes, NodeInfo{ID: n.ID, Addr: n.Addr})
		}
	}
	return nodes
}

func (c *Cluster) ForwardWithFailover(metric, apiKey, query string) ([]byte, error) {
	nodes := c.Ring.GetN(metric, c.ReplicationFactor)

	var lastErr error
	for _, node := range nodes {
		if node.ID == c.Self.ID {
			continue
		}
		result, err := c.pool.Send(node, query)
		if err != nil {
			lastErr = err
			log.Printf("forward to %s failed, trying next replica: %v", node.ID, err)
			continue
		}
		return result, nil
	}
	return nil, fmt.Errorf("all nodes failed: %v", lastErr)
}

func (c *Cluster) GetReplicaNodes(metric string) []Node {
	nodes := c.Ring.GetN(metric, c.ReplicationFactor)
	// primary rausfiltern, nur replicas
	var replicas []Node
	for _, n := range nodes {
		if n.ID != c.Self.ID {
			replicas = append(replicas, n)
		}
	}
	return replicas
}

func (c *Cluster) IsPrimaryFor(metric string) bool {
	nodes := c.Ring.GetN(metric, 1)
	if len(nodes) == 0 {
		return true
	}
	return nodes[0].ID == c.Self.ID
}

// IsLocal jetzt: bin ich primary ODER replica für diese metric?
func (c *Cluster) IsLocal(metric string) bool {
	nodes := c.Ring.GetN(metric, c.ReplicationFactor)
	for _, n := range nodes {
		if n.ID == c.Self.ID {
			return true
		}
	}
	return false
}

func (c *Cluster) AddNode(node Node) {
	c.Ring.Add(node)
}

func (c *Cluster) RemoveNode(nodeID string) {
	c.Ring.Remove(nodeID)
}

func (c *Cluster) PropagateJoin(node Node, apiKey string) {
	c.propagateJoin(node, apiKey)
}

func (c *Cluster) HandleJoin(node Node, apiKey string) {
	c.AddNode(node)
	go c.propagateJoin(node, apiKey)
}

func (c *Cluster) Knows(nodeID string) bool {
	c.Ring.mu.RLock()
	defer c.Ring.mu.RUnlock()
	for _, n := range c.Ring.nodes {
		if n.ID == nodeID {
			return true
		}
	}
	return false
}
