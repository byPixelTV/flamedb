package cluster

import (
	"fmt"
	"log"
	"sync"
)

type Cluster struct {
	Self              Node
	Ring              *Ring
	failures          sync.Map
	pool              *ConnPool
	ReplicationFactor int
}

func New(self Node, replicas int, apiKey string, replicationFactor int) *Cluster {
	c := &Cluster{
		Self:              self,
		Ring:              NewRing(replicas),
		pool:              NewConnPool(apiKey),
		ReplicationFactor: replicationFactor,
	}
	c.Ring.Add(self) // self immer zuerst adden
	return c
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
