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

const defaultReadPolicy = ReadPolicyLocal

type Cluster struct {
	Self              Node
	Ring              *Ring
	failures          sync.Map
	pool              *ConnPool
	ReplicationFactor int
	readCounter       atomic.Uint64 // für round-robin
	replicationFactor atomic.Int32
	readPolicy        atomic.Value
	rebalancing       sync.Map
	replicationQueues sync.Map
	fanoutQueues      sync.Map
	routeCache        sync.Map
	outbox            *replicationOutbox
	replicationQSize  int
	fanoutQSize       int
}

type routeInfo struct {
	nodes     []Node
	primary   Node
	replicas  []Node
	isLocal   bool
	isPrimary bool
}

func New(self Node, replicas int, apiKey string, replicationFactor int) *Cluster {
	c := &Cluster{
		Self:              self,
		Ring:              NewRing(replicas),
		pool:              NewConnPool(apiKey),
		ReplicationFactor: replicationFactor,
		replicationQSize:  asyncReplicationQueueSize,
		fanoutQSize:       asyncFanoutQueueSize,
	}
	c.Ring.Add(self) // self immer zuerst adden
	c.replicationFactor.Store(int32(replicationFactor))
	c.readPolicy.Store(defaultReadPolicy)
	return c
}

func (c *Cluster) SetQueueSizes(replicationQueueSize, fanoutQueueSize int) {
	if replicationQueueSize > 0 {
		c.replicationQSize = replicationQueueSize
	}
	if fanoutQueueSize > 0 {
		c.fanoutQSize = fanoutQueueSize
	}
}

func (c *Cluster) GetReplicationFactor() int {
	return int(c.replicationFactor.Load())
}

func (c *Cluster) SetReplicationFactor(n int) {
	if n < 1 {
		n = 1
	}
	c.replicationFactor.Store(int32(n))
	c.invalidateRoutes()
}

func (c *Cluster) SetReadPolicy(p ReadPolicy) {
	if p == "" {
		p = defaultReadPolicy
	}
	c.readPolicy.Store(p)
}

func (c *Cluster) getReadPolicy() ReadPolicy {
	if v := c.readPolicy.Load(); v != nil {
		return v.(ReadPolicy)
	}
	return defaultReadPolicy
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
	route := c.getRoute(metric)
	switch policy {
	case ReadPolicyPrimary:
		if route.primary.ID == "" {
			return c.Self
		}
		return route.primary
	case ReadPolicyLocal:
		if route.isLocal {
			return c.Self
		}
		// fallback: primary
		if route.primary.ID == "" {
			return c.Self
		}
		return route.primary
	default: // rr
		if len(route.nodes) == 0 {
			return c.Self
		}
		idx := c.readCounter.Add(1) % uint64(len(route.nodes))
		return route.nodes[idx]
	}
}

func (c *Cluster) SendToNode(node Node, query string) ([]byte, error) {
	return c.pool.Send(node, query)
}

func (c *Cluster) SendToNodeLocal(node Node, query string) ([]byte, error) {
	return c.pool.Send(node, query+" __local")
}

func (c *Cluster) ForwardToPrimary(metric, apiKey, query string) ([]byte, error) {
	route := c.getRoute(metric)
	if route.primary.ID == "" {
		return nil, fmt.Errorf("no primary found for metric: %s", metric)
	}
	if route.primary.ID == c.Self.ID {
		return nil, fmt.Errorf("self is primary, should not forward")
	}
	return c.pool.Send(route.primary, query)
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

func (c *Cluster) Topology() TopologyInfo {
	return TopologyInfo{
		Cluster:           "ok",
		Self:              NodeInfo{ID: c.Self.ID, Addr: c.Self.Addr},
		Nodes:             c.GetAllNodes(),
		ReplicationFactor: c.GetReplicationFactor(),
		VirtualNodes:      c.Ring.Replicas(),
		ReadPolicy:        string(c.getReadPolicy()),
	}
}

func (c *Cluster) ForwardWithFailover(metric, apiKey, query string) ([]byte, error) {
	route := c.getRoute(metric)

	var lastErr error
	for _, node := range route.nodes {
		if node.ID == c.Self.ID {
			continue
		}
		result, err := c.pool.Send(node, query+" __local")
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
	return c.getRoute(metric).replicas
}

func (c *Cluster) GetWriteNode(metric string) (Node, bool) {
	route := c.getRoute(metric)
	for _, node := range route.nodes {
		if node.ID != c.Self.ID {
			return node, true
		}
	}
	return Node{}, false
}

func (c *Cluster) GetPrimaryNode(metric string) (Node, bool) {
	primary := c.getRoute(metric).primary
	return primary, primary.ID != ""
}

func (c *Cluster) IsPrimaryFor(metric string) bool {
	return c.getRoute(metric).isPrimary
}

// IsLocal jetzt: bin ich primary ODER replica für diese metric?
func (c *Cluster) IsLocal(metric string) bool {
	return c.getRoute(metric).isLocal
}

func (c *Cluster) AddNode(node Node) {
	c.Ring.Add(node)
	c.invalidateRoutes()
}

func (c *Cluster) RemoveNode(nodeID string) {
	c.Ring.Remove(nodeID)
	c.invalidateRoutes()
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

func (c *Cluster) getRoute(metric string) routeInfo {
	if cached, ok := c.routeCache.Load(metric); ok {
		return cached.(routeInfo)
	}

	nodes := c.Ring.GetN(metric, c.GetReplicationFactor())
	info := routeInfo{nodes: nodes}
	if len(nodes) > 0 {
		info.primary = nodes[0]
		info.isPrimary = info.primary.ID == c.Self.ID
	}

	for _, node := range nodes {
		if node.ID == c.Self.ID {
			info.isLocal = true
			continue
		}
		info.replicas = append(info.replicas, node)
	}

	actual, _ := c.routeCache.LoadOrStore(metric, info)
	return actual.(routeInfo)
}

func (c *Cluster) invalidateRoutes() {
	c.routeCache.Range(func(key, _ any) bool {
		c.routeCache.Delete(key)
		return true
	})
}
