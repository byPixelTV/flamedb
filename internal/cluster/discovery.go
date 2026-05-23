package cluster

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"
)

type DiscoveryMessage struct {
	Type   string `json:"type"`
	NodeID string `json:"node_id"`
	Addr   string `json:"addr"`
}

// beim startup: announce zu allen seeds
func (c *Cluster) JoinSeeds(seeds []string, apiKey string) {
	for _, seed := range seeds {
		if err := c.announceToAddr(seed, apiKey); err != nil {
			log.Printf("could not reach seed %s: %v", seed, err)
		} else {
			log.Printf("joined via seed %s", seed)
		}
	}
}

func (c *Cluster) announceToAddr(addr, apiKey string) error {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	scanner.Scan() // {"auth":"required"}
	fmt.Fprintf(conn, "AUTH %s\n", apiKey)
	scanner.Scan() // {"auth":"ok"}

	msg := DiscoveryMessage{
		Type:   "JOIN",
		NodeID: c.Self.ID,
		Addr:   c.Self.Addr,
	}
	data, _ := json.Marshal(msg)
	fmt.Fprintf(conn, "CLUSTER %s\n", string(data))
	scanner.Scan()
	return nil
}

// heartbeat — nur nodes die bereits im ring sind
func (c *Cluster) StartHeartbeat(apiKey string) {
	go func() {
		for {
			time.Sleep(5 * time.Second)
			c.pingAll(apiKey)
		}
	}()
}

func (c *Cluster) pingAll(apiKey string) {
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
		conn, err := net.DialTimeout("tcp", node.Addr, 2*time.Second)
		if err != nil {
			c.recordFailure(node.ID)
			continue
		}
		c.clearFailure(node.ID)
		conn.Close()
	}
}

func (c *Cluster) recordFailure(nodeID string) {
	val, _ := c.failures.LoadOrStore(nodeID, 0)
	count := val.(int) + 1
	c.failures.Store(nodeID, count)
	if count >= 3 {
		log.Printf("node %s unreachable, removing from ring", nodeID)
		c.Ring.Remove(nodeID)
		c.failures.Delete(nodeID)
	} else {
		log.Printf("node %s ping failed (%d/3)", nodeID, count)
	}
}

func (c *Cluster) clearFailure(nodeID string) {
	c.failures.Delete(nodeID)
}
