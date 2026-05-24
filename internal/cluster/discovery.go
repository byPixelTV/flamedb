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
	Type       string   `json:"type"`
	NodeID     string   `json:"node_id"`
	Addr       string   `json:"addr"`
	KnownNodes []string `json:"known_nodes,omitempty"`
	// neu:
	Peers []NodeInfo `json:"peers,omitempty"`
}

type NodeInfo struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

// propagiert einen neuen node an alle bekannten nodes
func (c *Cluster) propagateJoin(newNode Node, apiKey string) {
	c.Ring.mu.RLock()
	nodes := make(map[string]Node)
	for _, n := range c.Ring.nodes {
		nodes[n.ID] = n
	}
	c.Ring.mu.RUnlock()

	for _, node := range nodes {
		// nicht an sich selbst oder den neuen node senden
		if node.ID == c.Self.ID || node.ID == newNode.ID {
			continue
		}
		go func(target Node) {
			if err := c.announceNodeToAddr(target.Addr, newNode, apiKey); err != nil {
				log.Printf("gossip to %s failed: %v", target.ID, err)
			} else {
				log.Printf("gossiped %s to %s", newNode.ID, target.ID)
			}
		}(node)
	}
}

// announced einen spezifischen node an eine adresse
func (c *Cluster) announceNodeToAddr(addr string, node Node, apiKey string) error {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	scanner.Scan() // {"auth":"required"}
	fmt.Fprintf(conn, "AUTH %s\n", apiKey)
	scanner.Scan() // {"auth":"ok"}

	c.Ring.mu.RLock()
	known := make([]string, 0)
	for _, n := range c.Ring.nodes {
		known = append(known, n.ID)
	}
	c.Ring.mu.RUnlock()
	// deduplizieren
	seen := make(map[string]bool)
	unique := known[:0]
	for _, id := range known {
		if !seen[id] {
			seen[id] = true
			unique = append(unique, id)
		}
	}

	msg := DiscoveryMessage{
		Type:       "JOIN",
		NodeID:     node.ID,
		Addr:       node.Addr,
		KnownNodes: unique,
	}
	data, _ := json.Marshal(msg)
	fmt.Fprintf(conn, "CLUSTER %s\n", string(data))
	scanner.Scan()
	return nil
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

	// response lesen — enthält peers
	if scanner.Scan() {
		var resp struct {
			Cluster string     `json:"cluster"`
			Peers   []NodeInfo `json:"peers"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &resp); err == nil {
			for _, peer := range resp.Peers {
				if peer.ID != c.Self.ID {
					c.AddNode(Node{ID: peer.ID, Addr: peer.Addr})
					log.Printf("learned about peer %s (%s)", peer.ID, peer.Addr)
				}
			}
		}
	}
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
