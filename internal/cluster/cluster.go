package cluster

import (
	"bufio"
	"fmt"
	"net"
	"sync"
)

type Cluster struct {
	Self     Node
	Ring     *Ring
	failures sync.Map
}

func New(self Node, replicas int) *Cluster {
	c := &Cluster{
		Self: self,
		Ring: NewRing(replicas),
	}
	c.Ring.Add(self)
	return c
}

func (c *Cluster) AddNode(node Node) {
	c.Ring.Add(node)
}

func (c *Cluster) RemoveNode(nodeID string) {
	c.Ring.Remove(nodeID)
}

func (c *Cluster) IsLocal(metric string) bool {
	node, ok := c.Ring.Get(metric)
	if !ok {
		return true // fallback: lokal handlen
	}
	return node.ID == c.Self.ID
}

func (c *Cluster) Forward(metric, apiKey, query string) ([]byte, error) {
	node, ok := c.Ring.Get(metric)
	if !ok {
		return nil, fmt.Errorf("no node found for metric: %s", metric)
	}

	conn, err := net.Dial("tcp", node.Addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", node.Addr, err)
	}
	defer conn.Close()

	scanner := bufio.NewScanner(conn)

	// auth
	scanner.Scan() // {"auth":"required"}
	fmt.Fprintf(conn, "AUTH %s\n", apiKey)
	scanner.Scan() // {"auth":"ok"}

	// query senden
	fmt.Fprintf(conn, "%s\n", query)
	scanner.Scan()

	return scanner.Bytes(), nil
}
