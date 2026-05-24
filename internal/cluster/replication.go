package cluster

import (
	"fmt"
	"log"
)

type ReplicationResult struct {
	NodeID  string
	Success bool
	Error   error
}

// ReplicateAsync — fire and forget, client wartet nicht
func (c *Cluster) ReplicateAsync(metric, query string) {
	replicas := c.GetReplicaNodes(metric)
	if len(replicas) == 0 {
		return
	}

	for _, replica := range replicas {
		go func(node Node) {
			_, err := c.pool.Send(node, query)
			if err != nil {
				log.Printf("async replication to %s failed: %v", node.ID, err)
			}
		}(replica)
	}
}

// ReplicateQuorum — warte bis majority replicas geschrieben haben
func (c *Cluster) ReplicateQuorum(metric, query string) error {
	replicas := c.GetReplicaNodes(metric)
	if len(replicas) == 0 {
		return nil // single node, kein quorum nötig
	}

	// majority = mehr als die hälfte aller nodes (inkl. primary der schon geschrieben hat)
	totalNodes := len(replicas) + 1 // +1 für primary
	majority := totalNodes/2 + 1
	needed := majority - 1 // primary hat schon geschrieben

	results := make(chan ReplicationResult, len(replicas))

	for _, replica := range replicas {
		go func(node Node) {
			_, err := c.pool.Send(node, query)
			results <- ReplicationResult{
				NodeID:  node.ID,
				Success: err == nil,
				Error:   err,
			}
		}(replica)
	}

	// warte bis genug replicas geantwortet haben
	succeeded := 0
	failed := 0
	maxFail := len(replicas) - needed

	for i := 0; i < len(replicas); i++ {
		r := <-results
		if r.Success {
			succeeded++
			if succeeded >= needed {
				return nil // quorum erreicht
			}
		} else {
			failed++
			log.Printf("quorum replication to %s failed: %v", r.NodeID, r.Error)
			if failed > maxFail {
				return fmt.Errorf("quorum not reached: %d/%d replicas failed", failed, len(replicas))
			}
		}
	}

	return nil
}

// ReplicateWrite — entscheidet ob async oder quorum
func (c *Cluster) ReplicateWrite(metric, query string, quorum bool) error {
	if quorum {
		return c.ReplicateQuorum(metric, query)
	}
	c.ReplicateAsync(metric, query)
	return nil
}
