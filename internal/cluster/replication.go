package cluster

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

const asyncReplicationWorkersPerNode = 64
const asyncReplicationQueueSize = 262144
const asyncReplicationBatchSize = 512
const asyncReplicationBatchWait = 2 * time.Millisecond
const asyncFanoutWorkersPerMetric = 8
const asyncFanoutQueueSize = 262144
const replicationBatchSeparator = "\x1f"
const replicationRetryInitialDelay = 10 * time.Millisecond
const replicationRetryMaxDelay = time.Second

type ReplicationResult struct {
	NodeID  string
	Success bool
	Error   error
}

type replicationQueue struct {
	node Node
	ch   chan string
}

type fanoutTask struct {
	query    string
	replicas []Node
}

type fanoutQueue struct {
	metric string
	ch     chan fanoutTask
}

// ReplicateAsync — fire and forget, client wartet nicht
func (c *Cluster) ReplicateAsync(metric, query string) {
	replicas := c.GetReplicaNodes(metric)
	if len(replicas) == 0 {
		return
	}

	c.enqueueFanout(metric, replicas, query)
}

func (c *Cluster) enqueueFanout(metric string, replicas []Node, query string) {
	q := c.getFanoutQueue(metric)
	q.ch <- fanoutTask{
		query:    query,
		replicas: replicas,
	}
}

func (c *Cluster) getFanoutQueue(metric string) *fanoutQueue {
	if existing, ok := c.fanoutQueues.Load(metric); ok {
		return existing.(*fanoutQueue)
	}

	q := &fanoutQueue{
		metric: metric,
		ch:     make(chan fanoutTask, asyncFanoutQueueSize),
	}
	actual, loaded := c.fanoutQueues.LoadOrStore(metric, q)
	if loaded {
		return actual.(*fanoutQueue)
	}

	for i := 0; i < asyncFanoutWorkersPerMetric; i++ {
		go c.fanoutWorker(q)
	}
	return q
}

func (c *Cluster) fanoutWorker(q *fanoutQueue) {
	for task := range q.ch {
		for _, replica := range task.replicas {
			c.enqueueReplication(replica, task.query)
		}
	}
}

func (c *Cluster) enqueueReplication(node Node, query string) {
	q := c.getReplicationQueue(node)
	q.ch <- query
}

func (c *Cluster) getReplicationQueue(node Node) *replicationQueue {
	if existing, ok := c.replicationQueues.Load(node.ID); ok {
		return existing.(*replicationQueue)
	}

	q := &replicationQueue{
		node: node,
		ch:   make(chan string, asyncReplicationQueueSize),
	}
	actual, loaded := c.replicationQueues.LoadOrStore(node.ID, q)
	if loaded {
		return actual.(*replicationQueue)
	}

	for i := 0; i < asyncReplicationWorkersPerNode; i++ {
		go c.replicationWorker(q)
	}
	return q
}

func (c *Cluster) replicationWorker(q *replicationQueue) {
	batch := make([]string, 0, asyncReplicationBatchSize)
	timer := time.NewTimer(asyncReplicationBatchWait)
	if !timer.Stop() {
		<-timer.C
	}

	for {
		query, ok := <-q.ch
		if !ok {
			return
		}
		batch = append(batch, query)
		timer.Reset(asyncReplicationBatchWait)

	drain:
		for len(batch) < asyncReplicationBatchSize {
			select {
			case query, ok := <-q.ch:
				if !ok {
					return
				}
				batch = append(batch, query)
			case <-timer.C:
				break drain
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}

		c.sendReplicationBatch(q.node, batch)
		batch = batch[:0]
	}
}

func (c *Cluster) sendReplicationBatch(node Node, queries []string) {
	if len(queries) == 0 {
		return
	}

	line := queries[0]
	if len(queries) == 1 {
		c.sendReplicationLineWithRetry(node, line)
		return
	}

	line = "REPL_BATCH " + strings.Join(queries, replicationBatchSeparator)
	c.sendReplicationLineWithRetry(node, line)
}

func (c *Cluster) sendReplicationLineWithRetry(node Node, line string) {
	delay := replicationRetryInitialDelay
	attempt := 0
	for {
		if _, err := c.pool.Send(node, line); err == nil {
			return
		} else {
			attempt++
			if attempt == 1 || attempt%100 == 0 {
				log.Printf("replication to %s failed, retrying (attempt=%d): %v", node.ID, attempt, err)
			}
		}

		time.Sleep(delay)
		if delay < replicationRetryMaxDelay {
			delay *= 2
			if delay > replicationRetryMaxDelay {
				delay = replicationRetryMaxDelay
			}
		}
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
	var wg sync.WaitGroup

	for _, replica := range replicas {
		wg.Add(1)
		go func(node Node) {
			defer wg.Done()
			_, err := c.pool.Send(node, query)
			results <- ReplicationResult{
				NodeID:  node.ID,
				Success: err == nil,
				Error:   err,
			}
		}(replica)
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	// warte bis genug replicas geantwortet haben
	succeeded := 0
	failed := 0
	maxFail := len(replicas) - needed

	for r := range results {
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
