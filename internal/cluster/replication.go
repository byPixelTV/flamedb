package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
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

var ErrServerBusy = errors.New("server busy")

type ReplicationResult struct {
	NodeID  string
	Success bool
	Error   error
}

type ReplicationItem struct {
	Metric string
	Query  string
	Quorum bool
}

type ReplicationStats struct {
	OutboxDepth       int64          `json:"outbox_depth"`
	FanoutQueueDepth  map[string]int `json:"fanout_queue_depth"`
	ReplicaQueueDepth map[string]int `json:"replica_queue_depth"`
}

type replicationQueue struct {
	node Node
	ch   chan replicationRecord
}

type fanoutTask struct {
	records []replicationRecord
}

type fanoutQueue struct {
	metric string
	ch     chan fanoutTask
}

type replicationRecord struct {
	node      Node
	query     string
	outboxKey []byte
}

type outboxEntry struct {
	Node  Node   `json:"node"`
	Query string `json:"query"`
}

type replicationOutbox struct {
	db    *pebble.DB
	seq   atomic.Uint64
	depth atomic.Int64
}

func newReplicationOutbox(db *pebble.DB) *replicationOutbox {
	o := &replicationOutbox{db: db}
	o.seq.Store(uint64(time.Now().UnixNano()))
	return o
}

func (o *replicationOutbox) put(node Node, query string) ([]byte, error) {
	keys, err := o.putBatch([]replicationRecord{{node: node, query: query}})
	if err != nil || len(keys) == 0 {
		return nil, err
	}
	return keys[0], nil
}

func (o *replicationOutbox) putBatch(records []replicationRecord) ([][]byte, error) {
	if o == nil || o.db == nil {
		return nil, nil
	}
	batch := o.db.NewBatch()
	defer batch.Close()

	keys := make([][]byte, 0, len(records))
	for _, record := range records {
		seq := o.seq.Add(1)
		key := []byte(fmt.Sprintf("repl-outbox:%s:%020d", record.node.ID, seq))
		value, err := json.Marshal(outboxEntry{Node: record.node, Query: record.query})
		if err != nil {
			return nil, err
		}
		if err := batch.Set(key, value, nil); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		return nil, err
	}
	o.depth.Add(int64(len(keys)))
	return keys, nil
}

func (o *replicationOutbox) deleteBatch(keys [][]byte) {
	if o == nil || o.db == nil || len(keys) == 0 {
		return
	}
	batch := o.db.NewBatch()
	defer batch.Close()
	for _, key := range keys {
		if len(key) == 0 {
			continue
		}
		_ = batch.Delete(key, nil)
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		log.Printf("replication outbox delete failed: %v", err)
		return
	}
	o.depth.Add(-int64(len(keys)))
}

func (c *Cluster) AttachReplicationOutbox(db *pebble.DB) {
	if db == nil {
		return
	}
	c.outbox = newReplicationOutbox(db)
	c.recoverReplicationOutbox()
}

func (c *Cluster) recoverReplicationOutbox() {
	if c.outbox == nil || c.outbox.db == nil {
		return
	}
	iter, err := c.outbox.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("repl-outbox:"),
		UpperBound: []byte("repl-outbox;"),
	})
	if err != nil {
		log.Printf("replication outbox recovery failed: %v", err)
		return
	}
	defer iter.Close()

	recovered := 0
	for iter.First(); iter.Valid(); iter.Next() {
		var entry outboxEntry
		if err := json.Unmarshal(iter.Value(), &entry); err != nil {
			log.Printf("replication outbox decode failed for %s: %v", string(iter.Key()), err)
			continue
		}
		key := append([]byte(nil), iter.Key()...)
		c.enqueueReplicationRecord(replicationRecord{
			node:      entry.Node,
			query:     entry.Query,
			outboxKey: key,
		})
		recovered++
	}
	c.outbox.depth.Store(int64(recovered))
	if err := iter.Error(); err != nil {
		log.Printf("replication outbox recovery iterator failed: %v", err)
	}
	if recovered > 0 {
		log.Printf("recovered %d pending replication outbox records", recovered)
	}
}

// ReplicateAsync — fire and forget, client wartet nicht
func (c *Cluster) ReplicateAsync(metric, query string) error {
	replicas := c.GetReplicaNodes(metric)
	if len(replicas) == 0 {
		return nil
	}

	records := make([]replicationRecord, 0, len(replicas))
	for _, replica := range replicas {
		key, err := c.persistReplication(replica, query)
		if err != nil {
			return fmt.Errorf("replication outbox persist failed for %s: %w", replica.ID, err)
		}
		records = append(records, replicationRecord{
			node:      replica,
			query:     query,
			outboxKey: key,
		})
	}

	if err := c.enqueueFanout(metric, records); err != nil {
		return err
	}
	return nil
}

func (c *Cluster) ReplicateBatch(items []ReplicationItem) error {
	if len(items) == 0 {
		return nil
	}

	type pendingRecord struct {
		metric string
		record replicationRecord
	}
	var pending []pendingRecord
	for _, item := range items {
		if item.Quorum {
			if err := c.ReplicateQuorum(item.Metric, item.Query); err != nil {
				return err
			}
			continue
		}
		replicas := c.GetReplicaNodes(item.Metric)
		for _, replica := range replicas {
			pending = append(pending, pendingRecord{
				metric: item.Metric,
				record: replicationRecord{
					node:  replica,
					query: item.Query,
				},
			})
		}
	}
	if len(pending) == 0 {
		return nil
	}

	asyncRecords := make([]replicationRecord, 0, len(pending))
	for _, p := range pending {
		asyncRecords = append(asyncRecords, p.record)
	}
	if c.outbox != nil {
		keys, err := c.outbox.putBatch(asyncRecords)
		if err != nil {
			return err
		}
		for i := range asyncRecords {
			asyncRecords[i].outboxKey = keys[i]
			pending[i].record.outboxKey = keys[i]
		}
	}

	byMetric := make(map[string][]replicationRecord)
	for _, p := range pending {
		byMetric[p.metric] = append(byMetric[p.metric], p.record)
	}

	for metric, records := range byMetric {
		if err := c.enqueueFanout(metric, records); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cluster) enqueueFanout(metric string, records []replicationRecord) error {
	q := c.getFanoutQueue(metric)
	task := fanoutTask{
		records: records,
	}
	select {
	case q.ch <- task:
		return nil
	default:
		return ErrServerBusy
	}
}

func (c *Cluster) getFanoutQueue(metric string) *fanoutQueue {
	if existing, ok := c.fanoutQueues.Load(metric); ok {
		return existing.(*fanoutQueue)
	}

	q := &fanoutQueue{
		metric: metric,
		ch:     make(chan fanoutTask, c.fanoutQSize),
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
		for _, record := range task.records {
			c.enqueueReplicationRecord(record)
		}
	}
}

func (c *Cluster) persistReplication(node Node, query string) ([]byte, error) {
	if c.outbox == nil {
		return nil, nil
	}
	return c.outbox.put(node, query)
}

func (c *Cluster) enqueueReplicationRecord(record replicationRecord) {
	q := c.getReplicationQueue(record.node)
	q.ch <- record
}

func (c *Cluster) getReplicationQueue(node Node) *replicationQueue {
	if existing, ok := c.replicationQueues.Load(node.ID); ok {
		return existing.(*replicationQueue)
	}

	q := &replicationQueue{
		node: node,
		ch:   make(chan replicationRecord, c.replicationQSize),
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
	batch := make([]replicationRecord, 0, asyncReplicationBatchSize)
	timer := time.NewTimer(asyncReplicationBatchWait)
	if !timer.Stop() {
		<-timer.C
	}

	for {
		record, ok := <-q.ch
		if !ok {
			return
		}
		batch = append(batch, record)
		timer.Reset(asyncReplicationBatchWait)

	drain:
		for len(batch) < asyncReplicationBatchSize {
			select {
			case record, ok := <-q.ch:
				if !ok {
					return
				}
				batch = append(batch, record)
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

func (c *Cluster) sendReplicationBatch(node Node, records []replicationRecord) {
	if len(records) == 0 {
		return
	}

	queries := make([]string, 0, len(records))
	keys := make([][]byte, 0, len(records))
	for _, record := range records {
		queries = append(queries, record.query)
		if len(record.outboxKey) > 0 {
			keys = append(keys, record.outboxKey)
		}
	}

	line := queries[0]
	if len(queries) == 1 {
		c.sendReplicationLineWithRetry(node, line)
		if c.outbox != nil {
			c.outbox.deleteBatch(keys)
		}
		return
	}

	line = "REPL_BATCH " + strings.Join(queries, replicationBatchSeparator)
	c.sendReplicationLineWithRetry(node, line)
	if c.outbox != nil {
		c.outbox.deleteBatch(keys)
	}
}

func (c *Cluster) sendReplicationLineWithRetry(node Node, line string) {
	delay := replicationRetryInitialDelay
	attempt := 0
	for {
		if data, err := c.pool.Send(node, line); err == nil {
			if err := replicationResponseError(data); err == nil {
				return
			} else {
				attempt++
				if attempt == 1 || attempt%100 == 0 {
					log.Printf("replication to %s returned error, retrying (attempt=%d): %v", node.ID, attempt, err)
				}
			}
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

func replicationResponseError(data []byte) error {
	var resp map[string]any
	if err := json.Unmarshal(data, &resp); err != nil {
		return fmt.Errorf("invalid replication response: %w", err)
	}
	if errValue, ok := resp["error"].(string); ok && errValue != "" {
		return errors.New(errValue)
	}
	if okValue, ok := resp["ok"].(bool); ok && !okValue {
		return errors.New("replication batch failed")
	}
	return nil
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
	return c.ReplicateAsync(metric, query)
}

func (c *Cluster) ReplicationStats() ReplicationStats {
	stats := ReplicationStats{
		FanoutQueueDepth:  make(map[string]int),
		ReplicaQueueDepth: make(map[string]int),
	}
	if c.outbox != nil {
		stats.OutboxDepth = c.outbox.depth.Load()
	}
	c.fanoutQueues.Range(func(key, value any) bool {
		q := value.(*fanoutQueue)
		stats.FanoutQueueDepth[key.(string)] = len(q.ch)
		return true
	})
	c.replicationQueues.Range(func(key, value any) bool {
		q := value.(*replicationQueue)
		stats.ReplicaQueueDepth[key.(string)] = len(q.ch)
		return true
	})
	return stats
}
