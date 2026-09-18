package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

const asyncReplicationWorkersPerNode = 1
const asyncReplicationQueueSize = 262144
const asyncReplicationBatchSize = 512
const asyncReplicationBatchWait = 2 * time.Millisecond
const asyncFanoutWorkersPerMetric = 1
const asyncFanoutQueueSize = 1024
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
	ack       chan string
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
	if db != nil {
		iter, err := db.NewIter(&pebble.IterOptions{LowerBound: []byte("repl-outbox:"), UpperBound: []byte("repl-outbox;")})
		if err == nil {
			for iter.First(); iter.Valid(); iter.Next() {
				key := string(iter.Key())
				if at := strings.LastIndexByte(key, ':'); at >= 0 {
					if seq, err := strconv.ParseUint(key[at+1:], 10, 64); err == nil && seq > o.seq.Load() {
						o.seq.Store(seq)
					}
				}
			}
			iter.Close()
		}
	}
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

	if err := batch.Commit(pebble.Sync); err != nil {
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
	c.recoveryDone = make(chan struct{})
	c.startWorker(func() { defer close(c.recoveryDone); c.recoverReplicationOutbox() })
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
		select {
		case <-c.done:
			return
		default:
		}
		var entry outboxEntry
		if err := json.Unmarshal(iter.Value(), &entry); err != nil {
			log.Printf("replication outbox decode failed for %s: %v", string(iter.Key()), err)
			continue
		}
		key := append([]byte(nil), iter.Key()...)
		c.outbox.depth.Add(1)
		c.enqueueReplicationRecord(replicationRecord{
			node:      entry.Node,
			query:     entry.Query,
			outboxKey: key,
		})
		recovered++
	}

	if err := iter.Error(); err != nil {
		log.Printf("replication outbox recovery iterator failed: %v", err)
	}
	if recovered > 0 {
		log.Printf("recovered %d pending replication outbox records", recovered)
	}
}

// ReplicateAsync — fire and forget, client wartet nicht
func (c *Cluster) ReplicateAsync(metric, query string) error {
	return c.ReplicateBatch([]ReplicationItem{{Metric: metric, Query: query}})
}

func (c *Cluster) ReplicateBatch(items []ReplicationItem) error {
	if err := c.waitRecovery(); err != nil {
		return err
	}
	start := 0
	for i, item := range items {
		if !item.Quorum {
			continue
		}
		if err := c.replicateAsyncBatch(items[start:i]); err != nil {
			return err
		}
		if err := c.ReplicateQuorum(item.Metric, item.Query); err != nil {
			return err
		}
		start = i + 1
	}
	return c.replicateAsyncBatch(items[start:])
}

func (c *Cluster) replicateAsyncBatch(items []ReplicationItem) error {
	var records []replicationRecord
	var metrics []string
	for _, item := range items {
		for _, node := range c.GetReplicaNodes(item.Metric) {
			records = append(records, replicationRecord{node: node, query: item.Query})
			metrics = append(metrics, item.Metric)
		}
	}
	if len(records) == 0 {
		return nil
	}
	if c.outbox != nil {
		keys, err := c.outbox.putBatch(records)
		if err != nil {
			return err
		}
		for i := range records {
			records[i].outboxKey = keys[i]
		}
	}
	grouped := make(map[string][]replicationRecord)
	for i, record := range records {
		grouped[metrics[i]] = append(grouped[metrics[i]], record)
	}
	for metric, batch := range grouped {
		if err := c.enqueueFanout(metric, batch); err != nil {
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
	case <-c.done:
		return ErrServerBusy
	}
}

func (c *Cluster) getFanoutQueue(metric string) *fanoutQueue {
	h := fnv.New32a()
	_, _ = h.Write([]byte(metric))
	metric = fmt.Sprintf("shard-%d", h.Sum32()%16)
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
		c.startWorker(func() { c.fanoutWorker(q) })
	}
	return q
}

func (c *Cluster) fanoutWorker(q *fanoutQueue) {
	for {
		select {
		case <-c.done:
			return
		case task := <-q.ch:
			for _, record := range task.records {
				c.enqueueReplicationRecord(record)
			}
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
	select {
	case q.ch <- record:
	case <-c.done:
	}
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
		c.startWorker(func() { c.replicationWorker(q) })
	}
	return q
}

func (c *Cluster) replicationWorker(q *replicationQueue) {
	batch := make([]replicationRecord, 0, asyncReplicationBatchSize)
	timer := time.NewTimer(asyncReplicationBatchWait)
	defer timer.Stop()
	if !timer.Stop() {
		<-timer.C
	}

	for {
		var record replicationRecord
		select {
		case <-c.done:
			return
		case record = <-q.ch:
		}
		batch = append(batch, record)
		timer.Reset(asyncReplicationBatchWait)

	drain:
		for len(batch) < asyncReplicationBatchSize {
			select {
			case <-c.done:
				return
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

	// Bound each network frame even when individual commands are large.
	size := 0
	for i, record := range records {
		if i > 0 && size+len(record.query)+1 > 8<<20 {
			c.sendReplicationBatch(node, records[:i])
			c.sendReplicationBatch(node, records[i:])
			return
		}
		size += len(record.query) + 1
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
		if !c.sendReplicationLineWithRetry(node, line) {
			return
		}
		for _, record := range records {
			if record.ack != nil {
				record.ack <- node.ID
			}
		}
		if c.outbox != nil {
			c.outbox.deleteBatch(keys)
		}
		return
	}

	line = "REPL_BATCH " + strings.Join(queries, replicationBatchSeparator)
	if !c.sendReplicationLineWithRetry(node, line) {
		return
	}
	for _, record := range records {
		if record.ack != nil {
			record.ack <- node.ID
		}
	}
	if c.outbox != nil {
		c.outbox.deleteBatch(keys)
	}
}

func (c *Cluster) sendReplicationLineWithRetry(node Node, line string) bool {
	delay := replicationRetryInitialDelay
	attempt := 0
	for {
		if data, err := c.pool.Send(node, line); err == nil {
			if err := replicationResponseError(data); err == nil {
				return true
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

		select {
		case <-c.done:
			return false
		case <-time.After(delay):
		}
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
	if err := c.waitRecovery(); err != nil {
		return err
	}
	replicas := c.GetReplicaNodes(metric)
	needed := c.GetReplicationFactor() / 2
	if len(replicas) < needed {
		return fmt.Errorf("not enough available nodes for configured quorum")
	}
	if needed == 0 {
		return c.replicateAsyncBatch([]ReplicationItem{{Metric: metric, Query: query}})
	}
	ack := make(chan string, len(replicas))
	records := make([]replicationRecord, len(replicas))
	for i, node := range replicas {
		records[i] = replicationRecord{node: node, query: query, ack: ack}
	}
	if c.outbox != nil {
		keys, err := c.outbox.putBatch(records)
		if err != nil {
			return err
		}
		for i := range records {
			records[i].outboxKey = keys[i]
		}
	}
	// Quorum uses the same ordered delivery path as async replication, so a SET
	// cannot overtake an earlier increment just because it requests a quorum.
	if err := c.enqueueFanout(metric, records); err != nil {
		return err
	}
	timer := time.NewTimer(poolRequestTimeout)
	defer timer.Stop()
	for succeeded := 0; succeeded < needed; {
		select {
		case <-ack:
			succeeded++
		case <-timer.C:
			return fmt.Errorf("quorum timeout (outcome unknown; delivery remains pending)")
		case <-c.done:
			return ErrServerBusy
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

func (c *Cluster) waitRecovery() error {
	if c.recoveryDone == nil {
		return nil
	}
	select {
	case <-c.recoveryDone:
		return nil
	case <-c.done:
		return ErrServerBusy
	}
}
