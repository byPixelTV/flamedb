package cluster

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"
)

type RebalanceRequest struct {
	Metric   string `json:"metric"`
	FromNode string `json:"from_node"`
}

type RebalanceStore interface {
	HasMetric(metric string) bool
	ExportMetricData(metric string) (RebalanceData, error)
	ImportRebalanceData(data RebalanceData) error
}

type RebalanceData struct {
	Metric      string             `json:"metric"`
	Events      []RawEvent         `json:"events"`
	Leaderboard []LeaderboardEntry `json:"leaderboard"`
}

type RawEvent struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

type LeaderboardEntry struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

type rebalanceConn struct {
	conn    net.Conn
	scanner *bufio.Scanner
	writer  *bufio.Writer
}

func newRebalanceConn(addr, apiKey string) (*rebalanceConn, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}

	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	// 64 MB buffer for large export responses.
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 64*1024), 64*1024*1024)
	writer := bufio.NewWriter(conn)

	rc := &rebalanceConn{conn: conn, scanner: scanner, writer: writer}

	// auth handshake
	if !scanner.Scan() {
		conn.Close()
		return nil, fmt.Errorf("no auth challenge received")
	}

	fmt.Fprintf(writer, "AUTH %s\n", apiKey)
	writer.Flush()

	if !scanner.Scan() {
		conn.Close()
		return nil, fmt.Errorf("no auth response received")
	}

	return rc, nil
}

func (rc *rebalanceConn) send(msg string) ([]byte, error) {
	_ = rc.conn.SetDeadline(time.Now().Add(30 * time.Second))
	fmt.Fprintf(rc.writer, "%s\n", msg)
	rc.writer.Flush()
	if !rc.scanner.Scan() {
		return nil, fmt.Errorf("connection closed during rebalance")
	}
	result := make([]byte, len(rc.scanner.Bytes()))
	copy(result, rc.scanner.Bytes())
	return result, nil
}

func (rc *rebalanceConn) close() {
	rc.conn.Close()
}

func (c *Cluster) TriggerRebalance(store RebalanceStore, apiKey string) {
	select {
	case <-c.done:
		return
	case <-time.After(2 * time.Second):
	}

	c.Ring.mu.RLock()
	nodes := make(map[string]Node)
	for _, n := range c.Ring.nodes {
		nodes[n.ID] = n
	}
	c.Ring.mu.RUnlock()

	if len(nodes) <= 1 {
		return
	}

	log.Printf("starting rebalance across %d nodes...", len(nodes)-1)

	for _, node := range nodes {
		if node.ID == c.Self.ID {
			continue
		}
		c.startWorker(func() { c.rebalanceFromNode(node, store, apiKey) })
	}
}

func (c *Cluster) rebalanceFromNode(node Node, store RebalanceStore, apiKey string) {
	// Use a dedicated fresh connection to avoid races with pooled connections.
	rc, err := newRebalanceConn(node.Addr, apiKey)
	if err != nil {
		log.Printf("rebalance: could not connect to %s: %v", node.ID, err)
		return
	}
	defer rc.close()

	// metrics liste holen
	metricsPayload, _ := json.Marshal(map[string]string{"type": "CLUSTER_METRICS"})
	result, err := rc.send("CLUSTER " + string(metricsPayload))
	if err != nil {
		log.Printf("rebalance: could not get metrics from %s: %v", node.ID, err)
		return
	}

	var metrics []string
	if err := json.Unmarshal(result, &metrics); err != nil {
		log.Printf("rebalance: invalid metrics response from %s: %s", node.ID, string(result))
		return
	}

	log.Printf("rebalance: %s has %d metrics", node.ID, len(metrics))

	for _, metric := range metrics {
		if !c.IsLocal(metric) {
			continue
		}
		if store.HasMetric(metric) {
			continue
		}

		// Prevent multiple goroutines from importing the same metric.
		if _, loaded := c.rebalancing.LoadOrStore(metric, true); loaded {
			continue
		}

		log.Printf("rebalance: requesting metric %s from %s", metric, node.ID)

		exportPayload, _ := json.Marshal(map[string]string{
			"type":   "CLUSTER_EXPORT",
			"metric": metric,
		})
		result, err = rc.send("CLUSTER " + string(exportPayload))
		if err != nil {
			log.Printf("rebalance: export request failed for %s: %v", metric, err)
			c.rebalancing.Delete(metric) // Release the lock on error.
			continue
		}

		var data RebalanceData
		if err := json.Unmarshal(result, &data); err != nil {
			log.Printf("rebalance: could not parse export data for %s: %v", metric, err)
			c.rebalancing.Delete(metric)
			continue
		}

		if err := store.ImportRebalanceData(data); err != nil {
			log.Printf("rebalance: import failed for %s: %v", metric, err)
			c.rebalancing.Delete(metric)
			continue
		}

		log.Printf("rebalance: imported metric %s (%d events, %d lb entries)",
			metric, len(data.Events), len(data.Leaderboard))
		c.rebalancing.Delete(metric) // Release after a successful import.
		// HasMetric now returns true.
	}
}
