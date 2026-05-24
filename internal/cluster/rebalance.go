package cluster

import (
	"encoding/json"
	"log"
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

// TriggerRebalance wird aufgerufen wenn dieser node gejoint hat
// checkt welche metrics jetzt zu ihm gehören und requested data
func (c *Cluster) TriggerRebalance(store RebalanceStore, apiKey string) {
	// kurz warten bis gossip propagiert ist
	time.Sleep(2 * time.Second)

	c.Ring.mu.RLock()
	nodes := make(map[string]Node)
	for _, n := range c.Ring.nodes {
		nodes[n.ID] = n
	}
	c.Ring.mu.RUnlock()

	if len(nodes) <= 1 {
		return // single node, nichts zu rebalancen
	}

	log.Printf("starting rebalance, checking which metrics belong to us...")

	// alle anderen nodes fragen welche metrics sie haben
	// und von denen die zu uns gehören die data holen
	for _, node := range nodes {
		if node.ID == c.Self.ID {
			continue
		}
		go c.rebalanceFromNode(node, store, apiKey)
	}
}

func (c *Cluster) rebalanceFromNode(node Node, store RebalanceStore, apiKey string) {
	// metrics liste von dem node holen (JSON)
	payload, _ := json.Marshal(DiscoveryMessage{Type: "CLUSTER_METRICS"})
	result, err := c.pool.Send(node, "CLUSTER "+string(payload))
	if err != nil {
		log.Printf("rebalance: could not get metrics from %s: %v", node.ID, err)
		return
	}

	var metrics []string
	if err := json.Unmarshal(result, &metrics); err != nil {
		log.Printf("rebalance: invalid metrics response from %s: %s", node.ID, string(result))
		return
	}

	for _, metric := range metrics {
		// gehoert diese metric zu uns (primary oder replica)?
		if !c.IsLocal(metric) {
			continue
		}

		log.Printf("rebalance: requesting metric %s from %s", metric, node.ID)

		// data requesten (JSON)
		payload, _ = json.Marshal(DiscoveryMessage{Type: "CLUSTER_EXPORT", Metric: metric})
		result, err = c.pool.Send(node, "CLUSTER "+string(payload))
		if err != nil {
			log.Printf("rebalance: export request failed: %v", err)
			continue
		}

		var data RebalanceData
		if err := json.Unmarshal(result, &data); err != nil {
			log.Printf("rebalance: could not parse export data: %v", err)
			continue
		}

		// importieren
		if err := store.ImportRebalanceData(data); err != nil {
			log.Printf("rebalance: import failed: %v", err)
			continue
		}

		log.Printf("rebalance: imported metric %s (%d events, %d lb entries)",
			metric, len(data.Events), len(data.Leaderboard))
	}
}
