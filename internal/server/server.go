package server

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"

	"github.com/byPixelTV/flamedb/internal/aggregates"
	"github.com/byPixelTV/flamedb/internal/auth"
	"github.com/byPixelTV/flamedb/internal/cluster"
	"github.com/byPixelTV/flamedb/internal/config"
	"github.com/byPixelTV/flamedb/internal/query"
	"github.com/byPixelTV/flamedb/internal/storage"
)

type Server struct {
	exec    *query.Executor
	auth    *auth.Auth
	cluster *cluster.Cluster
	apiKey  string
	store   *storage.Storage // neu
	cfgPath string
	cfgMu   sync.Mutex
	cfg     *config.Config
}

func New(store *storage.Storage, lb *aggregates.Leaderboard, a *auth.Auth, c *cluster.Cluster, internalKey string, cfg *config.Config, cfgPath string) *Server {
	return &Server{
		exec:    query.NewExecutor(store, lb),
		auth:    a,
		cluster: c,
		apiKey:  internalKey,
		store:   store,
		cfg:     cfg,
		cfgPath: cfgPath,
	}
}

func (s *Server) persistConfig(msg cluster.DiscoveryMessage) {
	if s.cfg == nil || s.cfgPath == "" {
		return
	}

	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()

	if msg.ReplicationFactor > 0 {
		s.cfg.Cluster.ReplicationFactor = msg.ReplicationFactor
	}
	if msg.ReadPolicy != "" {
		s.cfg.Cluster.ReadPolicy = msg.ReadPolicy
	}
	if msg.UpdateSeeds {
		peers := s.cluster.GetAllNodes()
		seeds := make([]string, 0, len(peers))
		for _, p := range peers {
			if p.ID != s.cluster.Self.ID {
				seeds = append(seeds, p.Addr)
			}
		}
		s.cfg.Cluster.Seeds = seeds
	}

	if err := config.Save(s.cfgPath, s.cfg); err != nil {
		log.Printf("persist config failed: %v", err)
	}
}

func (s *Server) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	log.Printf("FlameDB listening on %s", addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 0, 64*1024), 64<<20)

	// auth handshake
	writeJSON(conn, map[string]string{"auth": "required"})

	var session *auth.Session
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(strings.ToUpper(line), "AUTH ") {
			writeJSON(conn, map[string]string{"error": "authenticate first"})
			continue
		}

		key := strings.TrimSpace(line[5:])
		s, ok := s.auth.Validate(key)
		if !ok {
			writeJSON(conn, map[string]string{"error": "invalid key"})
			conn.Close()
			return
		}

		session = s
		writeJSON(conn, map[string]string{"auth": "ok", "name": session.Name})
		break
	}

	if session == nil {
		return
	}

	// main loop
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		if strings.HasPrefix(line, "REPL_BATCH ") {
			if !session.Can(auth.PermWrite) {
				writeJSON(conn, map[string]string{"error": "permission denied"})
				continue
			}
			queries := strings.Split(strings.TrimSpace(line[len("REPL_BATCH "):]), "\x1f")
			var batch []*query.Query
			for _, replicaLine := range queries {
				replicaLine = strings.TrimSpace(replicaLine)
				if replicaLine == "" {
					continue
				}
				q, err := query.Parse(replicaLine)
				if err != nil {
					writeJSON(conn, map[string]string{"error": err.Error()})
					continue
				}
				q.IsReplica = true
				q.ForceLocal = true
				batch = append(batch, q)
			}
			res, err := s.exec.ExecuteBatch(batch)
			if err != nil {
				writeJSON(conn, map[string]string{"error": err.Error()})
				continue
			}
			if res.Failed > 0 {
				writeJSON(conn, res)
				continue
			}
			writeJSON(conn, map[string]string{"cluster": "ok"})
			continue
		}

		if strings.HasPrefix(strings.ToUpper(line), "WRITE_BATCH") {
			if !session.Can(auth.PermWrite) {
				writeJSON(conn, map[string]string{"error": "permission denied: write required"})
				continue
			}
			s.handleWriteBatch(conn, scanner, line)
			continue
		}

		if strings.HasPrefix(strings.ToUpper(line), "CLUSTER ") {
			payload := strings.TrimSpace(line[8:])
			var msg cluster.DiscoveryMessage
			if err := json.Unmarshal([]byte(payload), &msg); err != nil {
				writeJSON(conn, map[string]string{"error": "invalid cluster message"})
				continue
			}
			if msg.Type == "CLUSTER_TOPOLOGY" {
				if !session.Can(auth.PermRead) && !session.Can(auth.PermWrite) {
					writeJSON(conn, map[string]string{"error": "permission denied"})
					continue
				}
				writeJSON(conn, s.cluster.Topology())
				continue
			}
			if !session.Can(auth.PermWrite) {
				writeJSON(conn, map[string]string{"error": "permission denied"})
				continue
			}
			switch msg.Type {
			case "CLUSTER_METRICS":
				// alle metrics die dieser node hat zurückschicken
				metrics := s.exec.GetAllMetrics()
				data, _ := json.Marshal(metrics)
				conn.Write(append(data, '\n'))
				continue
			case "SET_CONFIG":
				if msg.ReplicationFactor > 0 {
					s.cluster.SetReplicationFactor(msg.ReplicationFactor)
					go s.cluster.TriggerRebalance(s.store, s.apiKey)
				}
				if msg.ReadPolicy != "" {
					s.cluster.SetReadPolicy(cluster.ReadPolicy(msg.ReadPolicy))
				}

				// persist lokale config
				s.persistConfig(msg)

				// propagate an alle nodes (nur wenn Admin das will)
				if msg.Propagate {
					s.cluster.BroadcastConfig(msg)
				}

				writeJSON(conn, map[string]string{"cluster": "ok"})
				continue
			case "CLUSTER_EXPORT":
				metricName := strings.TrimSpace(msg.Metric)
				if metricName == "" {
					writeJSON(conn, map[string]string{"error": "missing metric"})
					continue
				}
				data, err := s.store.ExportMetricData(metricName)
				if err != nil {
					writeJSON(conn, map[string]string{"error": err.Error()})
					continue
				}
				encoded, _ := json.Marshal(data)
				conn.Write(append(encoded, '\n'))
				continue
			case "JOIN":
				newNode := cluster.Node{ID: msg.NodeID, Addr: msg.Addr}
				isNew := !s.cluster.Knows(newNode.ID)
				s.cluster.AddNode(newNode)
				log.Printf("node joined: %s (%s)", msg.NodeID, msg.Addr)
				if isNew {
					go s.cluster.PropagateJoin(newNode, s.apiKey)
				}
				// alle bekannten nodes zurückschicken
				peers := s.cluster.GetAllNodes()
				writeJSON(conn, map[string]interface{}{
					"cluster": "ok",
					"peers":   peers,
				})
			}
			continue
		}

		q, err := query.Parse(line)
		if err != nil {
			writeJSON(conn, map[string]string{"error": err.Error()})
			continue
		}

		// permission check
		switch q.Type {
		case query.QueryTypeWrite, query.QueryTypeSet, query.QueryTypeDelete:
			if !session.Can(auth.PermWrite) {
				writeJSON(conn, map[string]string{"error": "permission denied: write required"})
				continue
			}
		case query.QueryTypeGet, query.QueryTypeLeaderboard, query.QueryTypeStats, query.QueryTypeGroupLeaderboard:
			if !session.Can(auth.PermRead) {
				writeJSON(conn, map[string]string{"error": "permission denied: read required"})
				continue
			}
		}

		// multi-metric GET: scatter/gather across nodes
		if q.Type == query.QueryTypeGet && len(q.Metrics) > 1 {
			// build suffix after metric list (keeps WHERE/FROM/TO/etc)
			cmdEnd := strings.Index(line, " ")
			if cmdEnd == -1 {
				writeJSON(conn, map[string]string{"error": "invalid query"})
				continue
			}
			metricStart := cmdEnd + 1
			nextSpace := strings.Index(line[metricStart:], " ")
			suffix := ""
			if nextSpace != -1 {
				suffix = line[metricStart+nextSpace:]
			}

			metricsOut := make(map[string][]storage.Event, len(q.Metrics))
			aggregatesOut := make(map[string]*query.AggregateResult, len(q.Metrics))
			seriesOut := make(map[string][]query.SeriesPoint, len(q.Metrics))

			for _, metric := range q.Metrics {
				metricLine := "GET " + metric + suffix
				readNode := s.cluster.GetReadNode(metric)

				var res query.Result
				if readNode.ID == s.cluster.Self.ID {
					q2 := *q
					q2.Metric = metric
					q2.Metrics = nil
					r, err := s.exec.Execute(&q2)
					if err != nil {
						writeJSON(conn, map[string]string{"error": err.Error()})
						continue
					}
					res = *r
				} else {
					bytes, err := s.cluster.SendToNodeLocal(readNode, metricLine)
					if err != nil {
						writeJSON(conn, map[string]string{"error": err.Error()})
						continue
					}
					if err := json.Unmarshal(bytes, &res); err != nil {
						writeJSON(conn, map[string]string{"error": "invalid upstream response"})
						continue
					}
				}

				if res.Aggregate != nil {
					aggregatesOut[metric] = res.Aggregate
				} else if len(res.Series) > 0 {
					seriesOut[metric] = res.Series
				} else {
					if res.Events == nil {
						res.Events = []storage.Event{}
					}
					metricsOut[metric] = res.Events
				}
			}

			var out query.Result
			if len(aggregatesOut) > 0 {
				out.Aggregates = aggregatesOut
			} else if len(seriesOut) > 0 {
				out.SeriesByMetric = seriesOut
			} else {
				out.Metrics = metricsOut
			}

			writeJSON(conn, out)
			continue
		}

		requiresPrimary := q.Type == query.QueryTypeSet ||
			q.Type == query.QueryTypeDelete ||
			(q.Type == query.QueryTypeWrite && q.UpdateLB)

		if requiresPrimary &&
			!q.IsReplica &&
			!s.cluster.IsPrimaryFor(q.Metric) {

			result, err := s.cluster.ForwardToPrimary(q.Metric, s.apiKey, line)
			if err != nil {
				writeJSON(conn, map[string]string{"error": err.Error()})
				continue
			}
			conn.Write(append(result, '\n'))
			continue
		}

		// forwarden wenn nicht lokal
		if !q.ForceLocal && !s.cluster.IsLocal(q.Metric) {
			result, err := s.cluster.ForwardWithFailover(q.Metric, s.apiKey, line)
			if err != nil {
				writeJSON(conn, map[string]string{"error": err.Error()})
				continue
			}
			conn.Write(append(result, '\n'))
			continue
		}

		// für reads: round-robin über replicas
		switch q.Type {
		case query.QueryTypeGet, query.QueryTypeLeaderboard, query.QueryTypeStats:
			readNode := s.cluster.GetReadNode(q.Metric)
			if !q.ForceLocal && readNode.ID != s.cluster.Self.ID {
				result, err := s.cluster.SendToNodeLocal(readNode, line)
				if err != nil {
					// fallback: lokal handlen
					break
				}
				conn.Write(append(result, '\n'))
				continue
			}
		}

		// lokal ausführen (primary oder replica write)
		result, err := s.exec.Execute(q)
		if err != nil {
			writeJSON(conn, map[string]string{"error": err.Error()})
			continue
		}

		// replication nur vom primary, nicht von replicas
		switch q.Type {
		case query.QueryTypeWrite, query.QueryTypeSet, query.QueryTypeDelete:
			replicateFromThisNode := !q.IsReplica && (s.cluster.IsPrimaryFor(q.Metric) || (q.Type == query.QueryTypeWrite && !q.UpdateLB))
			if replicateFromThisNode {
				replicaQuery := line + " __replica"
				if err := s.cluster.ReplicateWrite(q.Metric, replicaQuery, q.Quorum); err != nil {
					writeJSON(conn, map[string]string{"error": fmt.Sprintf("quorum failed: %v", err)})
					continue
				}
			}
			writeEmptyResult(conn)
			continue
		}

		writeJSON(conn, result)
	}

	if err := scanner.Err(); err != nil {
		// nur loggen wenn nicht einfach connection closed
		if !isConnectionClosed(err) {
			log.Printf("scanner error: %v", err)
		}
	}
}

func isConnectionClosed(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "wsarecv") ||
		strings.Contains(s, "use of closed") ||
		strings.Contains(s, "connection reset")
}

func (s *Server) handleWriteBatch(conn net.Conn, scanner *bufio.Scanner, header string) {
	batchQuorum := strings.Contains(strings.ToUpper(header), "QUORUM")
	var lines []string
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.EqualFold(line, "END") {
			break
		}
		if line != "" {
			lines = append(lines, line)
		}
	}

	if len(lines) == 0 {
		writeJSON(conn, query.BatchResult{OK: true})
		return
	}

	result := &query.BatchResult{OK: true}
	type localItem struct {
		index int
		line  string
		q     *query.Query
	}
	var local []localItem

	for i, itemLine := range lines {
		q, err := query.Parse(itemLine)
		if err != nil {
			result.Failed++
			result.Errors = append(result.Errors, query.BatchItemError{Index: i, Error: err.Error()})
			continue
		}
		if q.Type != query.QueryTypeWrite {
			result.Failed++
			result.Errors = append(result.Errors, query.BatchItemError{Index: i, Error: "WRITE_BATCH only supports WRITE items"})
			continue
		}
		if batchQuorum {
			q.Quorum = true
		}

		requiresPrimary := q.UpdateLB
		if requiresPrimary && !q.IsReplica && !s.cluster.IsPrimaryFor(q.Metric) {
			data, err := s.cluster.ForwardToPrimary(q.Metric, s.apiKey, itemLine)
			if err != nil {
				result.Failed++
				result.Errors = append(result.Errors, query.BatchItemError{Index: i, Error: err.Error()})
				continue
			}
			if err := upstreamError(data); err != nil {
				result.Failed++
				result.Errors = append(result.Errors, query.BatchItemError{Index: i, Error: err.Error()})
				continue
			}
			result.Accepted++
			continue
		}

		if !q.ForceLocal && !s.cluster.IsLocal(q.Metric) {
			data, err := s.cluster.ForwardWithFailover(q.Metric, s.apiKey, itemLine)
			if err != nil {
				result.Failed++
				result.Errors = append(result.Errors, query.BatchItemError{Index: i, Error: err.Error()})
				continue
			}
			if err := upstreamError(data); err != nil {
				result.Failed++
				result.Errors = append(result.Errors, query.BatchItemError{Index: i, Error: err.Error()})
				continue
			}
			result.Accepted++
			continue
		}

		local = append(local, localItem{index: i, line: itemLine, q: q})
	}

	if len(local) > 0 {
		localQueries := make([]*query.Query, 0, len(local))
		localIndexByBatchIndex := make(map[int]int, len(local))
		for i, item := range local {
			localQueries = append(localQueries, item.q)
			localIndexByBatchIndex[i] = item.index
		}

		localResult, err := s.exec.ExecuteBatch(localQueries)
		if err != nil {
			for _, item := range local {
				result.Failed++
				result.Errors = append(result.Errors, query.BatchItemError{Index: item.index, Error: err.Error()})
			}
		} else {
			failedLocal := make(map[int]bool, len(localResult.Errors))
			for _, itemErr := range localResult.Errors {
				origIndex := localIndexByBatchIndex[itemErr.Index]
				failedLocal[origIndex] = true
				result.Errors = append(result.Errors, query.BatchItemError{Index: origIndex, Error: itemErr.Error})
			}
			result.Failed += localResult.Failed
			result.Accepted += localResult.Accepted

			for _, item := range local {
				if failedLocal[item.index] || item.q.IsReplica {
					continue
				}
				replicateFromThisNode := s.cluster.IsPrimaryFor(item.q.Metric) || !item.q.UpdateLB
				if !replicateFromThisNode {
					continue
				}
				replicaQuery := item.line + " __replica"
				if err := s.cluster.ReplicateWrite(item.q.Metric, replicaQuery, item.q.Quorum); err != nil {
					result.Accepted--
					result.Failed++
					result.Errors = append(result.Errors, query.BatchItemError{Index: item.index, Error: fmt.Sprintf("quorum failed: %v", err)})
				}
			}
		}
	}

	if result.Failed > 0 {
		result.OK = false
	}
	writeJSON(conn, result)
}

func upstreamError(data []byte) error {
	var resp struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return fmt.Errorf("invalid upstream response")
	}
	if resp.Error != "" {
		return errors.New(resp.Error)
	}
	return nil
}

func writeJSON(conn net.Conn, v any) {
	data, _ := json.Marshal(v)
	conn.Write(append(data, '\n'))
}

func writeEmptyResult(conn net.Conn) {
	_, _ = conn.Write([]byte("{}\n"))
}
