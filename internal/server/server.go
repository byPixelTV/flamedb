package server

import (
	"bufio"
	"encoding/json"
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

		if strings.HasPrefix(strings.ToUpper(line), "CLUSTER ") {
			payload := strings.TrimSpace(line[8:])
			var msg cluster.DiscoveryMessage
			if err := json.Unmarshal([]byte(payload), &msg); err != nil {
				writeJSON(conn, map[string]string{"error": "invalid cluster message"})
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

		if (q.Type == query.QueryTypeWrite || q.Type == query.QueryTypeSet || q.Type == query.QueryTypeDelete) &&
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
		if !s.cluster.IsLocal(q.Metric) {
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
			if readNode.ID != s.cluster.Self.ID {
				result, err := s.cluster.SendToNode(readNode, line)
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
			if s.cluster.IsPrimaryFor(q.Metric) && !q.IsReplica {
				replicaQuery := line + " __replica"
				if err := s.cluster.ReplicateWrite(q.Metric, replicaQuery, q.Quorum); err != nil {
					writeJSON(conn, map[string]string{"error": fmt.Sprintf("quorum failed: %v", err)})
					continue
				}
			}
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

func writeJSON(conn net.Conn, v any) {
	data, _ := json.Marshal(v)
	conn.Write(append(data, '\n'))
}
