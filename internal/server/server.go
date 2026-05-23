package server

import (
	"bufio"
	"encoding/json"
	"log"
	"net"
	"strings"

	"github.com/byPixelTV/flamedb/internal/aggregates"
	"github.com/byPixelTV/flamedb/internal/auth"
	"github.com/byPixelTV/flamedb/internal/cluster"
	"github.com/byPixelTV/flamedb/internal/query"
	"github.com/byPixelTV/flamedb/internal/storage"
)

type Server struct {
	exec    *query.Executor
	auth    *auth.Auth
	cluster *cluster.Cluster
	apiKey  string // interner key für node-to-node communication
}

func New(store *storage.Storage, lb *aggregates.Leaderboard, a *auth.Auth, c *cluster.Cluster, internalKey string) *Server {
	return &Server{
		exec:    query.NewExecutor(store, lb),
		auth:    a,
		cluster: c,
		apiKey:  internalKey,
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

	// main loop
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		if strings.HasPrefix(strings.ToUpper(line), "CLUSTER ") {
			payload := strings.TrimSpace(line[8:])
			var msg cluster.DiscoveryMessage
			log.Printf("received cluster message: %s", payload)
			if err := json.Unmarshal([]byte(payload), &msg); err != nil {
				writeJSON(conn, map[string]string{"error": "invalid cluster message"})
				continue
			}
			switch msg.Type {
			case "JOIN":
				s.cluster.AddNode(cluster.Node{ID: msg.NodeID, Addr: msg.Addr})
				log.Printf("node joined: %s (%s)", msg.NodeID, msg.Addr)
				writeJSON(conn, map[string]string{"cluster": "ok"})
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
		case query.QueryTypeGet, query.QueryTypeLeaderboard:
			if !session.Can(auth.PermRead) {
				writeJSON(conn, map[string]string{"error": "permission denied: read required"})
				continue
			}
		}

		if !s.cluster.IsLocal(q.Metric) {
			result, err := s.cluster.Forward(q.Metric, s.apiKey, line)
			if err != nil {
				writeJSON(conn, map[string]string{"error": err.Error()})
				continue
			}
			conn.Write(append(result, '\n'))
			continue
		}

		// lokal handlen wie vorher
		result, err := s.exec.Execute(q)
		if err != nil {
			writeJSON(conn, map[string]string{"error": err.Error()})
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

func writeJSON(conn net.Conn, v any) {
	data, _ := json.Marshal(v)
	conn.Write(append(data, '\n'))
}
