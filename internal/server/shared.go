package server

import (
	"context"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/byPixelTV/flamedb/internal/auth"
	"github.com/byPixelTV/flamedb/internal/cluster"
	"github.com/byPixelTV/flamedb/internal/config"
	"github.com/byPixelTV/flamedb/internal/query"
)

// NewShared keeps the legacy TCP framing and authorization while delegating all
// storage operations to a shared database. API nodes never replicate events.
func NewShared(engine query.Engine, a *auth.Auth, cfg *config.Config) *Server {
	max := 1024
	if cfg.Performance.MaxInflightRequests > 0 {
		max = cfg.Performance.MaxInflightRequests
	}
	id := cfg.Server.NodeID
	if id == "" {
		id = "api"
	}
	address := cfg.Server.AdvertiseAddr
	if address == "" {
		address = net.JoinHostPort(cfg.Server.Host, fmt.Sprint(cfg.Server.Port))
	}
	c := cluster.New(cluster.Node{ID: id, Addr: address}, 150, "", 1)
	lifecycle, cancel := context.WithCancel(context.Background())
	return &Server{exec: engine, auth: a, cfg: cfg, cluster: c, conns: make(map[net.Conn]struct{}), shared: true, requests: make(chan struct{}, max), lifecycle: lifecycle, stopLifecycle: cancel}
}

func (s *Server) connectionLimit() int {
	if s.cfg != nil && s.cfg.Server.MaxConnections > 0 {
		return s.cfg.Server.MaxConnections
	}
	return 1024
}
func (s *Server) requestLimit() int {
	if s.cfg != nil && s.cfg.Server.MaxRequestBytes > 0 {
		return s.cfg.Server.MaxRequestBytes
	}
	return 8 << 20
}
func (s *Server) responseLimit() int {
	if s.cfg != nil && s.cfg.Server.MaxResponseBytes > 0 {
		return s.cfg.Server.MaxResponseBytes
	}
	return 64 << 20
}
func (s *Server) batchLimit() int {
	if s.cfg != nil && s.cfg.Server.MaxBatchEntries > 0 {
		return s.cfg.Server.MaxBatchEntries
	}
	return 10000
}

func (s *Server) startShared() (context.Context, func(), error) {
	s.connMu.Lock()
	if s.closed {
		s.connMu.Unlock()
		return nil, nil, fmt.Errorf("server shutting down")
	}
	select {
	case s.requests <- struct{}{}:
	default:
		s.connMu.Unlock()
		s.busy.Add(1)
		return nil, nil, fmt.Errorf("server busy")
	}
	s.workWG.Add(1)
	s.connMu.Unlock()
	timeout := 30 * time.Second
	if s.cfg != nil && s.cfg.Server.RequestTimeout > 0 {
		timeout = s.cfg.Server.RequestTimeout
	}
	ctx, cancel := context.WithTimeout(s.lifecycle, timeout)
	var once sync.Once
	return ctx, func() { once.Do(func() { cancel(); <-s.requests; s.workWG.Done() }) }, nil
}

func (s *Server) executeShared(q *query.Query) (*query.Result, error) {
	ctx, done, err := s.startShared()
	if err != nil {
		return nil, err
	}
	defer done()
	var result *query.Result
	if engine, ok := s.exec.(query.ContextEngine); ok {
		result, err = engine.ExecuteContext(ctx, q)
	} else {
		result, err = s.exec.Execute(q)
	}
	s.completed.Add(1)
	if err != nil {
		s.failed.Add(1)
	}
	return result, err
}

func (s *Server) handleSharedBatch(conn net.Conn, lines []string, quorum, internal bool) {
	out := query.BatchResult{OK: true}
	queries := make([]*query.Query, 0, len(lines))
	indices := make([]int, 0, len(lines))
	for i, line := range lines {
		q, err := query.Parse(line)
		if err == nil && (q.IsReplica || q.ForceLocal || q.OperationID != "") && !internal {
			err = fmt.Errorf("internal flags require internal permission")
		}
		if err == nil && q.Type != query.QueryTypeWrite {
			err = fmt.Errorf("WRITE_BATCH only supports WRITE items")
		}
		if err != nil {
			out.Failed++
			out.Errors = append(out.Errors, query.BatchItemError{Index: i, Error: err.Error()})
			continue
		}
		q.Quorum = q.Quorum || quorum
		queries = append(queries, q)
		indices = append(indices, i)
	}
	if len(queries) > 0 {
		ctx, done, err := s.startShared()
		var result *query.BatchResult
		if err == nil {
			if engine, ok := s.exec.(query.ContextEngine); ok {
				result, err = engine.ExecuteBatchContext(ctx, queries)
			} else {
				result, err = s.exec.ExecuteBatch(queries)
			}
			done()
			s.completed.Add(1)
			if err != nil {
				s.failed.Add(1)
			}
		}
		if err != nil {
			for _, i := range indices {
				out.Failed++
				out.Errors = append(out.Errors, query.BatchItemError{Index: i, Error: err.Error()})
			}
		} else {
			out.Accepted += result.Accepted
			out.Failed += result.Failed
			for _, item := range result.Errors {
				if item.Index >= 0 && item.Index < len(indices) {
					item.Index = indices[item.Index]
				}
				out.Errors = append(out.Errors, item)
			}
		}
	}
	sort.SliceStable(out.Errors, func(i, j int) bool { return out.Errors[i].Index < out.Errors[j].Index })
	out.OK = out.Failed == 0
	writeJSON(conn, out)
}

type responseBoundConn struct {
	net.Conn
	limit int
}

func (c *responseBoundConn) Write(data []byte) (int, error) {
	if len(data) > c.limit {
		_, err := c.Conn.Write([]byte("{\"error\":\"response limit exceeded\"}\n"))
		if err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("response limit exceeded")
	}
	return c.Conn.Write(data)
}

// MetricsSnapshot exposes counters without metric-name labels or unbounded state.
func (s *Server) MetricsSnapshot() (connections, inflight int, completed, failed, busy uint64) {
	s.connMu.Lock()
	connections = len(s.conns)
	s.connMu.Unlock()
	return connections, len(s.requests), s.completed.Load(), s.failed.Load(), s.busy.Load()
}

func (s *Server) Draining() bool { s.connMu.Lock(); defer s.connMu.Unlock(); return s.closed }

func (s *Server) Listening() bool {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return !s.closed && s.listener != nil
}
