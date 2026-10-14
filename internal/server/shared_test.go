package server

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/byPixelTV/flamedb/internal/auth"
	"github.com/byPixelTV/flamedb/internal/config"
	"github.com/byPixelTV/flamedb/internal/query"
)

type blockingEngine struct {
	entered chan struct{}
	release chan struct{}
}

func TestListenerReadinessLifecycle(t *testing.T) {
	s := NewShared(&blockingEngine{}, auth.New(config.AuthConfig{}), &config.Config{})
	defer s.Close()
	if s.Listening() {
		t.Fatal("ready before TCP listener exists")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve(ln) }()
	deadline := time.Now().Add(time.Second)
	for !s.Listening() {
		if time.Now().After(deadline) {
			t.Fatal("listener did not become ready")
		}
		time.Sleep(time.Millisecond)
	}
	ln.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Serve did not exit")
	}
	if s.Listening() {
		t.Fatal("ready after TCP listener failed")
	}
}

func (e *blockingEngine) Execute(*query.Query) (*query.Result, error) { panic("context required") }
func (e *blockingEngine) ExecuteBatch([]*query.Query) (*query.BatchResult, error) {
	panic("context required")
}
func (e *blockingEngine) ListMetrics(map[string]string) ([]string, error) { return []string{}, nil }
func (e *blockingEngine) GetAllMetrics() []string                         { return []string{} }
func (e *blockingEngine) ExecuteContext(ctx context.Context, q *query.Query) (*query.Result, error) {
	select {
	case e.entered <- struct{}{}:
	default:
	}
	select {
	case <-e.release:
		return &query.Result{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (e *blockingEngine) ExecuteBatchContext(ctx context.Context, q []*query.Query) (*query.BatchResult, error) {
	if _, err := e.ExecuteContext(ctx, q[0]); err != nil {
		return nil, err
	}
	return &query.BatchResult{OK: true, Accepted: len(q)}, nil
}
func sharedFixture(t *testing.T, cfg *config.Config, e query.Engine, key string) (*Server, net.Conn, *bufio.Scanner) {
	t.Helper()
	cfg.Auth.Keys = []config.APIKey{{Name: "reader", Key: "read", Permissions: []string{"read"}}, {Name: "writer", Key: "all", Permissions: []string{"read", "write"}}}
	srv := NewShared(e, auth.New(cfg.Auth), cfg)
	client, peer := net.Pipe()
	done := make(chan struct{})
	go func() { srv.handleConn(peer); close(done) }()
	t.Cleanup(func() { client.Close(); <-done; srv.Close() })
	client.SetDeadline(time.Now().Add(5 * time.Second))
	sc := bufio.NewScanner(client)
	if !sc.Scan() {
		t.Fatal("greeting")
	}
	go fmt.Fprintln(client, "AUTH "+key)
	if !sc.Scan() {
		t.Fatal("auth")
	}
	return srv, client, sc
}
func TestSharedBackpressureAndCancellation(t *testing.T) {
	e := &blockingEngine{entered: make(chan struct{}, 2), release: make(chan struct{})}
	cfg := &config.Config{Performance: config.PerformanceConfig{MaxInflightRequests: 1}, Server: config.ServerConfig{RequestTimeout: 50 * time.Millisecond}}
	s, c, sc := sharedFixture(t, cfg, e, "all")
	go fmt.Fprintln(c, "WRITE metric 1")
	select {
	case <-e.entered:
	case <-time.After(time.Second):
		t.Fatal("write didn't start")
	}
	if _, err := s.executeShared(&query.Query{Type: query.QueryTypeWrite}); err == nil || err.Error() != "server busy" {
		t.Fatalf("overload: %v", err)
	}
	if !sc.Scan() || !strings.Contains(sc.Text(), "deadline exceeded") {
		t.Fatalf("deadline response: %s %v", sc.Text(), sc.Err())
	}
	connections, inflight, completed, failed, busy := s.MetricsSnapshot()
	_ = connections
	if inflight != 0 || completed != 1 || failed != 1 || busy != 1 {
		t.Fatalf("metrics: %d %d %d %d", inflight, completed, failed, busy)
	}
}
func TestSharedRankingNeedsReadPermissionAndInternalCommandsReject(t *testing.T) {
	e := &blockingEngine{entered: make(chan struct{}, 1), release: make(chan struct{})}
	close(e.release)
	_, c, sc := sharedFixture(t, &config.Config{}, e, "read")
	for _, input := range []string{"RANK metric alice", "SCORE metric alice", "AROUND metric alice 2", "LEADERBOARD_COUNT metric"} {
		if got := frame(t, c, sc, input); got["error"] != nil {
			t.Fatal(input, got)
		}
	}
	if got := frame(t, c, sc, "WRITE metric 1"); got["error"] != "permission denied: write required" {
		t.Fatal(got)
	}
	if got := frame(t, c, sc, `CLUSTER {"type":"CLUSTER_EXPORT","metric":"metric"}`); got["error"] != "internal permission required" {
		t.Fatal(got)
	}
}
func TestSharedResponseAndBatchLimits(t *testing.T) {
	e := &blockingEngine{entered: make(chan struct{}, 1), release: make(chan struct{})}
	close(e.release)
	_, c, sc := sharedFixture(t, &config.Config{Server: config.ServerConfig{MaxBatchEntries: 1}}, e, "all")
	got := frame(t, c, sc, "WRITE_BATCH\nWRITE x 1\nWRITE x 2\nEND")
	if got["error"] != "batch limit exceeded" {
		t.Fatal(got)
	}
	select {
	case <-e.entered:
		t.Fatal("oversized batch executed")
	default:
	}
	a, b := net.Pipe()
	t.Cleanup(func() { a.Close(); b.Close() })
	limited := &responseBoundConn{Conn: b, limit: 10}
	go writeJSON(limited, map[string]string{"large": strings.Repeat("x", 100)})
	reader := bufio.NewScanner(a)
	if !reader.Scan() || !strings.Contains(reader.Text(), "response limit exceeded") {
		t.Fatal("response bound failed")
	}
}
func TestSharedShutdownDrainsAdmittedWrites(t *testing.T) {
	e := &blockingEngine{entered: make(chan struct{}, 1), release: make(chan struct{})}
	s := NewShared(e, auth.New(config.AuthConfig{}), &config.Config{Server: config.ServerConfig{ShutdownTimeout: time.Second}})
	result := make(chan error, 1)
	go func() { _, err := s.executeShared(&query.Query{Type: query.QueryTypeWrite}); result <- err }()
	<-e.entered
	closed := make(chan struct{})
	go func() { s.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("shutdown abandoned admitted write")
	case <-time.After(30 * time.Millisecond):
	}
	close(e.release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("shutdown didn't finish")
	}
	if _, err := s.executeShared(&query.Query{}); err == nil {
		t.Fatal("accepted after shutdown")
	}
}

func TestSharedShutdownTimeoutCancelsDatabaseWork(t *testing.T) {
	e := &blockingEngine{entered: make(chan struct{}, 1), release: make(chan struct{})}
	s := NewShared(e, auth.New(config.AuthConfig{}), &config.Config{Server: config.ServerConfig{RequestTimeout: time.Minute, ShutdownTimeout: 20 * time.Millisecond}})
	result := make(chan error, 1)
	go func() { _, err := s.executeShared(&query.Query{Type: query.QueryTypeGet}); result <- err }()
	<-e.entered
	start := time.Now()
	s.Close()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("uncancelled work")
		}
	case <-time.After(time.Second):
		t.Fatal("database context not cancelled")
	}
	if time.Since(start) > time.Second || !s.Draining() {
		t.Fatal("shutdown bounds/readiness failed")
	}
}
