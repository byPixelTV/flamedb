package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/byPixelTV/flamedb/internal/aggregates"
	"github.com/byPixelTV/flamedb/internal/auth"
	"github.com/byPixelTV/flamedb/internal/cluster"
	"github.com/byPixelTV/flamedb/internal/config"
	"github.com/byPixelTV/flamedb/internal/storage"
	"net"
	"strings"
	"testing"
	"time"
)

func testConnection(t *testing.T, key string) (net.Conn, *bufio.Scanner) {
	t.Helper()
	store, err := storage.Open(t.TempDir(), "none")
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.AuthConfig{InternalKey: "unit-test-internal", Keys: []config.APIKey{{Name: "user", Key: "user", Permissions: []string{"read", "write"}}}}
	srv := New(store, aggregates.New(store.DB()), auth.New(cfg), cluster.New(cluster.Node{ID: "self"}, 150, "unit-test-internal", 1), "unit-test-internal", nil, "")
	client, server := net.Pipe()
	done := make(chan struct{})
	go func() { srv.handleConn(server); close(done) }()
	t.Cleanup(func() { client.Close(); <-done; store.Close() })
	client.SetDeadline(time.Now().Add(5 * time.Second))
	sc := bufio.NewScanner(client)
	if !sc.Scan() {
		t.Fatal("missing challenge")
	}
	fmt.Fprintln(client, "AUTH "+key)
	if !sc.Scan() {
		t.Fatal("missing auth")
	}
	return client, sc
}
func frame(t *testing.T, c net.Conn, sc *bufio.Scanner, line string) map[string]any {
	t.Helper()
	go fmt.Fprintln(c, line)
	if !sc.Scan() {
		t.Fatalf("no response: %v", sc.Err())
	}
	var v map[string]any
	if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
		t.Fatal(err)
	}
	return v
}
func TestInternalAuthorization(t *testing.T) {
	c, sc := testConnection(t, "user")
	for _, line := range []string{`CLUSTER {"type":"CLUSTER_EXPORT","metric":"x"}`, `WRITE x 1 __replica`, `REPL_BATCH WRITE x 1`, `WRITE_BATCH` + "\nWRITE x 1 __local\nEND"} {
		v := frame(t, c, sc, line)
		if v["error"] == nil && v["failed"] != float64(1) {
			t.Fatalf("allowed %q: %v", line, v)
		}
	}
}
func TestReplicaBatchOneReplyAndMixedMutations(t *testing.T) {
	c, sc := testConnection(t, "unit-test-internal")
	v := frame(t, c, sc, "REPL_BATCH WRITE x 1\x1fSET x 2 lb=\x1fWRITE x 3")
	if v["error"] == nil {
		t.Fatal(v)
	}
	v = frame(t, c, sc, `GET x COUNT`)
	agg := v["aggregate"].(map[string]any)
	if agg["count"] != float64(0) {
		t.Fatal(v)
	}
	v = frame(t, c, sc, strings.Join([]string{`REPL_BATCH SET x 5 lb="p"`, `DELETE x lb="p"`}, "\x1f"))
	if v["cluster"] != "ok" {
		t.Fatal(v)
	}
}

func TestTwoNodeQuorumAndForwardedBatch(t *testing.T) {
	type instance struct {
		srv   *Server
		c     *cluster.Cluster
		store *storage.Storage
		ln    net.Listener
	}
	var nodes []instance
	cfg := config.AuthConfig{InternalKey: "integration-internal", Keys: []config.APIKey{{Name: "user", Key: "user", Permissions: []string{"read", "write"}}}}
	for _, id := range []string{"a", "b"} {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		store, err := storage.Open(t.TempDir(), "none")
		if err != nil {
			t.Fatal(err)
		}
		c := cluster.New(cluster.Node{ID: id, Addr: ln.Addr().String()}, 150, cfg.InternalKey, 2)
		c.AttachReplicationOutbox(store.DB())
		srv := New(store, aggregates.New(store.DB()), auth.New(cfg), c, cfg.InternalKey, nil, "")
		nodes = append(nodes, instance{srv, c, store, ln})
	}
	for _, n := range nodes {
		for _, other := range nodes {
			n.c.AddNode(other.c.Self)
		}
		go n.srv.Serve(n.ln)
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.c.Close()
		}
		for _, n := range nodes {
			n.srv.Close()
			n.store.Close()
		}
	})
	primary, _ := nodes[0].c.GetPrimaryNode("kills")
	ingress := nodes[0]
	if ingress.c.Self.ID == primary.ID {
		ingress = nodes[1]
	}
	conn, err := net.Dial("tcp", ingress.ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	sc := bufio.NewScanner(conn)
	if !sc.Scan() {
		t.Fatal(sc.Err())
	}
	fmt.Fprintln(conn, "AUTH user")
	if !sc.Scan() {
		t.Fatal(sc.Err())
	}
	v := frame(t, conn, sc, "WRITE_BATCH QUORUM\nWRITE kills 3 lb=\"p\"\nEND")
	if v["accepted"] != float64(1) || v["failed"] != float64(0) {
		t.Fatal(v)
	}
	var stamp int64
	for i, n := range nodes {
		events, err := n.store.ReadRange("kills", 0, 1<<62)
		if err != nil || len(events) != 1 {
			t.Fatalf("%v %v", events, err)
		}
		if i == 0 {
			stamp = events[0].Timestamp
		} else if events[0].Timestamp != stamp {
			t.Fatal("replica timestamp differs")
		}
		score, err := aggregates.New(n.store.DB()).Get("kills", "p")
		if err != nil || score != 3 {
			t.Fatalf("score=%v err=%v", score, err)
		}
	}

	for _, line := range []string{`SET kills 5 lb="p"`, `WRITE kills 1 lb="p" QUORUM`} {
		result := frame(t, conn, sc, line)
		if result["error"] != nil {
			t.Fatal(result)
		}
	}
	for _, n := range nodes {
		score, err := aggregates.New(n.store.DB()).Get("kills", "p")
		if err != nil || score != 6 {
			t.Fatalf("async SET / quorum WRITE reordered: %v %v", score, err)
		}
	}
}
