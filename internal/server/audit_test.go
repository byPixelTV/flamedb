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
	for _, metric := range []string{"kills", "smp:", "smp:kills"} {
		t.Run(metric, func(t *testing.T) { testTwoNodeQuorum(t, metric) })
	}
}

func testTwoNodeQuorum(t *testing.T, metric string) {
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
	primary, _ := nodes[0].c.GetPrimaryNode(metric)
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
	v := frame(t, conn, sc, fmt.Sprintf("WRITE_BATCH QUORUM\nWRITE %s 3 lb=\"p\"\nEND", metric))
	if v["accepted"] != float64(1) || v["failed"] != float64(0) {
		t.Fatal(v)
	}
	// Each replica reports the same key, but discovery must return it once.
	if result := frame(t, conn, sc, "METRICS"); fmt.Sprint(result["metric_keys"]) != "["+metric+"]" {
		t.Fatal(result)
	}
	// Add a metric only on the other node to verify cluster-wide gathering.
	for _, n := range nodes {
		if n.c.Self.ID == ingress.c.Self.ID {
			continue
		}
		if err := n.store.WriteEvent(storage.Event{Metric: "remote-only", Timestamp: 123, Value: 1, Tags: map[string]string{"uuid": "remote-player"}}, true); err != nil {
			t.Fatal(err)
		}
	}
	if result := frame(t, conn, sc, `METRICS WHERE uuid="remote-player"`); fmt.Sprint(result["metric_keys"]) != "[remote-only]" {
		t.Fatal(result)
	}
	var stamp int64
	for i, n := range nodes {
		events, err := n.store.ReadRange(metric, 0, 1<<62)
		if err != nil || len(events) != 1 {
			t.Fatalf("%v %v", events, err)
		}
		if i == 0 {
			stamp = events[0].Timestamp
		} else if events[0].Timestamp != stamp {
			t.Fatal("replica timestamp differs")
		}
		score, err := aggregates.New(n.store.DB()).Get(metric, "p")
		if err != nil || score != 3 {
			t.Fatalf("score=%v err=%v", score, err)
		}
	}

	for _, line := range []string{fmt.Sprintf(`SET %s 5 lb="p"`, metric), fmt.Sprintf(`WRITE %s 1 lb="p" QUORUM`, metric)} {
		result := frame(t, conn, sc, line)
		if result["error"] != nil {
			t.Fatal(result)
		}
	}
	for _, n := range nodes {
		score, err := aggregates.New(n.store.DB()).Get(metric, "p")
		if err != nil || score != 6 {
			t.Fatalf("async SET / quorum WRITE reordered: %v %v", score, err)
		}
	}
	// Filtered deletion is forwarded to the primary and applied on both replicas.
	for _, line := range []string{
		fmt.Sprintf(`WRITE %s 2 player="a" lb="a" QUORUM`, metric),
		fmt.Sprintf(`WRITE %s 4 player="b" lb="b" QUORUM`, metric),
		fmt.Sprintf(`PREVIEW_DELETE %s WHERE player="a" lb="a"`, metric),
		fmt.Sprintf(`DELETE %s WHERE player="a" lb="a" QUORUM`, metric),
	} {
		result := frame(t, conn, sc, line)
		if result["error"] != nil {
			t.Fatal(result)
		}
		if strings.HasPrefix(line, "DELETE") || strings.HasPrefix(line, "PREVIEW_DELETE") {
			deletion := result["delete"].(map[string]any)
			if deletion["events"] != float64(1) || deletion["leaderboard_entries"] != float64(1) {
				t.Fatal(result)
			}
			if deletion["dry_run"] != strings.HasPrefix(line, "PREVIEW_DELETE") {
				t.Fatal(result)
			}
		}
	}
	for _, n := range nodes {
		rows, err := n.store.ReadRangeWithTags(metric, 0, 1<<62, map[string]string{"player": "a"})
		if err != nil || len(rows) != 0 {
			t.Fatal(rows, err)
		}
		rows, err = n.store.ReadRangeWithTags(metric, 0, 1<<62, map[string]string{"player": "b"})
		if err != nil || len(rows) != 1 {
			t.Fatal(rows, err)
		}
		score, err := aggregates.New(n.store.DB()).Get(metric, "a")
		if err != nil || score != 0 {
			t.Fatal(score, err)
		}
	}

}

func TestMetricDiscovery(t *testing.T) {
	c, sc := testConnection(t, "user")
	if got := frame(t, c, sc, "METRICS")["metric_keys"].([]any); len(got) != 0 {
		t.Fatal(got)
	}
	for _, line := range []string{`WRITE smp:kills 1 uuid="a" region="eu"`, `WRITE deaths 1 uuid="b"`, `SET balance 4 lb="a"`} {
		if v := frame(t, c, sc, line); v["error"] != nil {
			t.Fatal(v)
		}
	}
	for line, want := range map[string]string{
		"METRICS":                                "[balance deaths smp:kills]",
		`METRICS WHERE uuid="a"`:                 "[smp:kills]",
		`METRICS WHERE uuid="a" AND region="us"`: "[]",
	} {
		if v := frame(t, c, sc, line); fmt.Sprint(v["metric_keys"]) != want {
			t.Fatalf("%s: %v", line, v)
		}
	}
	for _, line := range []string{"METRICS WHERE", `METRICS WHERE uuid="a" AND`, "METRICS LIMIT 1", "METRICS __local"} {
		if v := frame(t, c, sc, line); v["error"] == nil {
			t.Fatal(line, v)
		}
	}
}
