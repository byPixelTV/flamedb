package compat

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func noError(t *testing.T, got any) {
	t.Helper()
	if m, ok := got.(map[string]any); !ok || m["error"] != nil {
		t.Fatalf("unexpected error response: %v", got)
	}
}
func score(t *testing.T, w *wire, metric, entity string, want int64) {
	t.Helper()
	r := w.send(t, fmt.Sprintf("SCORE %s %q", metric, entity))
	noError(t, r)
	rank, ok := r.(map[string]any)["ranking"].(map[string]any)
	if !ok || rank["score"] != json.Number(fmt.Sprint(want)) {
		t.Fatalf("score %s %s expected %d got %v", metric, entity, want, r)
	}
}

func TestSharedNodesConcurrentDurabilityAndRecovery(t *testing.T) {
	binary := prerequisite(t, "FLAMEDB_NEXTGEN_BINARY")
	redisAddr := prerequisite(t, "FLAMEDB_TEST_REDIS")
	first, kill := startServerControlled(t, binary, true)
	second := startServer(t, binary, true)
	third := startServer(t, binary, true)
	clients := []*wire{connect(t, first, "compat-key"), connect(t, second, "compat-key"), connect(t, third, "compat-key")}
	metric := fmt.Sprintf("reliable%d", time.Now().UnixNano())
	base := time.Now().UnixNano()
	const perNode = 40
	var wg sync.WaitGroup
	for node, client := range clients {
		wg.Add(1)
		go func(node int, w *wire) {
			defer wg.Done()
			for i := 0; i < perNode; i++ {
				noError(t, w.send(t, fmt.Sprintf(`WRITE %s 1 lb="shared" lb.reason="all" player="shared" ts=%d`, metric, base+int64(node*perNode+i))))
			}
		}(node, client)
	}
	wg.Wait()
	for _, w := range clients {
		score(t, w, metric, "shared", 3*perNode)
		r := w.send(t, "GET "+metric+" COUNT")
		noError(t, r)
		if r.(map[string]any)["aggregate"].(map[string]any)["count"] != json.Number("120") {
			t.Fatalf("event count: %v", r)
		}
	}
	// Hard kill immediately after acknowledged mutations. A replacement API reads the same PostgreSQL truth.
	kill()
	replacement := connect(t, startServer(t, binary, true), "compat-key")
	score(t, replacement, metric, "shared", 120)
	// SET is absolute, DELETE of events leaves absolute all-time scores intact.
	noError(t, replacement.send(t, `SET `+metric+` 321 lb="shared"`))
	score(t, clients[1], metric, "shared", 321)
	noError(t, clients[1].send(t, "DELETE "+metric))
	score(t, clients[2], metric, "shared", 321)
	r := clients[2].send(t, "GET "+metric+" COUNT")
	noError(t, r)
	if r.(map[string]any)["aggregate"].(map[string]any)["count"] != json.Number("0") {
		t.Fatalf("delete count: %v", r)
	}
	// Remove only FlameDB projection keys, never unrelated Redis data. The next rank query rebuilds exactly.
	cache := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer cache.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	iter := cache.Scan(ctx, 0, "flamedb:*", 1000).Iterator()
	var keys []string
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		t.Fatal(err)
	}
	if len(keys) == 0 {
		t.Fatal("no real Redis projection keys to erase")
	}
	for _, key := range keys {
		if err := cache.Del(ctx, key).Err(); err != nil {
			t.Fatal(err)
		}
	}
	for _, w := range []*wire{clients[1], clients[2], replacement} {
		score(t, w, metric, "shared", 321)
	}
	noError(t, clients[2].send(t, `DELETE `+metric+` lb="shared"`))
	missing := replacement.send(t, `SCORE `+metric+` "shared"`)
	noError(t, missing)
	if missing.(map[string]any)["ranking"].(map[string]any)["score"] != nil {
		t.Fatalf("deleted score still present: %v", missing)
	}
}

func docker(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}
func waitDatabase(t *testing.T, container string, postgres bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		args := []string{"exec", container, "redis-cli", "ping"}
		if postgres {
			args = []string{"exec", container, "pg_isready", "-U", "flamedb", "-d", "flamedb"}
		}
		if exec.Command("docker", args...).Run() == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("database recovery timeout")
}

// This test stops only explicitly named throwaway containers. Run sequentially,
// separate from benchmark/SDK jobs, with FLAMEDB_FAILURE_INTEGRATION=1.
func TestDatabaseOutagesRejectAndRecover(t *testing.T) {
	if os.Getenv("FLAMEDB_FAILURE_INTEGRATION") != "1" {
		t.Skip("requires explicit FLAMEDB_FAILURE_INTEGRATION=1 and throwaway container names")
	}
	binary := prerequisite(t, "FLAMEDB_NEXTGEN_BINARY")
	pg := prerequisite(t, "FLAMEDB_TEST_POSTGRES_CONTAINER")
	rd := prerequisite(t, "FLAMEDB_TEST_REDIS_CONTAINER")
	for _, name := range []string{pg, rd} {
		if !strings.HasPrefix(name, "flamedb-nextgen-") {
			t.Fatalf("refusing outage of non-test container %q", name)
		}
	}
	w := connect(t, startServer(t, binary, true), "compat-key")
	metric := fmt.Sprintf("outage%d", time.Now().UnixNano())
	noError(t, w.send(t, `WRITE `+metric+` 1 lb="player"`))
	// Redis is disposable: confirmed writes and exact rank remain available from PostgreSQL.
	docker(t, "stop", "-t", "1", rd)
	t.Cleanup(func() { _ = exec.Command("docker", "start", rd).Run() })
	noError(t, w.send(t, `WRITE `+metric+` 2 lb="player"`))
	score(t, w, metric, "player", 3)
	docker(t, "start", rd)
	waitDatabase(t, rd, false)
	score(t, w, metric, "player", 3)
	// A write while PostgreSQL is stopped must not be acknowledged as success.
	docker(t, "stop", "-t", "1", pg)
	t.Cleanup(func() { _ = exec.Command("docker", "start", pg).Run() })
	result := w.send(t, `WRITE `+metric+` 100 lb="player"`)
	if m, ok := result.(map[string]any); !ok || m["error"] == nil {
		t.Fatalf("DB outage acknowledged mutation: %v", result)
	}
	docker(t, "start", pg)
	waitDatabase(t, pg, true)
	score(t, w, metric, "player", 3)
	noError(t, w.send(t, `WRITE `+metric+` 4 lb="player"`))
	score(t, w, metric, "player", 7)
}
