package ha

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/byPixelTV/flamedb/internal/analytics"
	"github.com/byPixelTV/flamedb/internal/query"
	"github.com/jackc/pgx/v5"
)

func required(t *testing.T, key string) string {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		t.Fatalf("%s is required in explicit HA mode", key)
	}
	return value
}

// This explicit opt-in fences only a labelled test primary, promotes its real
// physical standby, and verifies the same pool reconnects without write retries.
func TestSynchronousPostgresQuorumAndFailover(t *testing.T) {
	if os.Getenv("FLAMEDB_HA_INTEGRATION") != "1" {
		t.Skip("dedicated physical PostgreSQL primary/standby required")
	}
	primary := required(t, "FLAMEDB_HA_PRIMARY_CONTAINER")
	if !strings.HasPrefix(primary, "flamedb-nextgen-ha-") {
		t.Fatal("refusing to fence a non-test primary")
	}
	label, err := exec.Command("docker", "inspect", primary, "--format", `{{index .Config.Labels "flamedb.test"}}`).CombinedOutput()
	if err != nil || strings.TrimSpace(string(label)) != "nextgen-ha" {
		t.Fatalf("test ownership label missing: %v %s", err, label)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	peer, err := pgx.Connect(ctx, required(t, "FLAMEDB_HA_REPLICA_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close(ctx)
	var standby bool
	if err = peer.QueryRow(ctx, `SELECT pg_is_in_recovery()`).Scan(&standby); err != nil || !standby {
		t.Fatalf("physical standby required: %v %v", standby, err)
	}
	engine, err := analytics.Open(ctx, analytics.Options{DSN: required(t, "FLAMEDB_HA_FAILOVER_DSN"), Timescale: true, QuorumReplicationFactor: 2, RequestTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	metric := fmt.Sprintf("ha%d", time.Now().UnixNano())
	if _, err = engine.ExecuteContext(ctx, &query.Query{Type: query.QueryTypeWrite, Metric: metric, Timestamp: 1, Value: 3, UpdateLB: true, LBEntityID: "entity", Quorum: true}); err != nil {
		t.Fatal(err)
	}
	var raw int
	var value float64
	if err = peer.QueryRow(ctx, `SELECT count(*) FROM flame_events WHERE metric=$1`, metric).Scan(&raw); err != nil || raw != 1 {
		t.Fatalf("ACK not applied on replica: %d %v", raw, err)
	}
	if err = peer.QueryRow(ctx, `SELECT value FROM flame_scores WHERE metric=$1 AND board='' AND entity='entity'`, metric).Scan(&value); err != nil || value != 3 {
		t.Fatalf("replica score: %g %v", value, err)
	}
	if out, err := exec.Command("docker", "kill", "--signal", "KILL", primary).CombinedOutput(); err != nil {
		t.Fatalf("primary fencing: %v %s", err, out)
	}
	// Keep the old primary fenced: promotion must never create two write leaders.
	var promoted bool
	if err = peer.QueryRow(ctx, `SELECT pg_promote(true,20)`).Scan(&promoted); err != nil || !promoted {
		t.Fatalf("promotion: %v %v", promoted, err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		if err = engine.Ready(ctx); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pool did not fail over: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	rank, err := engine.ExecuteContext(ctx, &query.Query{Type: query.QueryTypeScore, Metric: metric, EntityID: "entity"})
	if err != nil || rank.Ranking.Score == nil || *rank.Ranking.Score != 3 {
		t.Fatalf("confirmed score after promotion: %v %v", rank, err)
	}
	// The promoted singleton has no synchronous standbys; it cannot grant RF2.
	if _, err = peer.Exec(ctx, `ALTER SYSTEM SET synchronous_standby_names='ANY 1 (flame_standby)'`); err != nil {
		t.Fatal(err)
	}
	if _, err = peer.Exec(ctx, `SELECT pg_reload_conf()`); err != nil {
		t.Fatal(err)
	}
	for {
		var setting string
		if err = peer.QueryRow(ctx, `SHOW synchronous_standby_names`).Scan(&setting); err != nil {
			t.Fatal(err)
		}
		if setting == "ANY 1 (flame_standby)" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	if _, err = engine.ExecuteContext(ctx, &query.Query{Type: query.QueryTypeWrite, Metric: metric, Timestamp: 2, Value: 100, UpdateLB: true, LBEntityID: "entity", Quorum: true}); err == nil {
		t.Fatal("unavailable quorum acknowledged")
	}
	if _, err = peer.Exec(ctx, `ALTER SYSTEM SET synchronous_standby_names=''`); err != nil {
		t.Fatal(err)
	}
	if _, err = peer.Exec(ctx, `SELECT pg_reload_conf()`); err != nil {
		t.Fatal(err)
	}
	for {
		var setting string
		if err = peer.QueryRow(ctx, `SHOW synchronous_standby_names`).Scan(&setting); err != nil {
			t.Fatal(err)
		}
		if setting == "" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	if _, err = engine.ExecuteContext(ctx, &query.Query{Type: query.QueryTypeWrite, Metric: metric, Timestamp: 3, Value: 2, UpdateLB: true, LBEntityID: "entity"}); err != nil {
		t.Fatal(err)
	}
	result, err := engine.ExecuteContext(ctx, &query.Query{Type: query.QueryTypeGet, Metric: metric, Aggregate: query.AggCount})
	if err != nil || result.Aggregate.Count != 2 {
		t.Fatalf("rejected quorum left event: %v %v", result, err)
	}
	rank, err = engine.ExecuteContext(ctx, &query.Query{Type: query.QueryTypeScore, Metric: metric, EntityID: "entity"})
	if err != nil || rank.Ranking.Score == nil || *rank.Ranking.Score != 5 {
		t.Fatalf("post-failover mutation: %v %v", rank, err)
	}
	t.Log("physical standby remote_apply ACK, abrupt primary loss, fenced promotion, pool reconnection and unavailable-quorum rejection verified")
}
