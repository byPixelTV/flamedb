package server

import (
	"bufio"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/byPixelTV/flamedb/internal/aggregates"
	"github.com/byPixelTV/flamedb/internal/auth"
	"github.com/byPixelTV/flamedb/internal/cluster"
	"github.com/byPixelTV/flamedb/internal/config"
	"github.com/byPixelTV/flamedb/internal/storage"
)

func TestGroupedWireReadsLargeHistoryWithReadonlyKey(t *testing.T) {
	store, err := storage.Open(t.TempDir(), "none")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	events := make([]storage.Event, 60000)
	for i := range events {
		events[i] = storage.Event{Metric: "regions", Timestamp: int64(i + 1), Value: float64(i), Tags: map[string]string{"region": string(rune('a' + i%2))}}
	}
	if err := store.WriteEvents(events, true); err != nil {
		t.Fatal(err)
	}
	srv := New(store, aggregates.New(store.DB()), auth.New(config.AuthConfig{Keys: []config.APIKey{{Name: "grafana", Key: "readonly", Permissions: []string{"read"}}}}), cluster.New(cluster.Node{ID: "self"}, 150, "internal", 1), "internal", nil, "")
	client, peer := net.Pipe()
	defer client.Close()
	done := make(chan struct{})
	go func() { srv.handleConn(peer); close(done) }()
	defer func() { client.Close(); <-done }()
	client.SetDeadline(time.Now().Add(10 * time.Second))
	sc := bufio.NewScanner(client)
	if !sc.Scan() {
		t.Fatal("no challenge")
	}
	fmt.Fprintln(client, "AUTH readonly")
	if !sc.Scan() {
		t.Fatal("no auth")
	}
	result := frame(t, client, sc, "GET regions GROUP BY 1s COUNT TAGS region POINTS 8")
	if result["error"] != nil {
		t.Fatal(result)
	}
	total := 0.0
	for _, s := range result["grouped"].(map[string]any)["series"].([]any) {
		for _, p := range s.(map[string]any)["points"].([]any) {
			total += p.(map[string]any)["value"].(float64)
		}
	}
	if total != 60000 {
		t.Fatalf("count=%v", total)
	}
	result = frame(t, client, sc, "GET regions TAGS region LAST")
	if len(result["grouped"].(map[string]any)["latest"].([]any)) != 2 {
		t.Fatal(result)
	}
	if frame(t, client, sc, "WRITE regions 1")["error"] == nil {
		t.Fatal("read-only grouped session accepted a write")
	}
}
