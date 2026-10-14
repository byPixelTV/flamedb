package server

import (
	"fmt"
	"strings"
	"testing"
)

func TestPebbleTCPLeaderboardBatch128(t *testing.T) {
	c, scanner := testConnection(t, "user")
	var batch strings.Builder
	batch.WriteString("WRITE_BATCH\n")
	for i := 0; i < 128; i++ {
		fmt.Fprintf(&batch, "WRITE batch:kills 1 lb=\"p\" lb.team=\"a\" player=\"p\" ts=%d\n", i+1)
	}
	batch.WriteString("END")
	r := frame(t, c, scanner, batch.String())
	if r["ok"] != true || r["accepted"] != float64(128) || r["failed"] != float64(0) {
		t.Fatal(r)
	}
	for _, command := range []string{"LEADERBOARD batch:kills LIMIT 10", "LEADERBOARD batch:kills BOARD team LIMIT 10"} {
		r := frame(t, c, scanner, command)
		entries := r["leaderboard"].([]any)
		if len(entries) != 1 || entries[0].(map[string]any)["value"] != float64(128) {
			t.Fatal(command, r)
		}
	}
	r = frame(t, c, scanner, "GET batch:kills SUM WHERE player=\"p\"")
	aggregate := r["aggregate"].(map[string]any)
	if aggregate["value"] != float64(128) || aggregate["count"] != float64(128) {
		t.Fatal(r)
	}
}
