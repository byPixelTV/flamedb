package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/byPixelTV/flamedb/internal/aggregates"
	"github.com/byPixelTV/flamedb/internal/analytics"
	"github.com/byPixelTV/flamedb/internal/query"
	"github.com/byPixelTV/flamedb/internal/storage"
	"github.com/jackc/pgx/v5"
)

func snapshotDigest(t *testing.T, path string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(path, func(file string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(data)
		relative, _ := filepath.Rel(path, file)
		out[relative] = hex.EncodeToString(sum[:])
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func TestImportPreservesEventsAbsoluteScoresAndSource(t *testing.T) {
	dsn := os.Getenv("FLAMEDB_TEST_POSTGRES")
	if dsn == "" {
		if os.Getenv("FLAMEDB_INTEGRATION") == "1" {
			t.Fatal("FLAMEDB_TEST_POSTGRES required")
		}
		t.Skip("real PostgreSQL required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	database := fmt.Sprintf("flame_import_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{database}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		connection, err := pgx.Connect(cleanup, dsn)
		if err != nil {
			t.Error(err)
			return
		}
		defer connection.Close(cleanup)
		_, err = connection.Exec(cleanup, "DROP DATABASE "+pgx.Identifier{database}.Sanitize()+" WITH (FORCE)")
		if err != nil {
			t.Error(err)
		}
	})
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + database
	destination := parsed.String()
	source := t.TempDir()
	store, err := storage.Open(source, "none")
	if err != nil {
		t.Fatal(err)
	}
	executor := query.NewExecutor(store, aggregates.New(store.DB()))
	randomText := func(size int) string {
		data := make([]byte, size/2)
		if _, err := rand.Read(data); err != nil {
			t.Fatal(err)
		}
		return hex.EncodeToString(data)
	}
	largeMetric, largeEntity, largeBoard, largeTag := randomText(4096), randomText(4096), randomText(4096), randomText(4096)
	for _, command := range []string{`WRITE app:kills 3 lb="a" lb.team="red" player="a" ts=1704067200000000001`, `WRITE app:kills -2 lb="b" player="b" ts=1704067200000000002`, `SET app:kills 9 lb="a"`, `DELETE app:kills WHERE player="b"`, `WRITE app:kills 5 lb="nul\u0000entity" player="nul\u0000entity" weird="\xff" ts=1704067200000000003`,
		fmt.Sprintf(`WRITE %s 2 lb="%s" lb.%s="named" %s="old" ts=1704067200000000001`, largeMetric, largeEntity, largeBoard, largeTag),
		fmt.Sprintf(`WRITE %s 3 lb="%s" lb.%s="named" %s="new" ts=1704067200000000001`, largeMetric, largeEntity, largeBoard, largeTag),
		fmt.Sprintf(`SET %s 9 lb="%s"`, largeMetric, largeEntity),
	} {
		q, err := query.Parse(command)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = executor.Execute(q); err != nil {
			t.Fatal(err)
		}
	}
	commands := []string{`GET app:kills ORDER ASC`, `GET app:kills SUM`, `LEADERBOARD app:kills`, `LEADERBOARD app:kills BOARD team`, `GET app:kills WHERE weird="\xff"`, `LEADERBOARD app:kills FROM 2024-01-01 TO 2024-01-02 ENTITY player`}
	commands = append(commands, `GET `+largeMetric, `GET `+largeMetric+` SUM`, `LEADERBOARD `+largeMetric, `LEADERBOARD `+largeMetric+` BOARD `+largeBoard)
	statsCommand := `STATS ` + largeMetric + ` TAGS ` + largeTag
	statsQuery, err := query.Parse(statsCommand)
	if err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(5 * time.Second)
	for {
		result, err := executor.Execute(statsQuery)
		if err != nil {
			t.Fatal(err)
		}
		if result.Stats.TagStats[0].Cardinality == 2 {
			break
		}
		if time.Now().After(until) {
			t.Fatal("legacy large-tag cardinality did not settle")
		}
		time.Sleep(10 * time.Millisecond)
	}
	commands = append(commands, statsCommand)
	expected := map[string]string{}
	for _, command := range commands {
		q, _ := query.Parse(command)
		result, err := executor.Execute(q)
		if err != nil {
			t.Fatal(err)
		}
		data, _ := json.Marshal(result)
		expected[command] = string(data)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	before := snapshotDigest(t, source)
	if err = importSnapshot(ctx, source, destination, true); err != nil {
		t.Fatal(err)
	}
	if after := snapshotDigest(t, source); !reflect.DeepEqual(before, after) {
		t.Fatalf("source contents changed: %v %v", before, after)
	}
	next, err := analytics.Open(ctx, analytics.Options{DSN: destination, Timescale: true})
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	for _, command := range commands {
		q, _ := query.Parse(command)
		result, err := next.ExecuteContext(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		data, _ := json.Marshal(result)
		if string(data) != expected[command] {
			t.Errorf("%s: %s != %s", command, data, expected[command])
		}
	}
	if err = importSnapshot(ctx, source, destination, true); err == nil {
		t.Fatal("overwrote nonempty destination")
	}
}

func TestCorruptSnapshotRowsReject(t *testing.T) {
	for _, fixture := range []struct {
		kind, key string
		value     []byte
	}{{"scores", "lb-entity:metric:entity", []byte{1}}, {"tags", "card:metric:tag", nil}, {"events", "metric:\x00\x00\x00\x00\x00\x00\x00\x01", []byte("bad-json")}} {
		if _, _, err := decode(fixture.kind, []byte(fixture.key), fixture.value); err == nil {
			t.Fatal(fixture)
		}
	}
}
