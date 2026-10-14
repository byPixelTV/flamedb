package analytics

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/byPixelTV/flamedb/internal/query"
	"github.com/redis/go-redis/v9"
)

// stalledRedis replies just far enough to stall the chosen stage. This covers
// connection establishment, a cold rebuild and a cached Lua read separately.
func stalledRedis(t *testing.T, stage string) string {
	return stalledRedisSwitch(t, stage, nil)
}

func stalledRedisSwitch(t *testing.T, stage string, blocked *atomic.Bool) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	stop, accepted := make(chan struct{}), make(chan struct{})
	var handlers sync.WaitGroup
	var mutex sync.Mutex
	connections := []net.Conn{}
	go func() {
		defer close(accepted)
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			mutex.Lock()
			connections = append(connections, connection)
			mutex.Unlock()
			handlers.Add(1)
			go func(connection net.Conn) {
				defer handlers.Done()
				defer connection.Close()
				reader := bufio.NewReader(connection)
				for {
					command, err := readRedisCommand(reader)
					if err != nil {
						return
					}
					if stage == "connect" || (blocked != nil && blocked.Load()) {
						<-stop
						return
					}
					response := ""
					switch strings.ToUpper(command[0]) {
					case "HELLO":
						response = "-ERR unknown command 'hello'\r\n"
					case "CLIENT":
						response = "+OK\r\n"
					case "COMMAND":
						response = "*1\r\n*6\r\n$3\r\nget\r\n:2\r\n*1\r\n$8\r\nreadonly\r\n:1\r\n:1\r\n:1\r\n"
					case "GET":
						response = "$13\r\nfixture-epoch\r\n"
					case "EVALSHA", "EVAL":
						if stage == "rebuild" {
							// The atomic read rejects this generation. The next
							// rebuild pipeline stalls on its first ZADD below.
							response = "$-1\r\n"
						} else {
							<-stop
							return
						}
					default:
						<-stop
						return
					}
					if _, err = io.WriteString(connection, response); err != nil {
						return
					}
				}
			}(connection)
		}
	}()
	t.Cleanup(func() {
		close(stop)
		listener.Close()
		<-accepted
		mutex.Lock()
		for _, connection := range connections {
			connection.Close()
		}
		mutex.Unlock()
		handlers.Wait()
	})
	return listener.Addr().String()
}

func readRedisCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("expected Redis array")
	}
	count, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil || count < 1 || count > 100 {
		return nil, fmt.Errorf("invalid Redis command size")
	}
	command := make([]string, count)
	for i := range command {
		line, err = reader.ReadString('\n')
		if err != nil || !strings.HasPrefix(line, "$") {
			return nil, fmt.Errorf("invalid Redis bulk length")
		}
		length, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil || length < 0 || length > 1<<20 {
			return nil, fmt.Errorf("invalid Redis bulk length")
		}
		data := make([]byte, length+2)
		if _, err = io.ReadFull(reader, data); err != nil {
			return nil, err
		}
		command[i] = string(data[:length])
	}
	return command, nil
}

func TestRedisAttemptBudgetPreservesPostgresFallback(t *testing.T) {
	seed, options := integration(t)
	for _, stage := range []string{"connect", "rebuild", "read"} {
		t.Run(stage, func(t *testing.T) {
			metric := "cache_budget_" + token()
			run(t, seed, &query.Query{Type: query.QueryTypeSet, Metric: metric, Value: 11, UpdateLB: true, LBEntityID: "x"})
			run(t, seed, &query.Query{Type: query.QueryTypeSet, Metric: metric, Value: 5, UpdateLB: true, LBEntityID: "y"})
			// Positive control: the authoritative data is available before the
			// cache fault; the fixture never manufactures the returned scores.
			if score := run(t, seed, &query.Query{Type: query.QueryTypeScore, Metric: metric, EntityID: "x"}).Ranking.Score; score == nil || *score != 11 {
				t.Fatal("invalid SQL seed")
			}
			broken := options
			broken.RedisMode = ""
			broken.RedisOptions = redis.UniversalOptions{Addrs: []string{stalledRedis(t, stage)}, Protocol: 2, DisableIdentity: true, MaxRetries: -1}
			broken.RequestTimeout = 600 * time.Millisecond
			engine, err := Open(context.Background(), broken)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(engine.Close)
			for _, kind := range []query.QueryType{query.QueryTypeRank, query.QueryTypeAround, query.QueryTypeLeaderboardCount, query.QueryTypeLeaderboard} {
				started := time.Now()
				result, err := engine.Execute(&query.Query{Type: kind, Metric: metric, EntityID: "x", Radius: 1, Limit: 2})
				if err != nil {
					t.Fatalf("%s stalled Redis consumed PostgreSQL fallback budget: %v", kind, err)
				}
				if elapsed := time.Since(started); elapsed >= 500*time.Millisecond {
					t.Fatalf("%s left no useful fallback reserve: %s", kind, elapsed)
				}
				if kind == query.QueryTypeLeaderboard {
					if len(result.Leaderboard) != 2 || result.Leaderboard[0].EntityID != "x" || result.Leaderboard[0].Value != 11 {
						t.Fatalf("SQL fallback leaderboard: %v", result)
					}
				} else {
					if result.Ranking.Count != 2 {
						t.Fatalf("SQL fallback count: %v", result.Ranking)
					}
					if kind != query.QueryTypeLeaderboardCount && (result.Ranking.Rank == nil || *result.Ranking.Rank != 1 || result.Ranking.Score == nil || *result.Ranking.Score != 11) {
						t.Fatalf("SQL fallback rank/score: %v", result.Ranking)
					}
					if kind == query.QueryTypeAround && len(result.Ranking.Around) != 2 {
						t.Fatalf("SQL fallback around: %v", result.Ranking)
					}
				}
			}
			if stage == "connect" {
				// More than one worker wave must still share a single cache budget.
				engine.batchSize = 2048
				items := make([]*query.Query, 1024)
				for i := range items {
					items[i] = &query.Query{Type: query.QueryTypeWrite, Metric: metric + strconv.Itoa(i%32), Timestamp: int64(i/32 + 1), Value: 3, UpdateLB: true, LBEntityID: "x"}
				}
				started := time.Now()
				batch, err := engine.ExecuteBatch(items)
				if err != nil || batch.Accepted != len(items) || batch.Failed != 0 {
					t.Fatalf("durable batch failed on optional Redis: %v %v", batch, err)
				}
				if elapsed := time.Since(started); elapsed >= 500*time.Millisecond {
					t.Fatalf("per-metric cache attempts exhausted batch response deadline: %s", elapsed)
				}
				for _, item := range items[:32] {
					if score := run(t, seed, &query.Query{Type: query.QueryTypeScore, Metric: item.Metric, EntityID: "x"}).Ranking.Score; score == nil || *score != 96 {
						t.Fatal("acknowledged batch score not durable")
					}
				}
			}
		})
	}
	t.Run("cold_cluster_startup", func(t *testing.T) {
		for _, mode := range []string{"cluster", "inferred"} {
			t.Run(mode, func(t *testing.T) {
				metric := "cold_cluster_" + token()
				run(t, seed, &query.Query{Type: query.QueryTypeSet, Metric: metric, Value: 17, UpdateLB: true, LBEntityID: "x"})
				broken := options
				broken.RedisMode = "cluster"
				broken.RedisOptions = redis.UniversalOptions{Addrs: []string{stalledRedis(t, "connect")}, Protocol: 2, DisableIdentity: true, MaxRetries: -1, ReadOnly: true, RouteByLatency: true, RouteRandomly: true}
				if mode == "inferred" {
					broken.RedisMode = ""
					broken.RedisOptions.Addrs = append(broken.RedisOptions.Addrs, broken.RedisOptions.Addrs[0])
				}
				broken.RequestTimeout = 600 * time.Millisecond
				engine, err := Open(context.Background(), broken)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(engine.Close)
				started := time.Now()
				result, err := engine.Execute(&query.Query{Type: query.QueryTypeRank, Metric: metric, EntityID: "x"})
				if err != nil || result.Ranking.Score == nil || *result.Ranking.Score != 17 {
					t.Fatalf("unprimed cluster startup exhausted SQL fallback: %v %v", result, err)
				}
				if elapsed := time.Since(started); elapsed >= 500*time.Millisecond {
					t.Fatalf("unprimed COMMAND metadata bypassed budget: %s", elapsed)
				}
			})
		}
	})
	t.Run("stale_cluster_topology", testClusterTopologyRecovery)
}
