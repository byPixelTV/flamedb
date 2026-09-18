package flamedb

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

func fakeServer(t *testing.T, respond func(string) string) *Client {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		fmt.Fprintln(conn, `{"auth":"required"}`)
		sc := bufio.NewScanner(conn)
		if !sc.Scan() {
			return
		}
		fmt.Fprintln(conn, `{"auth":"ok"}`)
		for sc.Scan() {
			reply := respond(sc.Text())
			if reply != "" {
				fmt.Fprintln(conn, reply)
			}
		}
	}()
	host, port, _ := net.SplitHostPort(ln.Addr().String())
	n, _ := strconv.Atoi(port)
	client, err := New(Config{Host: host, Port: n, APIKey: "test", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}
func TestResultMappingsAndClosedClient(t *testing.T) {
	c := fakeServer(t, func(line string) string {
		if strings.HasPrefix(line, "STATS") {
			return `{"stats":{"metric":"m","tag_stats":[{"tag_key":"p","cardinality":4}]}}`
		}
		return `{"leaderboard":[{"entity_id":"p","value":42}]}`
	})
	rows, err := c.Leaderboard(context.Background(), "m", LeaderboardOpts{})
	if err != nil || len(rows) != 1 || rows[0].Score != 42 {
		t.Fatalf("%v %v", rows, err)
	}
	stats, err := c.Stats(context.Background(), "m", []string{"p"})
	if err != nil || stats.Metric != "m" || len(stats.TagStats) != 1 {
		t.Fatalf("%v %v", stats, err)
	}
	c.Close()
	if err = c.Write(context.Background(), "m", 1, WriteOpts{}); err == nil {
		t.Fatal("write after close")
	}
}
func TestCancellationClosesAmbiguousConnection(t *testing.T) {
	c := fakeServer(t, func(string) string { return "" })
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := c.Get(ctx, []string{"m"}, GetOpts{}); err == nil {
		t.Fatal("missing timeout")
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("context ignored")
	}
	if _, err := c.Get(context.Background(), []string{"m"}, GetOpts{}); err == nil {
		t.Fatal("reused timed-out connection")
	}
}
func TestQuotedValuesAndInjection(t *testing.T) {
	line := buildWrite("m", 1, WriteOpts{Tags: map[string]string{"tag": "a\"\nb"}})
	if strings.Contains(line, "\n") {
		t.Fatal(line)
	}
	c := &Client{}
	if err := c.Write(context.Background(), "m 2 lb=", 1, WriteOpts{}); err == nil {
		t.Fatal("invalid metric accepted")
	}
}
