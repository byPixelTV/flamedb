package flamedb

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
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

func TestNamespacedIdentifiers(t *testing.T) {
	received := make(chan string, 1)
	c := fakeServer(t, func(line string) string { received <- line; return `{}` })
	err := c.Write(context.Background(), "smp:", 1, WriteOpts{Tags: map[string]string{"player:id": "p:1"}})
	if err != nil {
		t.Fatal(err)
	}
	line := <-received
	if !strings.HasPrefix(line, "WRITE smp: 1") || !strings.Contains(line, `player:id="p:1"`) {
		t.Fatal(line)
	}
}

func TestReconnectWithoutReplayingWrite(t *testing.T) {
	for _, mode := range []string{"disconnect", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()
			commands := make(chan string, 16)
			done := make(chan error, 1)
			go func() {
				for attempt := 0; attempt < 2; attempt++ {
					conn, err := ln.Accept()
					if err != nil {
						done <- err
						return
					}
					defer conn.Close()
					conn.SetDeadline(time.Now().Add(5 * time.Second))
					sc := bufio.NewScanner(conn)
					fmt.Fprintln(conn, `{"auth":"required"}`)
					if !sc.Scan() || sc.Text() != "AUTH test" {
						done <- fmt.Errorf("missing auth")
						return
					}
					fmt.Fprintln(conn, `{"auth":"ok"}`)
					if attempt == 0 {
						if !sc.Scan() {
							done <- fmt.Errorf("missing first write")
							return
						}
						commands <- sc.Text()
						if mode == "timeout" && sc.Scan() {
							done <- fmt.Errorf("replayed on expired connection")
							return
						}
						conn.Close()
					} else {
						for sc.Scan() {
							commands <- sc.Text()
							fmt.Fprintln(conn, `{}`)
						}
					}
				}
				done <- nil
			}()
			host, port, _ := net.SplitHostPort(ln.Addr().String())
			n, _ := strconv.Atoi(port)
			client, err := New(Config{Host: host, Port: n, APIKey: "test", Timeout: 300 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			if err := client.Write(context.Background(), "first", 1, WriteOpts{}); err == nil {
				t.Fatal("expected lost ACK")
			}
			var wg sync.WaitGroup
			failures := make(chan error, 8)
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					failures <- client.Write(context.Background(), fmt.Sprintf("next%d", i), 1, WriteOpts{})
				}(i)
			}
			wg.Wait()
			close(failures)
			for err := range failures {
				if err != nil {
					t.Fatal(err)
				}
			}
			client.Close()
			if err := client.Write(context.Background(), "closed", 1, WriteOpts{}); err == nil {
				t.Fatal("reconnected after Close")
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("server stuck")
			}
			close(commands)
			seen := map[string]int{}
			for line := range commands {
				seen[strings.Fields(line)[1]]++
			}
			if len(seen) != 9 || seen["first"] != 1 {
				t.Fatalf("unexpected commands: %v", seen)
			}
			for name, count := range seen {
				if count != 1 {
					t.Fatalf("replayed %s", name)
				}
			}
		})
	}
}

func TestReconnectHandshakeHonorsContext(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		first, err := ln.Accept()
		if err != nil {
			return
		}
		defer first.Close()
		first.SetDeadline(time.Now().Add(3 * time.Second))
		sc := bufio.NewScanner(first)
		fmt.Fprintln(first, `{"auth":"required"}`)
		if !sc.Scan() {
			return
		}
		fmt.Fprintln(first, `{"auth":"ok"}`)
		if !sc.Scan() {
			return
		}
		first.Close()
		second, err := ln.Accept()
		if err != nil {
			return
		}
		defer second.Close()
		second.SetDeadline(time.Now().Add(3 * time.Second))
		// Deliberately withhold the greeting during reconnect.
		bufio.NewScanner(second).Scan()
	}()
	host, port, _ := net.SplitHostPort(ln.Addr().String())
	n, _ := strconv.Atoi(port)
	client, err := New(Config{Host: host, Port: n, APIKey: "test", Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.Write(context.Background(), "first", 1, WriteOpts{}); err == nil {
		t.Fatal("missing lost response error")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := client.Write(ctx, "second", 1, WriteOpts{}); err == nil {
		t.Fatal("missing reconnect timeout")
	}
	if time.Since(start) > time.Second {
		t.Fatal("reconnect ignored context")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reconnect socket not closed")
	}
}
