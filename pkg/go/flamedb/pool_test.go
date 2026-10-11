package flamedb

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func pooledServer(t *testing.T, cfg Config, respond func(string) string) *Client {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	sockets := map[net.Conn]bool{}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			sockets[conn] = true
			mu.Unlock()
			go func() {
				defer conn.Close()
				fmt.Fprintln(conn, `{"auth":"required"}`)
				sc := bufio.NewScanner(conn)
				if !sc.Scan() || sc.Text() != "AUTH test" {
					return
				}
				fmt.Fprintln(conn, `{"auth":"ok"}`)
				for sc.Scan() {
					line := sc.Text()
					if line == "WRITE_BATCH" {
						var frame []string
						for sc.Scan() {
							frame = append(frame, sc.Text())
							if sc.Text() == "END" {
								break
							}
						}
						line += "|" + strings.Join(frame, "|")
					}
					if reply := respond(line); reply != "" {
						fmt.Fprintln(conn, reply)
					}
				}
			}()
		}
	}()
	host, port, _ := net.SplitHostPort(ln.Addr().String())
	cfg.Host = host
	cfg.Port, _ = strconv.Atoi(port)
	cfg.APIKey = "test"
	client, err := New(cfg)
	if err != nil {
		ln.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		client.Close()
		ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for conn := range sockets {
			conn.Close()
		}
	})
	return client
}

func waitCommand(t *testing.T, started <-chan struct{}) {
	t.Helper()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("test server did not receive command")
	}
}

func TestConnectUsesOneDeadlineForChallengeAndAuthentication(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	release, done := make(chan struct{}), make(chan struct{})
	defer close(release)
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(2 * time.Second))
		time.Sleep(100 * time.Millisecond)
		fmt.Fprintln(conn, `{"auth":"required"}`)
		if !bufio.NewScanner(conn).Scan() {
			return
		}
		<-release
		fmt.Fprintln(conn, `{"auth":"ok"}`)
	}()
	host, port, _ := net.SplitHostPort(ln.Addr().String())
	n, _ := strconv.Atoi(port)
	started := time.Now()
	c, err := New(Config{Host: host, Port: n, APIKey: "test", Timeout: 300 * time.Millisecond})
	if err == nil {
		c.Close()
		t.Fatal("expected total handshake timeout")
	}
	if time.Since(started) >= 380*time.Millisecond {
		t.Fatal("authentication got a fresh deadline after the greeting")
	}
}

func TestPoolReadIsolationAndResponseOwnership(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	timings := make(chan CommandTiming, 64)
	c := pooledServer(t, Config{Timeout: 3 * time.Second, OnCommandTiming: func(v CommandTiming) { timings <- v }}, func(line string) string {
		if strings.HasPrefix(line, "WRITE_BATCH") {
			close(started)
			<-release
			return `{"ok":true,"accepted":1,"failed":0}`
		}
		metric := strings.Fields(line)[1]
		return fmt.Sprintf(`{"leaderboard":[{"entity_id":%q,"value":42}]}`, metric)
	})
	write := make(chan error, 1)
	go func() {
		_, err := c.WriteBatch(context.Background(), []WriteBatchItem{{Metric: "blocked", Value: 1}})
		write <- err
	}()
	waitCommand(t, started)
	var wg sync.WaitGroup
	errors := make(chan error, 40)
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			metric := fmt.Sprintf("clan:%d", i)
			rows, err := c.Leaderboard(ctx, metric, LeaderboardOpts{})
			if err == nil && (len(rows) != 1 || rows[0].EntityID != metric || rows[0].Score != 42) {
				err = fmt.Errorf("wrong response for %s: %v", metric, rows)
			}
			errors <- err
		}(i)
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Error(err)
		}
	}
	select {
	case err := <-write:
		t.Fatalf("write unexpectedly completed: %v", err)
	default:
	}
	if len(c.connections) != 3 {
		t.Fatalf("pool not bounded: %d", len(c.connections))
	}
	for i := 0; i < 40; i++ {
		timing := <-timings
		if !timing.Read || !timing.Success || timing.RequestTime < 0 || timing.QueueWait < 0 {
			t.Fatalf("bad timing: %+v", timing)
		}
	}
}

func TestPoolQueuedCancellationNeverSends(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	var mu sync.Mutex
	var commands []string
	timings := make(chan CommandTiming, 8)
	c := pooledServer(t, Config{Timeout: 2 * time.Second, OnCommandTiming: func(v CommandTiming) { timings <- v }}, func(line string) string {
		mu.Lock()
		commands = append(commands, line)
		mu.Unlock()
		if strings.Contains(line, "first") {
			close(started)
			<-release
		}
		return `{}`
	})
	first := make(chan error, 1)
	go func() { first <- c.Write(context.Background(), "first", 1, WriteOpts{}) }()
	waitCommand(t, started)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := c.Write(ctx, "queued", 1, WriteOpts{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued deadline: %v", err)
	}
	timing := <-timings
	if timing.RequestTime != 0 || timing.Success || timing.QueueWait < 30*time.Millisecond {
		t.Fatalf("bad queued timing: %+v", timing)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(commands) != 1 {
		t.Fatalf("cancelled request sent: %v", commands)
	}
	select {
	case err := <-first:
		t.Fatalf("queued timeout affected active request: %v", err)
	default:
	}
}

func TestPoolTotalDeadlineIncludesWait(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	timings := make(chan CommandTiming, 8)
	c := pooledServer(t, Config{Timeout: 300 * time.Millisecond, OnCommandTiming: func(v CommandTiming) { timings <- v }}, func(line string) string {
		if strings.Contains(line, "first") {
			close(started)
			time.Sleep(100 * time.Millisecond)
		} else {
			<-release
		}
		return `{}`
	})
	first := make(chan error, 1)
	go func() { first <- c.Write(context.Background(), "first", 1, WriteOpts{}) }()
	waitCommand(t, started)
	if err := c.Write(context.Background(), "second", 1, WriteOpts{}); err == nil {
		t.Fatal("missing total deadline")
	}
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	a, b := <-timings, <-timings
	if a.Success {
		a = b
	}
	if a.Success || a.QueueWait < 50*time.Millisecond || a.RequestTime >= 280*time.Millisecond || a.QueueWait+a.RequestTime >= 380*time.Millisecond {
		t.Fatalf("deadline excluded pool wait: %+v", a)
	}
}

func TestPoolCloseWakesActiveAndQueuedCalls(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	c := pooledServer(t, Config{Timeout: 5 * time.Second}, func(string) string { close(started); <-release; return `{}` })
	results := make(chan error, 2)
	go func() { results <- c.Write(context.Background(), "first", 1, WriteOpts{}) }()
	waitCommand(t, started)
	go func() { results <- c.Write(context.Background(), "queued", 1, WriteOpts{}) }()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		select {
		case err := <-results:
			if err == nil {
				t.Fatal("closed call succeeded")
			}
		case <-time.After(time.Second):
			t.Fatal("close waited for socket deadline")
		}
	}
}

func TestPoolActiveCancellationAndTelemetryPanic(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	var mu sync.Mutex
	var commands []string
	c := pooledServer(t, Config{Timeout: 2 * time.Second, OnCommandTiming: func(CommandTiming) { panic("observer failed") }}, func(line string) string {
		mu.Lock()
		commands = append(commands, line)
		mu.Unlock()
		if strings.Contains(line, "blocked") {
			close(started)
			<-release
		}
		return `{}`
	})
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() { first <- c.Write(ctx, "blocked", 1, WriteOpts{}) }()
	waitCommand(t, started)
	cancel()
	select {
	case err := <-first:
		if err == nil {
			t.Fatal("missing cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not close active I/O")
	}
	if err := c.Write(context.Background(), "next", 1, WriteOpts{}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(commands) != 2 || !strings.Contains(commands[1], "next") {
		t.Fatalf("write replayed: %v", commands)
	}
}
