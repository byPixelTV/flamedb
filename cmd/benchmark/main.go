package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"math"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type benchMode string

const (
	modeWrite benchMode = "write"
	modeRead  benchMode = "read"
	modeMixed benchMode = "mixed"
)

type benchClient struct {
	conn    net.Conn
	r       *bufio.Reader
	w       *bufio.Writer
	timeout time.Duration
}

type workerResult struct {
	writeLat []int64
	readLat  []int64
}

func main() {
	addr := flag.String("addr", "127.0.0.1:7777", "server address")
	key := flag.String("key", "flame_abc123", "api key")
	workers := flag.Int("workers", 100, "number of concurrent workers")
	duration := flag.Duration("duration", 10*time.Second, "benchmark duration")
	metric := flag.String("metric", "bench_kills", "metric name")
	mode := flag.String("mode", "write", "mode: write|read|mixed")
	timeout := flag.Duration("timeout", 2*time.Second, "per-request timeout")
	flag.Parse()

	m := benchMode(strings.ToLower(strings.TrimSpace(*mode)))
	switch m {
	case modeWrite, modeRead, modeMixed:
	default:
		fmt.Fprintf(os.Stderr, "invalid mode: %s\n", *mode)
		os.Exit(2)
	}

	if *workers <= 0 {
		fmt.Fprintln(os.Stderr, "workers must be > 0")
		os.Exit(2)
	}
	if *duration <= 0 {
		fmt.Fprintln(os.Stderr, "duration must be > 0")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	var writeCount int64
	var readCount int64
	var writeErrors int64
	var readErrors int64

	results := make(chan workerResult, *workers)
	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			var client *benchClient
			var localWrites []int64
			var localReads []int64
			var op int

			for {
				select {
				case <-ctx.Done():
					if client != nil {
						client.close()
					}
					results <- workerResult{writeLat: localWrites, readLat: localReads}
					return
				default:
				}

				if client == nil {
					c, err := newBenchClient(*addr, *key, *timeout)
					if err != nil {
						atomic.AddInt64(&writeErrors, 1)
						time.Sleep(10 * time.Millisecond)
						continue
					}
					client = c
				}

				isWrite := false
				switch m {
				case modeWrite:
					isWrite = true
				case modeRead:
					isWrite = false
				case modeMixed:
					isWrite = op%2 == 0
				}
				op++

				var line string
				if isWrite {
					line = fmt.Sprintf("WRITE %s 1", *metric)
				} else {
					line = fmt.Sprintf("GET %s LIMIT 1 ORDER DESC", *metric)
				}

				started := time.Now()
				err := client.send(line)
				dur := time.Since(started).Nanoseconds()
				if err != nil {
					client.close()
					client = nil
					if isWrite {
						atomic.AddInt64(&writeErrors, 1)
					} else {
						atomic.AddInt64(&readErrors, 1)
					}
					continue
				}

				if isWrite {
					atomic.AddInt64(&writeCount, 1)
					localWrites = append(localWrites, dur)
				} else {
					atomic.AddInt64(&readCount, 1)
					localReads = append(localReads, dur)
				}
			}
		}(i)
	}

	wg.Wait()
	close(results)

	var writeLat []int64
	var readLat []int64
	for r := range results {
		writeLat = append(writeLat, r.writeLat...)
		readLat = append(readLat, r.readLat...)
	}

	elapsed := time.Since(start)
	writes := atomic.LoadInt64(&writeCount)
	reads := atomic.LoadInt64(&readCount)
	wErrors := atomic.LoadInt64(&writeErrors)
	rErrors := atomic.LoadInt64(&readErrors)

	fmt.Printf("mode: %s\n", m)
	fmt.Printf("duration: %s\n", elapsed.Round(time.Millisecond))
	if writes > 0 {
		fmt.Printf("writes: %d (%.1f ops/s)\n", writes, float64(writes)/elapsed.Seconds())
	}
	if reads > 0 {
		fmt.Printf("reads: %d (%.1f ops/s)\n", reads, float64(reads)/elapsed.Seconds())
	}
	fmt.Printf("errors: %d (write %d, read %d)\n", wErrors+rErrors, wErrors, rErrors)

	if len(writeLat) > 0 {
		p50 := percentile(writeLat, 0.50)
		p99 := percentile(writeLat, 0.99)
		fmt.Printf("write latency p50: %s, p99: %s\n", p50, p99)
	}
	if len(readLat) > 0 {
		p50 := percentile(readLat, 0.50)
		p99 := percentile(readLat, 0.99)
		fmt.Printf("read latency p50: %s, p99: %s\n", p50, p99)
	}
}

func newBenchClient(addr, key string, timeout time.Duration) (*benchClient, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}

	client := &benchClient{
		conn:    conn,
		r:       bufio.NewReader(conn),
		w:       bufio.NewWriter(conn),
		timeout: timeout,
	}

	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		conn.Close()
		return nil, err
	}

	if _, err := client.r.ReadString('\n'); err != nil {
		conn.Close()
		return nil, err
	}

	if _, err := client.w.WriteString("AUTH " + key + "\n"); err != nil {
		conn.Close()
		return nil, err
	}
	if err := client.w.Flush(); err != nil {
		conn.Close()
		return nil, err
	}
	if _, err := client.r.ReadString('\n'); err != nil {
		conn.Close()
		return nil, err
	}

	return client, nil
}

func (c *benchClient) send(line string) error {
	if c == nil {
		return fmt.Errorf("client is nil")
	}
	if err := c.conn.SetDeadline(time.Now().Add(c.timeout)); err != nil {
		return err
	}
	if _, err := c.w.WriteString(line + "\n"); err != nil {
		return err
	}
	if err := c.w.Flush(); err != nil {
		return err
	}
	_, err := c.r.ReadString('\n')
	return err
}

func (c *benchClient) close() {
	if c == nil || c.conn == nil {
		return
	}
	_ = c.conn.Close()
}

func percentile(values []int64, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	copyVals := make([]int64, len(values))
	copy(copyVals, values)
	sort.Slice(copyVals, func(i, j int) bool { return copyVals[i] < copyVals[j] })

	if p <= 0 {
		return time.Duration(copyVals[0])
	}
	if p >= 1 {
		return time.Duration(copyVals[len(copyVals)-1])
	}

	idx := int(math.Ceil(p*float64(len(copyVals)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(copyVals) {
		idx = len(copyVals) - 1
	}
	return time.Duration(copyVals[idx])
}
