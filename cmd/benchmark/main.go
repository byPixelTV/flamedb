package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
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

	"github.com/byPixelTV/flamedb/internal/cluster"
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
	addrs := flag.String("addrs", "", "comma-separated server addresses; overrides --addr")
	nodeIDs := flag.String("node-ids", "", "comma-separated node IDs matching --addrs; defaults to node-1,node-2,...")
	key := flag.String("key", "flame_abc123", "api key")
	workers := flag.Int("workers", 100, "number of concurrent workers")
	duration := flag.Duration("duration", 10*time.Second, "benchmark duration")
	metric := flag.String("metric", "bench_kills", "metric name")
	metrics := flag.Int("metrics", 1, "number of metric shards to spread load across")
	mode := flag.String("mode", "write", "mode: write|read|mixed")
	routeMode := flag.String("route", "round-robin", "target routing: round-robin|owner|topology")
	replicationFactor := flag.Int("replication-factor", 1, "replication factor for --route owner")
	vnodes := flag.Int("vnodes", 150, "virtual nodes per physical node for --route owner")
	pipeline := flag.Int("pipeline", 1, "number of commands to pipeline per worker iteration")
	batchSize := flag.Int("batch-size", 1, "number of WRITE commands per native WRITE_BATCH request")
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
	if *metrics <= 0 {
		fmt.Fprintln(os.Stderr, "metrics must be > 0")
		os.Exit(2)
	}
	if *replicationFactor <= 0 {
		fmt.Fprintln(os.Stderr, "replication-factor must be > 0")
		os.Exit(2)
	}
	if *vnodes <= 0 {
		fmt.Fprintln(os.Stderr, "vnodes must be > 0")
		os.Exit(2)
	}
	if *pipeline <= 0 {
		fmt.Fprintln(os.Stderr, "pipeline must be > 0")
		os.Exit(2)
	}
	if *batchSize <= 0 {
		fmt.Fprintln(os.Stderr, "batch-size must be > 0")
		os.Exit(2)
	}
	if *duration <= 0 {
		fmt.Fprintln(os.Stderr, "duration must be > 0")
		os.Exit(2)
	}

	targets := parseAddrs(*addr, *addrs)
	if len(targets) == 0 {
		fmt.Fprintln(os.Stderr, "at least one address is required")
		os.Exit(2)
	}
	ids := parseNodeIDs(*nodeIDs, len(targets))
	if len(ids) != len(targets) {
		fmt.Fprintln(os.Stderr, "node-ids count must match address count")
		os.Exit(2)
	}
	router, err := newTargetRouter(*routeMode, targets, ids, *replicationFactor, *vnodes, *key, *timeout)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
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
			clients := make(map[string]*benchClient)
			var localWrites []int64
			var localReads []int64
			var op int
			defer func() {
				for _, client := range clients {
					client.close()
				}
			}()

			for {
				select {
				case <-ctx.Done():
					results <- workerResult{writeLat: localWrites, readLat: localReads}
					return
				default:
				}

				if *pipeline > 1 {
					ops := make([]benchOp, 0, *pipeline)
					for len(ops) < *pipeline {
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

						metricName := *metric
						if *metrics > 1 {
							metricName = fmt.Sprintf("%s_%d", *metric, (workerID+op)%*metrics)
						}
						line := fmt.Sprintf("GET %s LIMIT 1 ORDER DESC", metricName)
						if isWrite {
							line = fmt.Sprintf("WRITE %s 1", metricName)
						}
						ops = append(ops, benchOp{
							addr:    router.addrFor(metricName, workerID, op),
							line:    line,
							isWrite: isWrite,
						})
					}
					runPipelinedOps(ops, clients, *key, *timeout, &writeCount, &readCount, &writeErrors, &readErrors, &localWrites, &localReads)
					continue
				}

				if *batchSize > 1 && m == modeWrite {
					batches := make(map[string][]string)
					for j := 0; j < *batchSize; j++ {
						op++
						name := *metric
						if *metrics > 1 {
							name = fmt.Sprintf("%s_%d", *metric, (workerID+op)%*metrics)
						}
						targetAddr := router.addrFor(name, workerID, op)
						batches[targetAddr] = append(batches[targetAddr], fmt.Sprintf("WRITE %s 1", name))
					}
					started := time.Now()
					accepted := 0
					failed := 0
					for targetAddr, writes := range batches {
						client := clients[targetAddr]
						if client == nil {
							c, err := newBenchClient(targetAddr, *key, *timeout)
							if err != nil {
								failed += len(writes)
								continue
							}
							client = c
							clients[targetAddr] = c
						}

						lines := make([]string, 0, len(writes)+2)
						lines = append(lines, "WRITE_BATCH")
						lines = append(lines, writes...)
						lines = append(lines, "END")

						n, err := client.sendWriteBatch(lines)
						if err != nil {
							client.close()
							delete(clients, targetAddr)
							failed += len(writes)
							continue
						}
						accepted += n
						if n < len(writes) {
							failed += len(writes) - n
						}
					}
					perOp := time.Since(started).Nanoseconds() / int64(*batchSize)
					atomic.AddInt64(&writeCount, int64(accepted))
					atomic.AddInt64(&writeErrors, int64(failed))
					for j := 0; j < accepted; j++ {
						localWrites = append(localWrites, perOp)
					}
					continue
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
				metricName := *metric
				if *metrics > 1 {
					metricName = fmt.Sprintf("%s_%d", *metric, (workerID+op)%*metrics)
				}
				targetAddr := router.addrFor(metricName, workerID, op)
				client := clients[targetAddr]
				if client == nil {
					c, err := newBenchClient(targetAddr, *key, *timeout)
					if err != nil {
						if isWrite {
							atomic.AddInt64(&writeErrors, 1)
						} else {
							atomic.AddInt64(&readErrors, 1)
						}
						time.Sleep(10 * time.Millisecond)
						continue
					}
					client = c
					clients[targetAddr] = c
				}
				if isWrite {
					line = fmt.Sprintf("WRITE %s 1", metricName)
				} else {
					line = fmt.Sprintf("GET %s LIMIT 1 ORDER DESC", metricName)
				}

				started := time.Now()
				err := client.send(line)
				dur := time.Since(started).Nanoseconds()
				if err != nil {
					client.close()
					delete(clients, targetAddr)
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
	_, err := c.request(line)
	return err
}

func (c *benchClient) sendWriteBatch(lines []string) (int, error) {
	if c == nil {
		return 0, fmt.Errorf("client is nil")
	}
	if len(lines) == 0 {
		return 0, nil
	}
	if err := c.conn.SetDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, err
	}
	for _, line := range lines {
		if _, err := c.w.WriteString(line + "\n"); err != nil {
			return 0, err
		}
	}
	if err := c.w.Flush(); err != nil {
		return 0, err
	}
	resp, err := c.r.ReadBytes('\n')
	if err != nil {
		return 0, err
	}
	var out struct {
		OK       bool   `json:"ok"`
		Accepted int    `json:"accepted"`
		Error    string `json:"error"`
	}
	if err := json.Unmarshal(resp, &out); err != nil {
		return 0, fmt.Errorf("invalid batch response: %w", err)
	}
	if out.Error != "" {
		return 0, errors.New(out.Error)
	}
	return out.Accepted, nil
}

func (c *benchClient) pipeline(lines []string) error {
	if c == nil {
		return fmt.Errorf("client is nil")
	}
	if len(lines) == 0 {
		return nil
	}
	if err := c.conn.SetDeadline(time.Now().Add(c.timeout)); err != nil {
		return err
	}
	for _, line := range lines {
		if _, err := c.w.WriteString(line + "\n"); err != nil {
			return err
		}
	}
	if err := c.w.Flush(); err != nil {
		return err
	}
	for range lines {
		if _, err := c.r.ReadBytes('\n'); err != nil {
			return err
		}
	}
	return nil
}

func (c *benchClient) request(line string) ([]byte, error) {
	if c == nil {
		return nil, fmt.Errorf("client is nil")
	}
	if err := c.conn.SetDeadline(time.Now().Add(c.timeout)); err != nil {
		return nil, err
	}
	if _, err := c.w.WriteString(line + "\n"); err != nil {
		return nil, err
	}
	if err := c.w.Flush(); err != nil {
		return nil, err
	}
	resp, err := c.r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *benchClient) close() {
	if c == nil || c.conn == nil {
		return
	}
	_ = c.conn.Close()
}

type benchOp struct {
	addr    string
	line    string
	isWrite bool
}

func runPipelinedOps(
	ops []benchOp,
	clients map[string]*benchClient,
	key string,
	timeout time.Duration,
	writeCount, readCount, writeErrors, readErrors *int64,
	localWrites, localReads *[]int64,
) {
	groups := make(map[string][]benchOp)
	for _, op := range ops {
		groups[op.addr] = append(groups[op.addr], op)
	}

	for addr, group := range groups {
		client := clients[addr]
		if client == nil {
			c, err := newBenchClient(addr, key, timeout)
			if err != nil {
				recordPipelineErrors(group, writeErrors, readErrors)
				continue
			}
			client = c
			clients[addr] = c
		}

		lines := make([]string, len(group))
		for i, op := range group {
			lines[i] = op.line
		}

		started := time.Now()
		err := client.pipeline(lines)
		perOp := time.Since(started).Nanoseconds() / int64(len(group))
		if err != nil {
			client.close()
			delete(clients, addr)
			recordPipelineErrors(group, writeErrors, readErrors)
			continue
		}

		for _, op := range group {
			if op.isWrite {
				atomic.AddInt64(writeCount, 1)
				*localWrites = append(*localWrites, perOp)
			} else {
				atomic.AddInt64(readCount, 1)
				*localReads = append(*localReads, perOp)
			}
		}
	}
}

func recordPipelineErrors(group []benchOp, writeErrors, readErrors *int64) {
	for _, op := range group {
		if op.isWrite {
			atomic.AddInt64(writeErrors, 1)
		} else {
			atomic.AddInt64(readErrors, 1)
		}
	}
}

func parseAddrs(addr, addrs string) []string {
	raw := strings.TrimSpace(addrs)
	if raw == "" {
		raw = strings.TrimSpace(addr)
	}
	if raw == "" {
		return nil
	}

	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func parseNodeIDs(raw string, n int) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		ids := make([]string, n)
		for i := 0; i < n; i++ {
			ids[i] = fmt.Sprintf("node-%d", i+1)
		}
		return ids
	}

	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

type targetRouter struct {
	mode              string
	targets           []string
	ring              *cluster.Ring
	replicationFactor int
}

func newTargetRouter(mode string, targets, nodeIDs []string, replicationFactor, vnodes int, key string, timeout time.Duration) (*targetRouter, error) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		mode = "round-robin"
	}
	r := &targetRouter{
		mode:              mode,
		targets:           targets,
		replicationFactor: replicationFactor,
	}
	switch mode {
	case "round-robin", "rr":
		r.mode = "round-robin"
		return r, nil
	case "owner":
		ring := cluster.NewRing(vnodes)
		for i, addr := range targets {
			ring.Add(cluster.Node{ID: nodeIDs[i], Addr: addr})
		}
		r.ring = ring
		return r, nil
	case "topology":
		topology, err := fetchTopology(targets[0], key, timeout)
		if err != nil {
			return nil, err
		}
		if topology.ReplicationFactor <= 0 {
			return nil, fmt.Errorf("topology returned invalid replication_factor: %d", topology.ReplicationFactor)
		}
		if topology.VirtualNodes <= 0 {
			return nil, fmt.Errorf("topology returned invalid virtual_nodes: %d", topology.VirtualNodes)
		}
		if len(topology.Nodes) == 0 {
			return nil, fmt.Errorf("topology returned no nodes")
		}

		ring := cluster.NewRing(topology.VirtualNodes)
		r.targets = r.targets[:0]
		for _, node := range topology.Nodes {
			ring.Add(cluster.Node{ID: node.ID, Addr: node.Addr})
			r.targets = append(r.targets, node.Addr)
		}
		r.mode = "owner"
		r.ring = ring
		r.replicationFactor = topology.ReplicationFactor
		return r, nil
	default:
		return nil, fmt.Errorf("invalid route: %s", mode)
	}
}

type topologyResponse struct {
	Cluster           string             `json:"cluster"`
	Self              cluster.NodeInfo   `json:"self"`
	Nodes             []cluster.NodeInfo `json:"nodes"`
	ReplicationFactor int                `json:"replication_factor"`
	VirtualNodes      int                `json:"virtual_nodes"`
	ReadPolicy        string             `json:"read_policy"`
	Error             string             `json:"error"`
}

func fetchTopology(addr, key string, timeout time.Duration) (topologyResponse, error) {
	client, err := newBenchClient(addr, key, timeout)
	if err != nil {
		return topologyResponse{}, err
	}
	defer client.close()

	data, err := client.request(`CLUSTER {"type":"CLUSTER_TOPOLOGY"}`)
	if err != nil {
		return topologyResponse{}, err
	}

	var topology topologyResponse
	if err := json.Unmarshal(data, &topology); err != nil {
		return topologyResponse{}, fmt.Errorf("invalid topology response: %w", err)
	}
	if topology.Error != "" {
		return topologyResponse{}, fmt.Errorf("topology error: %s", topology.Error)
	}
	return topology, nil
}

func (r *targetRouter) addrFor(metric string, workerID, op int) string {
	if r.mode != "owner" || r.ring == nil {
		return r.targets[workerID%len(r.targets)]
	}
	nodes := r.ring.GetN(metric, r.replicationFactor)
	if len(nodes) == 0 {
		return r.targets[workerID%len(r.targets)]
	}
	return nodes[(workerID+op)%len(nodes)].Addr
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
