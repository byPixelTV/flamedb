// loadtest measures confirmed TCP responses and performs an independent readback.
// It never retries a mutation after an ambiguous transport failure.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
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

type options struct {
	addrs, key, mode, prefix, durability        string
	workers, batch, pipeline, metrics, entities int
	duration, timeout                           time.Duration
	leaderboard                                 bool
	automaticTimestamps                         bool
	fixtureEvents                               int
	readCommands, fingerprints                  []string
	verificationFile, verifyFile                string
	timestampStart                              int64
}
type response struct {
	canonical string
	Error     string `json:"error"`
	OK        bool   `json:"ok"`
	Accepted  int    `json:"accepted"`
	Failed    int    `json:"failed"`
	Errors    []struct {
		Index int `json:"index"`
	} `json:"errors"`
	Aggregate *struct {
		Value float64 `json:"value"`
		Count int64   `json:"count"`
	} `json:"aggregate"`
	Leaderboard []boardEntry `json:"leaderboard"`
}
type client struct {
	net.Conn
	reader *bufio.Reader
}

func dial(addr, key string, timeout time.Duration) (*client, error) {
	c, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	x := &client{c, bufio.NewReaderSize(c, 64<<10)}
	c.SetDeadline(time.Now().Add(timeout))
	if _, err = x.read(); err != nil {
		c.Close()
		return nil, err
	}
	fmt.Fprintln(c, "AUTH "+key)
	line, err := x.reader.ReadBytes('\n')
	if err != nil {
		c.Close()
		return nil, err
	}
	var auth struct{ Auth string }
	if json.Unmarshal(line, &auth) != nil || auth.Auth != "ok" {
		c.Close()
		return nil, fmt.Errorf("auth rejected: %s", line)
	}
	return x, nil
}
func (c *client) read() (response, error) {
	line, err := c.reader.ReadBytes('\n')
	if err != nil {
		return response{}, err
	}
	var r response
	err = json.Unmarshal(line, &r)
	if err == nil {
		var value any
		d := json.NewDecoder(bytes.NewReader(line))
		d.UseNumber()
		if err = d.Decode(&value); err == nil {
			b, _ := json.Marshal(value)
			r.canonical = string(b)
		}
	}
	return r, err
}

// The fixed logarithmic histogram has bounded memory; reported quantiles are bucket upper bounds.
type histogram struct {
	mu      sync.Mutex
	buckets [4096]uint64
	count   uint64
}

func (h *histogram) add(d time.Duration) {
	us := math.Max(float64(d)/float64(time.Microsecond), 1)
	i := int(math.Ceil(math.Log2(us) * 64))
	if i >= len(h.buckets) {
		i = len(h.buckets) - 1
	}
	h.mu.Lock()
	h.buckets[i]++
	h.count++
	h.mu.Unlock()
}
func (h *histogram) quantile(q float64) float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.count == 0 {
		return 0
	}
	want := uint64(math.Ceil(float64(h.count) * q))
	var n uint64
	for i, c := range h.buckets {
		n += c
		if n >= want {
			return math.Pow(2, float64(i)/64) / 1000
		}
	}
	return 0
}

type counters struct {
	requests, confirmedRequests, submitted, confirmed, reads, errors, ambiguous atomic.Int64
	latency                                                                     histogram
	expected                                                                    []atomic.Int64
	expectedScores                                                              []atomic.Int64
	seq                                                                         atomic.Int64
	readSeq                                                                     atomic.Int64
	readTargets                                                                 []atomic.Bool
}
type pending struct {
	start         time.Time
	metrics       []int
	entities      []int
	read          bool
	fixtureMetric int
}

func metric(o options, i int) string { return fmt.Sprintf("%s.%d", o.prefix, i) }
func frame(o options, s *counters, worker, iteration int) (string, pending) {
	p := pending{start: time.Now()}
	if o.mode == "player-sum" || (o.mode == "player-mixed" && iteration%5 == 4) {
		p.read = true
		p.fixtureMetric = int((s.readSeq.Add(1) - 1) % int64(len(o.readCommands)))
		return o.readCommands[p.fixtureMetric] + "\n", p
	}
	if o.mode != "write" && o.mode != "mixed" && o.mode != "player-mixed" {
		p.read = true
		p.fixtureMetric = iteration % o.metrics
		return o.readCommands[p.fixtureMetric] + "\n", p
	}
	if o.mode == "mixed" && iteration%5 == 4 {
		p.read = true
		return "LEADERBOARD " + metric(o, iteration%o.metrics) + " LIMIT 10\n", p
	}
	var b strings.Builder
	if o.batch > 1 {
		b.WriteString("WRITE_BATCH\n")
	}
	for i := 0; i < o.batch; i++ {
		seq := s.seq.Add(1)
		m := int(seq % int64(o.metrics))
		entity := (seq / int64(o.metrics)) % int64(o.entities)
		p.metrics = append(p.metrics, m)
		p.entities = append(p.entities, int(entity))
		fmt.Fprintf(&b, "WRITE %s 1", metric(o, m))
		if o.leaderboard {
			fmt.Fprintf(&b, " lb=\"player%d\"", entity)
		}
		fmt.Fprintf(&b, " player=\"player%d\" region=\"eu\"", entity)
		if !o.automaticTimestamps {
			fmt.Fprintf(&b, " ts=%d", seq)
		}
		b.WriteByte('\n')
	}
	if o.batch > 1 {
		b.WriteString("END\n")
	}
	return b.String(), p
}
func worker(ctx context.Context, o options, s *counters, id int, wg *sync.WaitGroup) {
	defer wg.Done()
	addrs := strings.Split(o.addrs, ",")
	c, err := dial(addrs[id%len(addrs)], o.key, o.timeout)
	if err != nil {
		s.errors.Add(1)
		return
	}
	defer c.Close()
	iteration := id
	for ctx.Err() == nil {
		queue := make([]pending, 0, o.pipeline)
		c.SetDeadline(time.Now().Add(o.timeout))
		var frames strings.Builder
		for i := 0; i < o.pipeline && ctx.Err() == nil; i++ {
			f, p := frame(o, s, id, iteration)
			iteration++
			frames.WriteString(f)
			queue = append(queue, p)
			s.requests.Add(1)
			s.submitted.Add(int64(len(p.metrics)))
		}
		if len(queue) == 0 {
			return
		}
		if _, err := fmt.Fprint(c, frames.String()); err != nil {
			s.errors.Add(1)
			for _, p := range queue {
				s.ambiguous.Add(int64(len(p.metrics)))
			}
			return
		}
		for index, p := range queue {
			r, err := c.read()
			s.latency.add(time.Since(p.start))
			if err != nil {
				s.errors.Add(1)
				for _, uncertain := range queue[index:] {
					s.ambiguous.Add(int64(len(uncertain.metrics)))
				}
				return
			}
			if r.Error != "" {
				s.errors.Add(1)
				continue
			}
			if p.read {
				if o.mode == "player-mixed" {
					if r.Aggregate == nil || r.Aggregate.Value != float64(r.Aggregate.Count) || r.Aggregate.Count < int64(o.fixtureEvents/o.entities) || r.Aggregate.Count > int64(o.fixtureEvents)+s.submitted.Load() {
						s.errors.Add(1)
						continue
					}
				} else if o.mode != "mixed" && r.canonical != o.fingerprints[p.fixtureMetric] {
					s.errors.Add(1)
					continue
				}
				s.reads.Add(1)
				if len(s.readTargets) > 0 {
					s.readTargets[p.fixtureMetric].Store(true)
				}
				s.confirmedRequests.Add(1)
				continue
			}
			if o.batch == 1 {
				// The legacy WRITE acknowledgment is exactly an empty JSON object.
				if r.canonical != "{}" {
					s.errors.Add(1)
					for _, uncertain := range queue[index:] {
						s.ambiguous.Add(int64(len(uncertain.metrics)))
					}
					return
				}
				s.confirmedRequests.Add(1)
				s.confirmed.Add(1)
				s.expected[p.metrics[0]].Add(1)
				if o.leaderboard {
					s.expectedScores[p.metrics[0]*o.entities+p.entities[0]].Add(1)
				}
				continue
			}
			if r.Accepted < 0 || r.Failed < 0 || r.Accepted+r.Failed != len(p.metrics) || len(r.Errors) != r.Failed || r.OK != (r.Failed == 0) {
				s.errors.Add(1)
				for _, uncertain := range queue[index:] {
					s.ambiguous.Add(int64(len(uncertain.metrics)))
				}
				return
			}
			failed := make(map[int]bool)
			for _, e := range r.Errors {
				if e.Index < 0 || e.Index >= len(p.metrics) || failed[e.Index] {
					s.errors.Add(1)
					for _, uncertain := range queue[index:] {
						s.ambiguous.Add(int64(len(uncertain.metrics)))
					}
					return
				}
				failed[e.Index] = true
			}
			s.confirmed.Add(int64(r.Accepted))
			if r.Failed == 0 {
				s.confirmedRequests.Add(1)
			}
			s.errors.Add(int64(r.Failed))
			for i, m := range p.metrics {
				if !failed[i] {
					s.expected[m].Add(1)
					if o.leaderboard {
						s.expectedScores[m*o.entities+p.entities[i]].Add(1)
					}
				}
			}
		}
	}
}

func readCommand(o options, m int) string {
	name := metric(o, m)
	switch o.mode {
	case "rank":
		return "RANK " + name + ` "player0"`
	case "window":
		return "LEADERBOARD " + name + " FROM 2024-01-01 TO 2024-01-02 ENTITY player LIMIT 10"
	case "sum", "avg", "count":
		return "GET " + name + " " + strings.ToUpper(o.mode)
	case "history":
		return "GET " + name + " ORDER ASC LIMIT 100"
	case "tags":
		return "GET " + name + ` WHERE region="eu" AND player="player0" ORDER ASC LIMIT 100`
	default:
		return "LEADERBOARD " + name + " LIMIT 10"
	}
}
func prepareFixture(o *options, s *counters) error {
	c, err := dial(strings.Split(o.addrs, ",")[0], o.key, o.timeout)
	if err != nil {
		return fmt.Errorf("fixture connection: %w", err)
	}
	defer c.Close()
	o.readCommands = make([]string, o.metrics)
	o.fingerprints = make([]string, o.metrics)
	for m := 0; m < o.metrics; m++ {
		for start := 0; start < o.fixtureEvents; start += 100 {
			end := min(start+100, o.fixtureEvents)
			var frame strings.Builder
			frame.WriteString("WRITE_BATCH\n")
			for i := start; i < end; i++ {
				fmt.Fprintf(&frame, "WRITE %s 1 lb=\"player%d\" player=\"player%d\" region=\"eu\" ts=%d\n", metric(*o, m), i%o.entities, i%o.entities, 1704067200000000000+int64(i))
			}
			frame.WriteString("END\n")
			c.SetDeadline(time.Now().Add(o.timeout))
			if _, err := fmt.Fprint(c, frame.String()); err != nil {
				return err
			}
			r, err := c.read()
			if err != nil || r.Error != "" || r.Accepted != end-start || r.Failed != 0 {
				return fmt.Errorf("fixture batch failed: %+v %v", r, err)
			}
			s.expected[m].Add(int64(r.Accepted))
			for i := start; i < end; i++ {
				s.expectedScores[m*o.entities+i%o.entities].Add(1)
			}
		}
		o.readCommands[m] = readCommand(*o, m)
		c.SetDeadline(time.Now().Add(o.timeout))
		if _, err := fmt.Fprintln(c, o.readCommands[m]); err != nil {
			return err
		}
		r, err := c.read()
		if err != nil || r.Error != "" {
			return fmt.Errorf("fixture read failed: %+v %v", r, err)
		}
		o.fingerprints[m] = r.canonical
	}
	if o.mode == "player-sum" || o.mode == "player-mixed" {
		// Exact expectations come from fixture input, not a sampled server result.
		o.readCommands = make([]string, o.metrics*o.entities)
		o.fingerprints = make([]string, len(o.readCommands))
		s.readTargets = make([]atomic.Bool, len(o.readCommands))
		for m := 0; m < o.metrics; m++ {
			for entity := 0; entity < o.entities; entity++ {
				index := m*o.entities + entity
				o.readCommands[index] = fmt.Sprintf("GET %s SUM WHERE player=\"player%d\" AND region=\"eu\"", metric(*o, m), entity)
				count := o.fixtureEvents / o.entities
				if entity < o.fixtureEvents%o.entities {
					count++
				}
				data, _ := json.Marshal(map[string]any{"aggregate": map[string]any{"type": "SUM", "value": count, "count": count}})
				o.fingerprints[index] = string(data)
			}
		}
	}
	return nil
}

func run(o options) error {
	if o.verifyFile != "" {
		return verifySaved(o)
	}
	if o.workers < 1 || o.workers > 10000 || o.batch < 1 || o.batch > 10000 || o.pipeline < 1 || o.pipeline > 1000 || o.metrics < 1 || o.entities < 1 || o.duration <= 0 || o.timeout <= 0 {
		return fmt.Errorf("invalid workload limits")
	}
	if o.batch*o.pipeline > 100000 || o.fixtureEvents < 1 || o.fixtureEvents > 1000000 {
		return fmt.Errorf("workload memory/fixture limit exceeded")
	}
	if o.entities > 100000 || o.metrics > 2000000/o.entities {
		return fmt.Errorf("score verification requires at most 100000 entities per metric and 2000000 metric/entity pairs")
	}
	if (o.mode == "player-sum" || o.mode == "player-mixed") && (o.metrics > 100000/o.entities || !o.leaderboard) {
		return fmt.Errorf("player workloads require leaderboard counters and at most 100000 metric/entity targets")
	}
	switch o.mode {
	case "write", "mixed", "rank", "window", "sum", "avg", "count", "history", "tags", "top", "player-sum", "player-mixed":
	default:
		return fmt.Errorf("unsupported mode")
	}
	if o.durability != "unspecified" && o.durability != "postgres-sync" {
		return fmt.Errorf("durability-contract must be unspecified or postgres-sync")
	}
	if strings.ContainsAny(o.prefix, " \r\n\"=,") || o.prefix == "" {
		return fmt.Errorf("invalid metric prefix")
	}
	s := &counters{expected: make([]atomic.Int64, o.metrics), expectedScores: make([]atomic.Int64, o.metrics*o.entities)}
	if o.timestampStart < 0 {
		return fmt.Errorf("timestamp start must be nonnegative")
	}
	startTimestamp := o.timestampStart
	if startTimestamp == 0 {
		startTimestamp = time.Now().UnixNano()
	}
	s.seq.Store(startTimestamp)
	if o.mode != "write" && o.mode != "mixed" {
		if err := prepareFixture(&o, s); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), o.duration)
	defer cancel()
	started := time.Now()
	var wg sync.WaitGroup
	for id := 0; id < o.workers; id++ {
		wg.Add(1)
		go worker(ctx, o, s, id, &wg)
	}
	wg.Wait()
	elapsed := time.Since(started).Seconds()
	c, err := dial(strings.Split(o.addrs, ",")[0], o.key, o.timeout)
	if err != nil {
		return fmt.Errorf("readback connect: %w", err)
	}
	defer c.Close()
	var observed int64
	var mismatches []string
	for m := 0; m < o.metrics; m++ {
		c.SetDeadline(time.Now().Add(o.timeout))
		fmt.Fprintln(c, "GET "+metric(o, m)+" SUM")
		r, err := c.read()
		if err != nil || r.Error != "" || r.Aggregate == nil {
			return fmt.Errorf("readback %s: response=%+v error=%v", metric(o, m), r, err)
		}
		observed += r.Aggregate.Count
		expected := s.expected[m].Load()
		if r.Aggregate.Count != expected || r.Aggregate.Value != float64(expected) {
			mismatches = append(mismatches, fmt.Sprintf("%s expected=%d count=%d sum=%g", metric(o, m), expected, r.Aggregate.Count, r.Aggregate.Value))
		}
	}
	document := verificationFrom(o, s)
	if err = verifyBoards(c, o.timeout, document); err != nil {
		mismatches = append(mismatches, err.Error())
	}
	playerSumsVerified := false
	if o.mode == "player-sum" || o.mode == "player-mixed" {
		if err = verifyPlayerSums(c, o, s); err != nil {
			mismatches = append(mismatches, err.Error())
		} else {
			playerSumsVerified = true
		}
	}
	sort.Strings(mismatches)
	durable := int64(0)
	if o.durability == "postgres-sync" {
		durable = s.confirmed.Load()
	}
	report := map[string]any{"metric_prefix": o.prefix, "elapsed_seconds_including_drain": elapsed, "workers": o.workers, "batch_size": o.batch, "pipeline": o.pipeline, "metrics": o.metrics, "entities": o.entities, "mode": o.mode, "requests_submitted": s.requests.Load(), "events_submitted": s.submitted.Load(), "events_confirmed": s.confirmed.Load(), "reads_confirmed": s.reads.Load(), "errors": s.errors.Load(), "events_ambiguous": s.ambiguous.Load(), "confirmed_events_per_second": float64(s.confirmed.Load()) / elapsed, "confirmed_reads_per_second": float64(s.reads.Load()) / elapsed, "latency_ms_bucket_upper_bound": map[string]float64{"p50": s.latency.quantile(.5), "p95": s.latency.quantile(.95), "p99": s.latency.quantile(.99)}, "events_durable_by_configured_contract": durable, "durability_contract": o.durability, "readback_events": observed, "readback_mismatches": mismatches, "correctness_pass": len(mismatches) == 0 && s.errors.Load() == 0 && s.ambiguous.Load() == 0, "readback_proves_restart_survival": false}
	enc := json.NewEncoder(os.Stdout)
	report["requests_confirmed"] = s.confirmedRequests.Load()
	report["confirmed_requests_per_second"] = float64(s.confirmedRequests.Load()) / elapsed
	report["leaderboard_on_write"] = o.leaderboard
	report["automatic_timestamps"] = o.automaticTimestamps
	report["timestamp_start"] = startTimestamp
	report["all_entity_scores_verified"] = document.Boards
	report["fixture_events_per_metric"] = 0
	if o.mode != "write" && o.mode != "mixed" {
		report["fixture_events_per_metric"] = o.fixtureEvents
		report["stable_read_responses_verified"] = o.mode != "player-mixed"
	}
	if len(s.readTargets) > 0 {
		visited := 0
		for i := range s.readTargets {
			if s.readTargets[i].Load() {
				visited++
			}
		}
		report["player_query_targets"] = len(s.readTargets)
		report["player_query_targets_observed"] = visited
		report["all_player_sums_verified"] = playerSumsVerified
	}
	enc.SetIndent("", "  ")
	if err := enc.Encode(report); err != nil {
		return err
	}
	if len(mismatches) > 0 || s.errors.Load() > 0 || s.ambiguous.Load() > 0 {
		return fmt.Errorf("workload or independent readback failed")
	}
	if o.verificationFile != "" {
		return saveVerification(o.verificationFile, document)
	}
	return nil
}
func main() {
	var o options
	flag.StringVar(&o.addrs, "addrs", "127.0.0.1:7777", "comma separated API addresses (round robin per client)")
	flag.StringVar(&o.key, "key", os.Getenv("FLAMEDB_API_KEY"), "API key; defaults to FLAMEDB_API_KEY")
	flag.StringVar(&o.mode, "mode", "write", "write, mixed (80/20 writes/top), player-sum, player-mixed (80/20 writes/player SUM), rank, window, sum, avg, count, history, tags, top")
	flag.BoolVar(&o.leaderboard, "leaderboard", true, "update all-time leaderboard on measured writes; false exercises plain event batching")
	flag.BoolVar(&o.automaticTimestamps, "automatic-timestamps", false, "omit explicit ts on measured writes, exercising the shared SDK-default allocator")
	flag.IntVar(&o.fixtureEvents, "fixture-events", 1000, "events per metric seeded outside timed read-only runs")
	flag.Int64Var(&o.timestampStart, "start-timestamp", 0, "initial explicit timestamp sequence; zero uses current time")
	flag.StringVar(&o.prefix, "prefix", fmt.Sprintf("load%d", time.Now().UnixNano()), "unique metric prefix; reuse causes readback failure")
	flag.StringVar(&o.durability, "durability-contract", "unspecified", "unspecified or postgres-sync; the latter requires synchronous PostgreSQL commits")
	flag.IntVar(&o.workers, "workers", 32, "concurrent connections")
	flag.IntVar(&o.batch, "batch-size", 1, "events in one WRITE_BATCH response")
	flag.IntVar(&o.pipeline, "pipeline", 1, "requests sent before reading responses")
	flag.IntVar(&o.metrics, "metrics", 64, "number of metrics")
	flag.IntVar(&o.entities, "entities", 10000, "leaderboard entities per metric")
	flag.DurationVar(&o.duration, "duration", 15*time.Second, "submission duration")
	flag.DurationVar(&o.timeout, "timeout", 30*time.Second, "TCP operation timeout")
	flag.StringVar(&o.verificationFile, "verification-file", "", "save independent count/sum/entity-score expectations for later restart verification")
	flag.StringVar(&o.verifyFile, "verify-file", "", "verify a saved workload after recovery, without writing or replaying events")
	flag.Parse()
	if err := run(o); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
