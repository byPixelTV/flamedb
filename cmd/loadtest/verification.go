package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// After every worker has drained, verify each player's raw history against the
// independent input/ACK counters. Scores alone do not qualify tagged SUM reads.
func verifyPlayerSums(c *client, o options, s *counters) error {
	for m := 0; m < o.metrics; m++ {
		for entity := 0; entity < o.entities; entity++ {
			c.SetDeadline(time.Now().Add(o.timeout))
			if _, err := fmt.Fprintln(c, playerReadCommand(o, m, entity)); err != nil {
				return err
			}
			r, err := c.read()
			want := s.expectedScores[m*o.entities+entity].Load()
			if err != nil || r.Error != "" || r.Aggregate == nil || r.Aggregate.Count != want || r.Aggregate.Value != float64(want) {
				return fmt.Errorf("player SUM readback %s player%d expected=%d response=%+v error=%v", metric(o, m), entity, want, r, err)
			}
		}
	}
	return nil
}

type boardEntry struct {
	EntityID string  `json:"entity_id"`
	Value    float64 `json:"value"`
}
type verificationMetric struct {
	Metric string           `json:"metric"`
	Events int64            `json:"events"`
	Scores map[string]int64 `json:"scores,omitempty"`
}
type verificationDocument struct {
	Version int                  `json:"version"`
	Boards  bool                 `json:"boards"`
	Metrics []verificationMetric `json:"metrics"`
}

func verificationFrom(o options, s *counters) verificationDocument {
	d := verificationDocument{Version: 1, Boards: o.leaderboard || (o.mode != "write" && o.mode != "mixed")}
	for m := 0; m < o.metrics; m++ {
		v := verificationMetric{Metric: metric(o, m), Events: s.expected[m].Load()}
		if d.Boards {
			v.Scores = map[string]int64{}
			for entity := 0; entity < o.entities; entity++ {
				if n := s.expectedScores[m*o.entities+entity].Load(); n > 0 {
					v.Scores["player"+strconv.Itoa(entity)] = n
				}
			}
		}
		d.Metrics = append(d.Metrics, v)
	}
	return d
}

func checkBoardPage(metric verificationMetric, page []boardEntry, seen map[string]bool) error {
	for _, entry := range page {
		want, exists := metric.Scores[entry.EntityID]
		if !exists || seen[entry.EntityID] || entry.Value != float64(want) {
			return fmt.Errorf("score readback %s entity=%q expected=%d exists=%t duplicate=%t actual=%g", metric.Metric, entry.EntityID, want, exists, seen[entry.EntityID], entry.Value)
		}
		seen[entry.EntityID] = true
	}
	return nil
}

func verifyBoards(c *client, timeout time.Duration, d verificationDocument) error {
	if !d.Boards {
		return nil
	}
	for _, metric := range d.Metrics {
		seen := map[string]bool{}
		for offset := 0; ; offset += 1000 {
			c.SetDeadline(time.Now().Add(timeout))
			if _, err := fmt.Fprintf(c, "LEADERBOARD %s LIMIT 1000 OFFSET %d\n", metric.Metric, offset); err != nil {
				return err
			}
			r, err := c.read()
			if err != nil || r.Error != "" {
				return fmt.Errorf("board readback %s: %s %v", metric.Metric, r.Error, err)
			}
			if err = checkBoardPage(metric, r.Leaderboard, seen); err != nil {
				return err
			}
			if len(r.Leaderboard) < 1000 {
				break
			}
			if len(seen) > len(metric.Scores) {
				return fmt.Errorf("unexpected board cardinality for %s", metric.Metric)
			}
		}
		if len(seen) != len(metric.Scores) {
			return fmt.Errorf("board readback %s expected %d entities, observed %d", metric.Metric, len(metric.Scores), len(seen))
		}
	}
	return nil
}

func saveVerification(path string, d verificationDocument) error {
	data, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0600)
}

func verifySaved(o options) error {
	if o.timeout <= 0 {
		return fmt.Errorf("verification timeout must be positive")
	}
	file, err := os.Open(o.verifyFile)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.Size() > 64<<20 {
		return fmt.Errorf("invalid or oversized verification file")
	}
	var d verificationDocument
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&d); err != nil {
		return err
	}
	if d.Version != 1 || len(d.Metrics) == 0 || len(d.Metrics) > 100000 {
		return fmt.Errorf("invalid verification document")
	}
	var events int64
	names := map[string]bool{}
	for _, m := range d.Metrics {
		if m.Metric == "" || strings.ContainsAny(m.Metric, " \t\r\n\"=,") || names[m.Metric] || m.Events < 0 || len(m.Scores) > 100000 {
			return fmt.Errorf("invalid verification metric")
		}
		names[m.Metric] = true
		var sum int64
		for _, score := range m.Scores {
			if score <= 0 || score > m.Events-sum {
				return fmt.Errorf("invalid score expectations")
			}
			sum += score
		}
		if d.Boards && sum != m.Events {
			return fmt.Errorf("score/event expectation mismatch")
		}
		c, err := dial(strings.Split(o.addrs, ",")[0], o.key, o.timeout)
		if err != nil {
			return err
		}
		c.SetDeadline(time.Now().Add(o.timeout))
		_, err = fmt.Fprintln(c, "GET "+m.Metric+" SUM")
		if err != nil {
			c.Close()
			return err
		}
		r, readErr := c.read()
		if readErr != nil || r.Error != "" || r.Aggregate == nil || r.Aggregate.Count != m.Events || r.Aggregate.Value != float64(m.Events) {
			c.Close()
			return fmt.Errorf("recovery readback %s expected=%d actual=%+v error=%v", m.Metric, m.Events, r, readErr)
		}
		err = verifyBoards(c, o.timeout, verificationDocument{Boards: d.Boards, Metrics: []verificationMetric{m}})
		c.Close()
		if err != nil {
			return err
		}
		events += m.Events
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"recovery_verified": true, "readback_events": events, "metrics": len(d.Metrics), "all_entity_scores_verified": d.Boards})
}
