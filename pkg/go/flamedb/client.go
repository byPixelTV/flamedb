// Package flamedb provides a Go client for FlameDB.
//
// Usage:
//
//	db, err := flamedb.New(flamedb.Config{
//	    Host:   "127.0.0.1",
//	    Port:   7777,
//	    APIKey: "flame_abc123",
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer db.Close()
//
//	if err := db.Write(ctx, "kills", 1, flamedb.WriteOpts{
//	    LeaderboardEntity: "pixel",
//	    Tags: map[string]string{"player": "pixel", "region": "eu"},
//	}); err != nil {
//	    log.Fatal(err)
//	}
package flamedb

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

// ─── Config ──────────────────────────────────────────────────────────────────

// Config holds connection settings for a FlameDB client.
type Config struct {
	Host string
	Port int
	// APIKey is the FlameDB API key used for authentication.
	APIKey string
	// Timeout for individual commands. Defaults to 5 seconds.
	Timeout time.Duration
}

func (c *Config) addr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

func (c *Config) timeout() time.Duration {
	if c.Timeout == 0 {
		return 5 * time.Second
	}
	return c.Timeout
}

// ─── Types ────────────────────────────────────────────────────────────────────

// Event is a single time-series event returned by GET.
type Event struct {
	Timestamp int64             `json:"timestamp"`
	Metric    string            `json:"metric"`
	Value     float64           `json:"value"`
	Tags      map[string]string `json:"tags,omitempty"`
}

// LeaderboardEntry is a ranked entry returned by LEADERBOARD.
type LeaderboardEntry struct {
	EntityID string  `json:"entity_id"`
	Score    float64 `json:"score"`
}

// SeriesPoint is a time-bucketed aggregate returned by GROUP BY queries.
type SeriesPoint struct {
	TS    int64   `json:"ts"`
	Value float64 `json:"value"`
	Count int     `json:"count"`
}

// AggregateResult is the result of COUNT/SUM/AVG aggregations.
type AggregateResult struct {
	Type  string  `json:"type"`
	Value float64 `json:"value"`
	Count int     `json:"count"`
}

// StatsResult holds cardinality info for tag keys on a metric.
type StatsResult struct {
	Metric   string     `json:"metric"`
	TagStats []TagStats `json:"tag_stats"`
}

// TagStats holds the cardinality for a single tag key.
type TagStats struct {
	TagKey      string `json:"tag_key"`
	Cardinality int    `json:"cardinality"`
}

// GetResult is returned by Get queries.
type GetResult struct {
	Events        []Event                     `json:"events,omitempty"`
	Metrics       map[string][]Event          `json:"metrics,omitempty"`
	Aggregate     *AggregateResult            `json:"aggregate,omitempty"`
	Aggregates    map[string]*AggregateResult `json:"aggregates,omitempty"`
	Series        []SeriesPoint               `json:"series,omitempty"`
	SeriesByMetric map[string][]SeriesPoint   `json:"series_by_metric,omitempty"`
}

// BatchResult is returned by WriteBatch.
type BatchResult struct {
	OK       bool              `json:"ok"`
	Accepted int               `json:"accepted"`
	Failed   int               `json:"failed"`
	Errors   []BatchItemError  `json:"errors,omitempty"`
}

// BatchItemError describes a failed item in a batch.
type BatchItemError struct {
	Index int    `json:"index"`
	Error string `json:"error"`
}

// WriteOpts configures a WRITE command.
type WriteOpts struct {
	// LeaderboardEntity sets the lb= tag; enables leaderboard increment.
	LeaderboardEntity string
	Tags              map[string]string
	// TimestampNs overrides the server-side timestamp (unix nanoseconds).
	TimestampNs int64
	// Quorum waits for a majority of replicas to confirm before returning.
	Quorum bool
}

// GetOpts configures a GET command.
type GetOpts struct {
	Where  map[string]string
	From   time.Time
	To     time.Time
	Limit  int
	Offset int
	Order  string // "ASC" or "DESC"
}

// LeaderboardOpts paginates LEADERBOARD results.
type LeaderboardOpts struct {
	Limit  int
	Offset int
}

// GroupDef defines a group for GROUP_LEADERBOARD.
type GroupDef struct {
	Name    string
	Members []string
}

// WriteBatchItem is one entry in a batch write.
type WriteBatchItem struct {
	Metric string
	Value  float64
	Opts   WriteOpts
}

// ─── Client ───────────────────────────────────────────────────────────────────

// Client is a thread-safe FlameDB client. It uses a single persistent
// TCP connection; concurrent callers are serialized by an internal mutex.
type Client struct {
	cfg     Config
	mu      sync.Mutex
	conn    net.Conn
	scanner *bufio.Scanner
	writer  *bufio.Writer
	authed  bool
}

// New creates a new Client and opens + authenticates a TCP connection.
func New(cfg Config) (*Client, error) {
	c := &Client{cfg: cfg}
	if err := c.dial(); err != nil {
		return nil, err
	}
	return c, nil
}

// Close closes the underlying TCP connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		return err
	}
	return nil
}

// ─── Internal ─────────────────────────────────────────────────────────────────

func (c *Client) dial() error {
	conn, err := net.DialTimeout("tcp", c.cfg.addr(), c.cfg.timeout())
	if err != nil {
		return fmt.Errorf("flamedb: connect: %w", err)
	}
	c.conn = conn
	c.scanner = bufio.NewScanner(conn)
	c.scanner.Buffer(make([]byte, 0, 64*1024), 4<<20)
	c.writer = bufio.NewWriter(conn)
	c.authed = false
	return c.auth()
}

func (c *Client) auth() error {
	// read {"auth":"required"}
	line, err := c.readLine()
	if err != nil {
		return err
	}
	var challenge map[string]string
	if err := json.Unmarshal([]byte(line), &challenge); err != nil || challenge["auth"] != "required" {
		return fmt.Errorf("flamedb: unexpected auth challenge: %s", line)
	}

	if err := c.sendLine("AUTH " + c.cfg.APIKey); err != nil {
		return err
	}

	resp, err := c.readLine()
	if err != nil {
		return err
	}
	var authResp map[string]string
	if err := json.Unmarshal([]byte(resp), &authResp); err != nil {
		return fmt.Errorf("flamedb: auth parse error: %w", err)
	}
	if authResp["auth"] != "ok" {
		return fmt.Errorf("flamedb: auth failed: %s", authResp["error"])
	}
	c.authed = true
	return nil
}

func (c *Client) readLine() (string, error) {
	_ = c.conn.SetReadDeadline(time.Now().Add(c.cfg.timeout()))
	if !c.scanner.Scan() {
		if err := c.scanner.Err(); err != nil {
			return "", fmt.Errorf("flamedb: read: %w", err)
		}
		return "", fmt.Errorf("flamedb: connection closed")
	}
	return c.scanner.Text(), nil
}

func (c *Client) sendLine(line string) error {
	_ = c.conn.SetWriteDeadline(time.Now().Add(c.cfg.timeout()))
	if _, err := c.writer.WriteString(line + "\n"); err != nil {
		return fmt.Errorf("flamedb: write: %w", err)
	}
	return c.writer.Flush()
}

func (c *Client) sendLines(lines []string) error {
	_ = c.conn.SetWriteDeadline(time.Now().Add(c.cfg.timeout()))
	for _, l := range lines {
		if _, err := c.writer.WriteString(l + "\n"); err != nil {
			return fmt.Errorf("flamedb: write: %w", err)
		}
	}
	return c.writer.Flush()
}

// command sends one line and parses the JSON response.
// Caller must hold c.mu.
func (c *Client) command(ctx context.Context, line string) (map[string]json.RawMessage, error) {
	return c.commandMulti(ctx, []string{line})
}

func (c *Client) commandMulti(ctx context.Context, lines []string) (map[string]json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.sendLines(lines); err != nil {
		return nil, err
	}
	raw, err := c.readLine()
	if err != nil {
		return nil, err
	}
	var result map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		return nil, fmt.Errorf("flamedb: parse response: %w", err)
	}
	if errMsg, ok := result["error"]; ok {
		var s string
		_ = json.Unmarshal(errMsg, &s)
		return nil, fmt.Errorf("flamedb: %s", s)
	}
	return result, nil
}

// ─── Write ────────────────────────────────────────────────────────────────────

// Write sends a WRITE command.
func (c *Client) Write(ctx context.Context, metric string, value float64, opts WriteOpts) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.command(ctx, buildWrite(metric, value, opts))
	return err
}

func buildWrite(metric string, value float64, opts WriteOpts) string {
	parts := []string{fmt.Sprintf("WRITE %s %g", metric, value)}
	if opts.LeaderboardEntity != "" {
		parts = append(parts, fmt.Sprintf(`lb="%s"`, opts.LeaderboardEntity))
	}
	for k, v := range opts.Tags {
		parts = append(parts, fmt.Sprintf(`%s="%s"`, k, v))
	}
	if opts.TimestampNs != 0 {
		parts = append(parts, fmt.Sprintf("ts=%d", opts.TimestampNs))
	}
	if opts.Quorum {
		parts = append(parts, "QUORUM")
	}
	return strings.Join(parts, " ")
}

// WriteBatch sends a WRITE_BATCH command with multiple items.
func (c *Client) WriteBatch(ctx context.Context, items []WriteBatchItem) (*BatchResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	lines := []string{"WRITE_BATCH"}
	for _, item := range items {
		lines = append(lines, buildWrite(item.Metric, item.Value, item.Opts))
	}
	lines = append(lines, "END")

	result, err := c.commandMulti(ctx, lines)
	if err != nil {
		return nil, err
	}

	var br BatchResult
	raw, _ := json.Marshal(result)
	if err := json.Unmarshal(raw, &br); err != nil {
		return nil, fmt.Errorf("flamedb: parse batch result: %w", err)
	}
	return &br, nil
}

// ─── Set ──────────────────────────────────────────────────────────────────────

// Set sends a SET command (absolute leaderboard value).
func (c *Client) Set(ctx context.Context, metric string, value float64, leaderboardEntity string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cmd := fmt.Sprintf("SET %s %g", metric, value)
	if leaderboardEntity != "" {
		cmd += fmt.Sprintf(` lb="%s"`, leaderboardEntity)
	}
	_, err := c.command(ctx, cmd)
	return err
}

// ─── Delete ───────────────────────────────────────────────────────────────────

// DeleteOpts configures a DELETE command.
type DeleteOpts struct {
	LeaderboardEntity string
	From              time.Time
	To                time.Time
}

// Delete sends a DELETE command.
func (c *Client) Delete(ctx context.Context, metric string, opts DeleteOpts) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	parts := []string{"DELETE " + metric}
	if opts.LeaderboardEntity != "" {
		parts = append(parts, fmt.Sprintf(`lb="%s"`, opts.LeaderboardEntity))
	}
	if !opts.From.IsZero() {
		parts = append(parts, "FROM "+fmtDate(opts.From))
	}
	if !opts.To.IsZero() {
		parts = append(parts, "TO "+fmtDate(opts.To))
	}
	_, err := c.command(ctx, strings.Join(parts, " "))
	return err
}

// ─── Get ──────────────────────────────────────────────────────────────────────

// Get sends a GET command and returns the result.
func (c *Client) Get(ctx context.Context, metrics []string, opts GetOpts) (*GetResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	parts := []string{"GET " + strings.Join(metrics, ",")}

	if len(opts.Where) > 0 {
		var clauses []string
		for k, v := range opts.Where {
			clauses = append(clauses, fmt.Sprintf(`%s="%s"`, k, v))
		}
		parts = append(parts, "WHERE "+strings.Join(clauses, " AND "))
	}
	if !opts.From.IsZero() {
		parts = append(parts, "FROM "+fmtDate(opts.From))
	}
	if !opts.To.IsZero() {
		parts = append(parts, "TO "+fmtDate(opts.To))
	}
	if opts.Limit > 0 {
		parts = append(parts, fmt.Sprintf("LIMIT %d", opts.Limit))
	}
	if opts.Offset > 0 {
		parts = append(parts, fmt.Sprintf("OFFSET %d", opts.Offset))
	}
	if opts.Order != "" {
		parts = append(parts, "ORDER "+opts.Order)
	}

	result, err := c.command(ctx, strings.Join(parts, " "))
	if err != nil {
		return nil, err
	}

	var gr GetResult
	raw, _ := json.Marshal(result)
	if err := json.Unmarshal(raw, &gr); err != nil {
		return nil, fmt.Errorf("flamedb: parse get result: %w", err)
	}
	return &gr, nil
}

// ─── Leaderboard ──────────────────────────────────────────────────────────────

// Leaderboard sends a LEADERBOARD command.
func (c *Client) Leaderboard(ctx context.Context, metric string, opts LeaderboardOpts) ([]LeaderboardEntry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	parts := []string{"LEADERBOARD " + metric}
	if opts.Limit > 0 {
		parts = append(parts, fmt.Sprintf("LIMIT %d", opts.Limit))
	}
	if opts.Offset > 0 {
		parts = append(parts, fmt.Sprintf("OFFSET %d", opts.Offset))
	}

	result, err := c.command(ctx, strings.Join(parts, " "))
	if err != nil {
		return nil, err
	}

	var resp struct {
		Leaderboard []LeaderboardEntry `json:"leaderboard"`
	}
	raw, _ := json.Marshal(result)
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("flamedb: parse leaderboard: %w", err)
	}
	return resp.Leaderboard, nil
}

// ─── Group Leaderboard ───────────────────────────────────────────────────────

// GroupLeaderboardEntry is one entry returned by GROUP_LEADERBOARD.
type GroupLeaderboardEntry struct {
	Group string  `json:"group"`
	Score float64 `json:"score"`
}

// GroupLeaderboard sends a GROUP_LEADERBOARD command.
func (c *Client) GroupLeaderboard(ctx context.Context, metric string, groups []GroupDef, opts LeaderboardOpts) ([]GroupLeaderboardEntry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	parts := []string{"GROUP_LEADERBOARD " + metric}
	for _, g := range groups {
		parts = append(parts, fmt.Sprintf(`GROUP "%s:%s"`, g.Name, strings.Join(g.Members, ",")))
	}
	if opts.Limit > 0 {
		parts = append(parts, fmt.Sprintf("LIMIT %d", opts.Limit))
	}
	if opts.Offset > 0 {
		parts = append(parts, fmt.Sprintf("OFFSET %d", opts.Offset))
	}

	result, err := c.command(ctx, strings.Join(parts, " "))
	if err != nil {
		return nil, err
	}

	var resp struct {
		Leaderboard []GroupLeaderboardEntry `json:"leaderboard"`
	}
	raw, _ := json.Marshal(result)
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("flamedb: parse group leaderboard: %w", err)
	}
	return resp.Leaderboard, nil
}

// ─── Stats ───────────────────────────────────────────────────────────────────

// Stats sends a STATS command.
func (c *Client) Stats(ctx context.Context, metric string, tags []string) (*StatsResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cmd := fmt.Sprintf("STATS %s TAGS %s", metric, strings.Join(tags, " "))
	result, err := c.command(ctx, cmd)
	if err != nil {
		return nil, err
	}

	var sr StatsResult
	raw, _ := json.Marshal(result)
	if err := json.Unmarshal(raw, &sr); err != nil {
		return nil, fmt.Errorf("flamedb: parse stats: %w", err)
	}
	return &sr, nil
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func fmtDate(t time.Time) string {
	return t.UTC().Format("2006-01-02")
}
