package flamedb

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"
)

// RankingOpts reuses all-time leaderboard options and adds a half-open event
// window. EntityTag is required with From or To; Board and windows cannot mix.
// Limit and Offset do not change an entity's global rank or the board's count.
type RankingOpts struct {
	LeaderboardOpts
	From      time.Time
	To        time.Time
	EntityTag string
}

// RankedEntry retains the global, one-based ordinal rank. Equal scores retain
// the selected leaderboard's order, so ties have distinct ranks.
type RankedEntry struct {
	EntityID string  `json:"entity_id"`
	Score    float64 `json:"value"`
	Rank     int64   `json:"rank"`
}

type rankingResult struct {
	Rank   *int64        `json:"rank"`
	Score  *float64      `json:"score"`
	Count  int64         `json:"count"`
	Around []RankedEntry `json:"around"`
}

// GetRank returns the one-based rank, or nil if the entity is absent.
func (c *Client) GetRank(ctx context.Context, metric, entity string, opts RankingOpts) (*int64, error) {
	r, err := c.ranking(ctx, "RANK", metric, entity, 0, opts)
	if err != nil {
		return nil, err
	}
	return r.Rank, nil
}

// GetScore returns the current score, or nil if the entity is absent. A present
// entity with score zero returns a non-nil pointer to zero.
func (c *Client) GetScore(ctx context.Context, metric, entity string, opts RankingOpts) (*float64, error) {
	r, err := c.ranking(ctx, "SCORE", metric, entity, 0, opts)
	if err != nil {
		return nil, err
	}
	return r.Score, nil
}

// GetAroundEntity returns the entity and up to radius neighbors on each side,
// with global ranks. An absent entity returns an empty slice.
func (c *Client) GetAroundEntity(ctx context.Context, metric, entity string, radius int, opts RankingOpts) ([]RankedEntry, error) {
	if radius < 0 || radius > 100000 {
		return nil, fmt.Errorf("flamedb: invalid ranking radius")
	}
	r, err := c.ranking(ctx, "AROUND", metric, entity, radius, opts)
	if err != nil {
		return nil, err
	}
	return r.Around, nil
}

// GetLeaderboardCount returns the number of entities in the selected board or
// event window, including entities whose score is zero.
func (c *Client) GetLeaderboardCount(ctx context.Context, metric string, opts RankingOpts) (int64, error) {
	r, err := c.ranking(ctx, "LEADERBOARD_COUNT", metric, "", 0, opts)
	if err != nil {
		return 0, err
	}
	return r.Count, nil
}

func (c *Client) ranking(ctx context.Context, command, metric, entity string, radius int, opts RankingOpts) (*rankingResult, error) {
	if err := validateIdentifiers([]string{metric}, nil); err != nil {
		return nil, err
	}
	windowed := !opts.From.IsZero() || !opts.To.IsZero()
	if opts.Board != "" {
		if err := validateBoards(map[string]string{opts.Board: "entity"}); err != nil {
			return nil, err
		}
		if windowed {
			return nil, fmt.Errorf("flamedb: BOARD cannot be combined with FROM or TO")
		}
	}
	if opts.EntityTag != "" {
		if err := validateIdentifiers([]string{opts.EntityTag}, nil); err != nil {
			return nil, err
		}
	}
	if windowed && opts.EntityTag == "" {
		return nil, fmt.Errorf("flamedb: EntityTag required with From or To")
	}
	if !opts.From.IsZero() && opts.From.UnixNano() < 0 || !opts.To.IsZero() && opts.To.UnixNano() < 0 || !opts.From.IsZero() && !opts.To.IsZero() && opts.From.After(opts.To) {
		return nil, fmt.Errorf("flamedb: invalid ranking time range")
	}
	if opts.Limit < 0 || opts.Limit > 1000000 || opts.Offset < 0 {
		return nil, fmt.Errorf("flamedb: invalid ranking pagination")
	}
	parts := []string{command, metric}
	if command != "LEADERBOARD_COUNT" {
		if entity == "" {
			return nil, fmt.Errorf("flamedb: ranking entity required")
		}
		quoted, _ := json.Marshal(entity)
		parts = append(parts, string(quoted))
	}
	if command == "AROUND" {
		parts = append(parts, fmt.Sprint(radius))
	}
	if opts.Board != "" {
		parts = append(parts, "BOARD", opts.Board)
	}
	if !opts.From.IsZero() {
		parts = append(parts, "FROM", opts.From.UTC().Format(time.RFC3339Nano))
	}
	if !opts.To.IsZero() {
		parts = append(parts, "TO", opts.To.UTC().Format(time.RFC3339Nano))
	}
	if windowed {
		parts = append(parts, "ENTITY", opts.EntityTag)
	}
	if opts.Limit > 0 {
		parts = append(parts, "LIMIT", fmt.Sprint(opts.Limit))
	}
	if opts.Offset > 0 {
		parts = append(parts, "OFFSET", fmt.Sprint(opts.Offset))
	}
	response, err := c.command(ctx, strings.Join(parts, " "))
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(response["ranking"], &fields); err != nil || fields == nil {
		return nil, fmt.Errorf("flamedb: missing or invalid ranking result (updated server required)")
	}
	for _, key := range []string{"rank", "score", "count"} {
		if _, ok := fields[key]; !ok {
			return nil, fmt.Errorf("flamedb: missing ranking field %s", key)
		}
	}
	if string(fields["count"]) == "null" {
		return nil, fmt.Errorf("flamedb: invalid ranking count")
	}
	var r rankingResult
	if err := json.Unmarshal(response["ranking"], &r); err != nil {
		return nil, fmt.Errorf("flamedb: invalid ranking result: %w", err)
	}
	if r.Count < 0 || (r.Rank == nil) != (r.Score == nil) || r.Rank != nil && (*r.Rank < 1 || *r.Rank > r.Count) || r.Score != nil && (math.IsNaN(*r.Score) || math.IsInf(*r.Score, 0)) {
		return nil, fmt.Errorf("flamedb: inconsistent ranking result")
	}
	if command == "AROUND" {
		if _, ok := fields["around"]; !ok || string(fields["around"]) == "null" {
			return nil, fmt.Errorf("flamedb: missing ranking around entries")
		}
		var entries []map[string]json.RawMessage
		if err := json.Unmarshal(fields["around"], &entries); err != nil {
			return nil, fmt.Errorf("flamedb: invalid ranking around entries: %w", err)
		}
		for _, entry := range entries {
			for _, key := range []string{"entity_id", "value", "rank"} {
				if value, ok := entry[key]; !ok || string(value) == "null" {
					return nil, fmt.Errorf("flamedb: missing ranking around entry field %s", key)
				}
			}
		}
	}
	for _, entry := range r.Around {
		if entry.EntityID == "" || entry.Rank < 1 || entry.Rank > r.Count || math.IsNaN(entry.Score) || math.IsInf(entry.Score, 0) {
			return nil, fmt.Errorf("flamedb: invalid ranking around entry")
		}
	}
	return &r, nil
}
