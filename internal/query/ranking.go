package query

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/byPixelTV/flamedb/internal/aggregates"
	"github.com/byPixelTV/flamedb/internal/types"
)

func isRankingCommand(cmd string) bool {
	return cmd == "RANK" || cmd == "SCORE" || cmd == "AROUND" || cmd == "LEADERBOARD_COUNT"
}

func (e *Executor) executeRanking(q *Query) (*Result, error) {
	out := &RankingResult{}
	around := q.Type == QueryTypeAround
	if around {
		out.Around = []RankedEntry{}
	}
	var previous []RankedEntry
	oldest := 0
	visit := func(entry types.LeaderboardEntry) bool {
		out.Count++
		if q.Type == QueryTypeLeaderboardCount {
			return true
		}
		row := RankedEntry{EntityID: entry.EntityID, Value: entry.Value, Rank: out.Count}
		if out.Rank != nil {
			if around && out.Count-*out.Rank <= int64(q.Radius) {
				out.Around = append(out.Around, row)
			}
			return true
		}
		if entry.EntityID == q.EntityID {
			position, score := row.Rank, row.Value
			out.Rank, out.Score = &position, &score
			if around {
				out.Around = append(out.Around, previous[oldest:]...)
				out.Around = append(out.Around, previous[:oldest]...)
				out.Around = append(out.Around, row)
			}
		} else if around && q.Radius > 0 {
			if len(previous) < q.Radius {
				previous = append(previous, row)
			} else {
				previous[oldest] = row
				oldest = (oldest + 1) % len(previous)
			}
		}
		return true
	}
	if q.From != 0 || q.To != 0 {
		window := *q
		window.Limit, window.Offset = 0, 0
		result, err := e.executeLeaderboard(&window)
		if err != nil {
			return nil, err
		}
		for _, entry := range result.Leaderboard {
			visit(entry)
		}
	} else if err := e.lb.VisitRanked(aggregates.BoardMetric(q.Metric, q.Board), visit); err != nil {
		return nil, err
	}
	return &Result{Ranking: out}, nil
}
func parseRanking(tokens []string) (*Query, error) {
	cmd := strings.ToUpper(tokens[0])
	tail := 2
	if len(tokens) < 2 {
		return nil, fmt.Errorf("%s requires metric", cmd)
	}
	entity := ""
	radius := 0
	if cmd != "LEADERBOARD_COUNT" {
		if len(tokens) < 3 || tokenValue(tokens[2]) == "" {
			return nil, fmt.Errorf("%s requires entity", cmd)
		}
		entity = tokenValue(tokens[2])
		tail = 3
	}
	if cmd == "AROUND" {
		if len(tokens) < 4 {
			return nil, fmt.Errorf("AROUND requires radius")
		}
		var err error
		radius, err = strconv.Atoi(tokens[3])
		if err != nil || radius < 0 || radius > 100000 {
			return nil, fmt.Errorf("invalid radius")
		}
		tail = 4
	}
	q, err := Parse("LEADERBOARD " + tokens[1] + " " + strings.Join(tokens[tail:], " "))
	if err != nil {
		return nil, err
	}
	q.Type = QueryType(cmd)
	q.EntityID = entity
	q.Radius = radius
	return q, nil
}
