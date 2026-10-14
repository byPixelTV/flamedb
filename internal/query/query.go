package query

import (
	"encoding/json"
	"time"

	"github.com/byPixelTV/flamedb/internal/storage"
	"github.com/byPixelTV/flamedb/internal/types"
)

type QueryType string

type GroupDef struct {
	Name    string
	Members []string
}

type AggType string

const (
	AggCount AggType = "COUNT"
	AggSum   AggType = "SUM"
	AggAvg   AggType = "AVG"
)

const (
	QueryTypeMetrics          QueryType = "METRICS"
	QueryTypeGet              QueryType = "GET"
	QueryTypeLeaderboard      QueryType = "LEADERBOARD"
	QueryTypeWrite            QueryType = "WRITE"
	QueryTypeSet              QueryType = "SET"
	QueryTypeDelete           QueryType = "DELETE"
	QueryTypeStats            QueryType = "STATS"
	QueryTypeGroupLeaderboard QueryType = "GROUP_LEADERBOARD"
)

type Query struct {
	EntityID    string
	Radius      int
	DryRun      bool
	OperationID string
	Type        QueryType
	Metric      string
	Metrics     []string
	Where       map[string]string
	Timestamp   int64
	From        int64
	To          int64
	Limit       int
	Offset      int
	Order       string
	// write specific
	Value      float64
	Tags       map[string]string
	UpdateLB   bool
	LBEntityID string
	// LBEntities updates named leaderboards in the same metric mutation.
	LBEntities  map[string]string
	Board       string
	TagKeys     []string
	Quorum      bool
	IsReplica   bool
	ForceLocal  bool
	Groups      []GroupDef
	Aggregate   AggType
	GroupBy     time.Duration
	GroupBySpec string

	// EntityTag is the tag key identifying entities in windowed leaderboards.
	// Required when FROM or TO is set.
	// Beispiel: LEADERBOARD kills FROM now-7d TO now ENTITY player
	EntityTag string
}

type DeleteResult struct {
	DryRun             bool  `json:"dry_run"`
	Events             int64 `json:"events"`
	LeaderboardEntries int64 `json:"leaderboard_entries"`
}

type Result struct {
	Ranking        *RankingResult              `json:"ranking,omitempty"`
	Delete         *DeleteResult               `json:"delete,omitempty"`
	MetricKeys     []string                    `json:"metric_keys,omitempty"`
	Events         []storage.Event             `json:"events,omitempty"`
	Metrics        map[string][]storage.Event  `json:"metrics,omitempty"`
	Leaderboard    []types.LeaderboardEntry    `json:"leaderboard,omitempty"`
	Stats          *StatsResult                `json:"stats,omitempty"`
	Aggregate      *AggregateResult            `json:"aggregate,omitempty"`
	Aggregates     map[string]*AggregateResult `json:"aggregates,omitempty"`
	Series         []SeriesPoint               `json:"series,omitempty"`
	SeriesByMetric map[string][]SeriesPoint    `json:"series_by_metric,omitempty"`
}

type RankedEntry struct {
	EntityID string  `json:"entity_id"`
	Value    float64 `json:"value"`
	Rank     int64   `json:"rank"`
}
type RankingResult struct {
	Rank   *int64        `json:"rank"`
	Score  *float64      `json:"score"`
	Count  int64         `json:"count"`
	Around []RankedEntry `json:"around,omitempty"`
}

// MarshalJSON distinguishes an absent around request from its valid empty page.
func (r RankingResult) MarshalJSON() ([]byte, error) {
	type fields struct {
		Rank   *int64         `json:"rank"`
		Score  *float64       `json:"score"`
		Count  int64          `json:"count"`
		Around *[]RankedEntry `json:"around,omitempty"`
	}
	out := fields{Rank: r.Rank, Score: r.Score, Count: r.Count}
	if r.Around != nil {
		out.Around = &r.Around
	}
	return json.Marshal(out)
}

const (
	QueryTypeRank             QueryType = "RANK"
	QueryTypeScore            QueryType = "SCORE"
	QueryTypeAround           QueryType = "AROUND"
	QueryTypeLeaderboardCount QueryType = "LEADERBOARD_COUNT"
)

type BatchItemError struct {
	Index int    `json:"index"`
	Error string `json:"error"`
}

type BatchResult struct {
	OK       bool             `json:"ok"`
	Accepted int              `json:"accepted"`
	Failed   int              `json:"failed"`
	Errors   []BatchItemError `json:"errors,omitempty"`
}

type SeriesPoint struct {
	TS    int64   `json:"ts"`
	Value float64 `json:"value"`
	Count int     `json:"count"`
}

type AggregateResult struct {
	Type  string  `json:"type"`
	Value float64 `json:"value"`
	Count int     `json:"count"`
}

type StatsResult struct {
	Metric   string             `json:"metric"`
	TagStats []storage.TagStats `json:"tag_stats"`
}
