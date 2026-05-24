package query

import (
	"time"

	"github.com/byPixelTV/flamedb/internal/aggregates"
	"github.com/byPixelTV/flamedb/internal/storage"
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
	QueryTypeGet              QueryType = "GET"
	QueryTypeLeaderboard      QueryType = "LEADERBOARD"
	QueryTypeWrite            QueryType = "WRITE"
	QueryTypeSet              QueryType = "SET"
	QueryTypeDelete           QueryType = "DELETE"
	QueryTypeStats            QueryType = "STATS"
	QueryTypeGroupLeaderboard QueryType = "GROUP_LEADERBOARD"
)

type Query struct {
	Type      QueryType
	Metric    string
	Metrics   []string
	Where     map[string]string
	Timestamp int64
	From      int64
	To        int64
	Limit     int
	Offset    int
	Order     string
	// write specific
	Value       float64
	Tags        map[string]string
	UpdateLB    bool     // should this update the leaderboard?
	LBEntityID  string   // entity ID for leaderboard updates
	TagKeys     []string // for get queries, which tag keys to return
	Quorum      bool
	IsReplica   bool
	ForceLocal  bool
	Groups      []GroupDef
	Aggregate   AggType
	GroupBy     time.Duration
	GroupBySpec string // original spec, z.B. "1y6m"
}

type Result struct {
	Events         []storage.Event               `json:"events,omitempty"`
	Metrics        map[string][]storage.Event    `json:"metrics,omitempty"`
	Leaderboard    []aggregates.LeaderboardEntry `json:"leaderboard,omitempty"`
	Stats          *StatsResult                  `json:"stats,omitempty"`
	Aggregate      *AggregateResult              `json:"aggregate,omitempty"`
	Aggregates     map[string]*AggregateResult   `json:"aggregates,omitempty"`
	Series         []SeriesPoint                 `json:"series,omitempty"`
	SeriesByMetric map[string][]SeriesPoint      `json:"series_by_metric,omitempty"`
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
