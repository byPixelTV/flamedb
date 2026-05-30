package query

import (
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
	UpdateLB    bool
	LBEntityID  string
	TagKeys     []string
	Quorum      bool
	IsReplica   bool
	ForceLocal  bool
	Groups      []GroupDef
	Aggregate   AggType
	GroupBy     time.Duration
	GroupBySpec string

	// EntityTag ist der Tag-Key für die entityID bei windowed Leaderboards.
	// Pflichtfeld wenn FROM oder TO gesetzt ist.
	// Beispiel: LEADERBOARD kills FROM now-7d TO now ENTITY player
	EntityTag string
}

type Result struct {
	Events         []storage.Event             `json:"events,omitempty"`
	Metrics        map[string][]storage.Event  `json:"metrics,omitempty"`
	Leaderboard    []types.LeaderboardEntry    `json:"leaderboard,omitempty"`
	Stats          *StatsResult                `json:"stats,omitempty"`
	Aggregate      *AggregateResult            `json:"aggregate,omitempty"`
	Aggregates     map[string]*AggregateResult `json:"aggregates,omitempty"`
	Series         []SeriesPoint               `json:"series,omitempty"`
	SeriesByMetric map[string][]SeriesPoint    `json:"series_by_metric,omitempty"`
}

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
