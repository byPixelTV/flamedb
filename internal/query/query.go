package query

import (
	"github.com/byPixelTV/flamedb/internal/aggregates"
	"github.com/byPixelTV/flamedb/internal/storage"
)

type QueryType string

type GroupDef struct {
	Name    string
	Members []string
}

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
	Where     map[string]string
	Timestamp int64
	From      int64
	To        int64
	Limit     int
	Offset    int
	Order     string
	// write specific
	Value      float64
	Tags       map[string]string
	UpdateLB   bool     // should this update the leaderboard?
	LBEntityID string   // entity ID for leaderboard updates
	TagKeys    []string // for get queries, which tag keys to return
	Quorum     bool
	IsReplica  bool
	Groups     []GroupDef
}

type Result struct {
	Events      []storage.Event               `json:"events,omitempty"`
	Leaderboard []aggregates.LeaderboardEntry `json:"leaderboard,omitempty"`
	Stats       *StatsResult                  `json:"stats,omitempty"`
}

type StatsResult struct {
	Metric   string             `json:"metric"`
	TagStats []storage.TagStats `json:"tag_stats"`
}
