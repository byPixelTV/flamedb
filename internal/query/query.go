package query

type QueryType string

const (
	QueryTypeGet         QueryType = "GET"
	QueryTypeLeaderboard QueryType = "LEADERBOARD"
	QueryTypeWrite       QueryType = "WRITE"
	QueryTypeSet         QueryType = "SET"
	QueryTypeDelete      QueryType = "DELETE"
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
	UpdateLB   bool   // should this update the leaderboard?
	LBEntityID string // entity ID for leaderboard updates
}
