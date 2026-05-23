package storage

type Event struct {
	Timestamp int64             // unix nano
	Metric    string            // z.b. "kills", "player_count", "tps"
	Value     float64           // value of the metric, z.b. 5.0 for "kills", 20.0 for "player_count", 19.8 for "tps"
	Tags      map[string]string // z.b. {"player": "pixel", "region": "eu"}
}
