package storage

import "encoding/json"

type Event struct {
	Timestamp int64             // unix nano
	Metric    string            // z.b. "kills", "player_count", "tps"
	Value     float64           // value of the metric, z.b. 5.0 for "kills", 20.0 for "player_count", 19.8 for "tps"
	Tags      map[string]string // z.b. {"player": "pixel", "region": "eu"}
}

// MarshalJSON retains every legacy field and adds the lowercase aliases used by
// released Kotlin/TypeScript SDKs. The aliases intentionally share one value.
func (e Event) MarshalJSON() ([]byte, error) {
	type fields struct {
		TimestampAlias int64             `json:"timestamp"`
		MetricAlias    string            `json:"metric"`
		ValueAlias     float64           `json:"value"`
		TagsAlias      map[string]string `json:"tags"`
		Timestamp      int64
		Metric         string
		Value          float64
		Tags           map[string]string
	}
	tags := e.Tags
	// Legacy Tags remains null for untagged events; Kotlin's non-null map accepts
	// the empty lowercase object, while all old uppercase bytes stay unchanged.
	// Emit the legacy fields last so Go's case-insensitive decoder preserves nil.
	aliases := tags
	if aliases == nil {
		aliases = map[string]string{}
	}
	return json.Marshal(fields{Timestamp: e.Timestamp, Metric: e.Metric, Value: e.Value, Tags: tags, TimestampAlias: e.Timestamp, MetricAlias: e.Metric, ValueAlias: e.Value, TagsAlias: aliases})
}
