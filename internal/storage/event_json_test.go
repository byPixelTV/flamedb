package storage

import (
	"encoding/json"
	"testing"
)

func TestEventAliasesPreserveLegacyAndExactNanoseconds(t *testing.T) {
	for _, tags := range []map[string]string{nil, {"player": "玩家"}} {
		event := Event{Timestamp: 1704067200000000001, Metric: "website:views", Value: -1.25, Tags: tags}
		data, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err = json.Unmarshal(data, &fields); err != nil {
			t.Fatal(err)
		}
		for _, pair := range [][2]string{{"Timestamp", "timestamp"}, {"Metric", "metric"}, {"Value", "value"}} {
			if string(fields[pair[0]]) != string(fields[pair[1]]) {
				t.Fatal(string(data))
			}
		}
		if string(fields["Timestamp"]) != "1704067200000000001" {
			t.Fatal("timestamp rounded")
		}
		if tags == nil {
			if string(fields["Tags"]) != "null" || string(fields["tags"]) != "{}" {
				t.Fatal(string(data))
			}
		} else if string(fields["Tags"]) != string(fields["tags"]) {
			t.Fatal(string(data))
		}
		var roundtrip Event
		if err = json.Unmarshal(data, &roundtrip); err != nil {
			t.Fatal(err)
		}
		if roundtrip.Timestamp != event.Timestamp || roundtrip.Metric != event.Metric || roundtrip.Value != event.Value {
			t.Fatal(roundtrip)
		}
		if tags == nil && roundtrip.Tags != nil {
			t.Fatal("legacy nil tags changed")
		}
	}
}
