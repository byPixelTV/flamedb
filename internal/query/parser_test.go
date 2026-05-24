package query

import "testing"

func TestParseInternalRoutingFlags(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		forceLocal bool
		isReplica  bool
	}{
		{
			name:       "get local",
			input:      "GET bench_kills LIMIT 1 ORDER DESC __local",
			forceLocal: true,
		},
		{
			name:       "stats local",
			input:      "STATS bench_kills TAGS player region __local",
			forceLocal: true,
		},
		{
			name:       "write replica local",
			input:      "WRITE bench_kills 1 __replica __local",
			forceLocal: true,
			isReplica:  true,
		},
		{
			name:      "set replica",
			input:     "SET bench_kills 5 lb=\"pixel\" __replica",
			isReplica: true,
		},
		{
			name:      "delete replica",
			input:     "DELETE bench_kills lb=\"pixel\" __replica",
			isReplica: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			if q.ForceLocal != tt.forceLocal {
				t.Fatalf("ForceLocal = %v, want %v", q.ForceLocal, tt.forceLocal)
			}
			if q.IsReplica != tt.isReplica {
				t.Fatalf("IsReplica = %v, want %v", q.IsReplica, tt.isReplica)
			}
		})
	}
}

func TestParseWriteBatch(t *testing.T) {
	queries, errs := ParseWriteBatch([]string{
		`WRITE kills 1 player="pixel"`,
		`GET kills LIMIT 1`,
		`WRITE money 5 lb="pixel" QUORUM`,
	})
	if len(queries) != 2 {
		t.Fatalf("len(queries) = %d, want 2", len(queries))
	}
	if len(errs) != 1 {
		t.Fatalf("len(errs) = %d, want 1", len(errs))
	}
	if errs[0].Index != 1 {
		t.Fatalf("errs[0].Index = %d, want 1", errs[0].Index)
	}
	if queries[0].Metric != "kills" || queries[0].Tags["player"] != "pixel" {
		t.Fatalf("first write parsed incorrectly: %+v", queries[0])
	}
	if !queries[1].UpdateLB || queries[1].LBEntityID != "pixel" || !queries[1].Quorum {
		t.Fatalf("second write parsed incorrectly: %+v", queries[1])
	}
}
