package query

import (
	"github.com/byPixelTV/flamedb/internal/aggregates"
	"github.com/byPixelTV/flamedb/internal/storage"
	"math"
	"testing"
	"time"
)

func TestRejectMalformedQueries(t *testing.T) {
	for _, input := range []string{`SET x 1 lb=`, `DELETE x FROM`, `DELETE x TO`, `GET x OFFSET -1`, `GROUP_LEADERBOARD x GROUP "g:a" OFFSET -1`, `GET x LIMIT -1`, `GET x GROUP BY 1`, `GET x GROUP BY 0s`, `GET x GROUP BY 999999999999999h`, `WRITE x NaN`, `WRITE x +Inf`, `WRITE lb 1`, `WRITE x 1 tag="open`, `WRITE x 1 junk`} {
		t.Run(input, func(t *testing.T) {
			if q, err := Parse(input); err == nil {
				t.Fatalf("accepted %q: %+v", input, q)
			}
		})
	}
}
func TestEscapedValues(t *testing.T) {
	q, err := Parse(`WRITE x 1 tag="a\"b\\c\nvalue"`)
	if err != nil {
		t.Fatal(err)
	}
	if q.Tags["tag"] != "a\"b\\c\nvalue" {
		t.Fatalf("%q", q.Tags["tag"])
	}
}
func TestFixedBucketsJumpToModernDates(t *testing.T) {
	ts := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC).UnixNano()
	got := aggregateSeriesUTC([]storage.Event{{Timestamp: ts, Value: 2}, {Timestamp: ts + 2e9, Value: 3}}, "1s", AggSum)
	if len(got) != 2 || got[0].Value != 2 || got[1].TS != ts+2e9 {
		t.Fatalf("%+v", got)
	}
}
func TestReplicaReceiptSurvivesRestart(t *testing.T) {
	path := t.TempDir()
	for pass := 0; pass < 2; pass++ {
		store, err := storage.Open(path, "none")
		if err != nil {
			t.Fatal(err)
		}
		lb := aggregates.New(store.DB())
		exec := NewExecutor(store, lb)
		q, err := Parse(`WRITE kills 2 lb="p" ts=123 __replica __op=0123456789abcdef0123456789abcdef`)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 2; i++ {
			if _, err = exec.Execute(q); err != nil {
				t.Fatal(err)
			}
		}
		score, err := lb.Get("kills", "p")
		if err != nil || score != 2 {
			t.Fatalf("score=%v err=%v", score, err)
		}
		if err = store.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
func TestDeleteEventsAndIndex(t *testing.T) {
	s, err := storage.Open(t.TempDir(), "none")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e := NewExecutor(s, aggregates.New(s.DB()))
	for _, line := range []string{`WRITE x 2 tag="a" ts=123`, `DELETE x`} {
		q, err := Parse(line)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = e.Execute(q); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.ReadRangeWithTags("x", 0, math.MaxInt64, map[string]string{"tag": "a"})
	if err != nil || len(got) != 0 {
		t.Fatalf("%v %v", got, err)
	}
}
func FuzzParse(f *testing.F) {
	for _, seed := range []string{"SET x 1 lb=", "DELETE x FROM", "GET x GROUP BY 1", "GET x OFFSET -1", `WRITE x 1 tag="hi"`} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) { _, _ = Parse(s) })
}

func BenchmarkFixedSecondBuckets(b *testing.B) {
	events := []storage.Event{{Timestamp: time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC).UnixNano(), Value: 1}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		aggregateSeriesUTC(events, "1s", AggSum)
	}
}
