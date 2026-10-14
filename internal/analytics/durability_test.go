package analytics

import (
	"context"
	"testing"

	"github.com/byPixelTV/flamedb/internal/query"
)

func TestSynchronousQuorumValidation(t *testing.T) {
	for _, test := range []struct {
		setting string
		factor  int
		valid   bool
	}{
		{"", 2, false}, {"FIRST 1 (standby)", 2, false}, {"standby", 2, false},
		{"ANY 0 (standby)", 2, false}, {"ANY 1 (standby)", 2, true},
		{"ANY 1 (one,two)", 3, true}, {"ANY 1 (one,two,three)", 4, false},
		{"ANY 2 (one,two,three)", 4, true}, {"any 2 (*)", 5, true},
	} {
		if _, err := quorumStandbys(test.setting, test.factor); (err == nil) != test.valid {
			t.Fatalf("%q RF%d: %v", test.setting, test.factor, err)
		}
	}
}

func TestPostgresSingleReplicaQuorum(t *testing.T) {
	_, options := integration(t)
	options.QuorumReplicationFactor = 1
	options.BatchSize = 2
	e, err := Open(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if e.batchSize != 2 {
		t.Fatal("configured batch size ignored")
	}
	m := "quorum_local_" + token()
	r := run(t, e, &query.Query{Type: query.QueryTypeWrite, Metric: m, Timestamp: 1, Value: 1, Quorum: true, UpdateLB: true, LBEntityID: "x"})
	if r == nil {
		t.Fatal("missing durable result")
	}
	batch, err := e.ExecuteBatch([]*query.Query{
		{Type: query.QueryTypeWrite, Metric: m, Timestamp: 2, Value: 2, Quorum: true, UpdateLB: true, LBEntityID: "x"},
		{Type: query.QueryTypeWrite, Metric: m, Timestamp: 3, Value: 3, UpdateLB: true, LBEntityID: "x"},
		{Type: query.QueryTypeWrite, Metric: m, Timestamp: 4, Value: 4, UpdateLB: true, LBEntityID: "x"},
		{Type: query.QueryTypeWrite, Metric: m, Timestamp: 5, Value: 5, UpdateLB: true, LBEntityID: "x"},
	})
	if err != nil || batch.Accepted != 4 || batch.Failed != 0 {
		t.Fatalf("mixed quorum batch: %v %v", batch, err)
	}
	score := run(t, e, &query.Query{Type: query.QueryTypeScore, Metric: m, EntityID: "x"}).Ranking.Score
	if score == nil || *score != 15 {
		t.Fatalf("mixed quorum score: %v", score)
	}
}
