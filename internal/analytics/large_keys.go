package analytics

import (
	_ "embed"
	"github.com/byPixelTV/flamedb/internal/query"
)

// Small text identities retain native unique UPSERT indexes. Larger identities
// keep their complete text and use a canonical fixed-size row ID under the
// PostgreSQL metric lock. Digests only narrow lookups; equality is always exact.
const indexTextBudget = 2000

//go:embed large_keys.sql
var largeKeySchema string

func needsLargeIdentityLock(q *query.Query) bool {
	metricBytes := len(EncodeString(q.Metric))
	if metricBytes > indexTextBudget {
		return true
	}
	for key, value := range q.Tags {
		if metricBytes+len(EncodeString(key))+len(EncodeString(value)) > indexTextBudget {
			return true
		}
	}
	return false
}
