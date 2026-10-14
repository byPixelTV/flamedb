package analytics

import (
	"context"
	"fmt"
	"regexp"
	"strconv"

	"github.com/jackc/pgx/v5"
)

var synchronousQuorum = regexp.MustCompile(`(?i)^\s*ANY\s+([0-9]+)\s*\(.+\)\s*$`)

// quorumStandbys returns the number of remote durable acknowledgements required
// by an explicitly configured PostgreSQL ANY quorum. PostgreSQL validates the
// standby-list grammar itself; FIRST/priority configurations are deliberately
// excluded so the advertised topology has one unambiguous interpretation.
func quorumStandbys(setting string, replicationFactor int) (int, error) {
	match := synchronousQuorum.FindStringSubmatch(setting)
	if match == nil {
		return 0, fmt.Errorf("QUORUM requires PostgreSQL synchronous_standby_names = 'ANY n (...)'")
	}
	n, err := strconv.Atoi(match[1])
	if err != nil || n < replicationFactor/2 {
		return 0, fmt.Errorf("QUORUM replication factor %d requires at least %d synchronous standbys", replicationFactor, replicationFactor/2)
	}
	return n, nil
}

func (e *Engine) configureDurability(ctx context.Context, tx pgx.Tx, quorum bool) error {
	if quorum && e.quorumReplicationFactor > 1 {
		var setting string
		if err := tx.QueryRow(ctx, `SHOW synchronous_standby_names`).Scan(&setting); err != nil {
			return err
		}
		needed, err := quorumStandbys(setting, e.quorumReplicationFactor)
		if err != nil {
			return err
		}
		var active int
		if err = tx.QueryRow(ctx, `SELECT count(*) FROM pg_stat_replication WHERE state='streaming' AND sync_state='quorum'`).Scan(&active); err != nil {
			return err
		}
		if active < needed {
			return fmt.Errorf("QUORUM requires %d active synchronous streaming standbys; found %d", needed, active)
		}
		// remote_apply waits for the configured number of standby WAL records to
		// be durably stored and replayed. If a standby is absent COMMIT waits or
		// returns an error at the deadline; callers never replay that mutation.
		_, err = tx.Exec(ctx, `SET LOCAL synchronous_commit = remote_apply`)
		return err
	}
	_, err := tx.Exec(ctx, `SET LOCAL synchronous_commit = on`)
	return err
}
