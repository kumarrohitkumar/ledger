package ledger

import "context"

// Report is the result of checking the ledger's invariants. Hard violations mean
// money is wrong; the lag figures only say how far behind the pipeline is.
type Report struct {
	TotalPostings         int64 `json:"total_postings_sum"`        // must be 0: money is never created or destroyed
	UnbalancedTransfers   int   `json:"unbalanced_transfers"`      // transfers whose postings do not sum to 0
	BalanceDrift          int   `json:"accounts_with_balance_drift"` // cached balance != sum of postings
	TransfersWithoutEvent int   `json:"transfers_without_event"`   // a transfer the outbox never recorded
	UnpublishedEvents     int   `json:"unpublished_events"`        // lag: waiting for the relay
	UnprocessedEvents     int   `json:"unprocessed_events"`        // lag: published, not yet consumed
}

func (r Report) OK() bool {
	return r.TotalPostings == 0 && r.UnbalancedTransfers == 0 && r.BalanceDrift == 0 && r.TransfersWithoutEvent == 0
}

// Reconcile checks every invariant from the database alone, so it can run as a
// cron job against production without trusting the service that wrote the data.
func (s *Service) Reconcile(ctx context.Context) (Report, error) {
	var r Report
	err := s.pool.QueryRow(ctx, `
		SELECT
		  (SELECT COALESCE(SUM(amount), 0) FROM postings),
		  (SELECT count(*) FROM (SELECT transfer_id FROM postings GROUP BY transfer_id HAVING SUM(amount) <> 0 OR count(*) <> 2) u),
		  (SELECT count(*) FROM accounts a WHERE a.balance <> COALESCE((SELECT SUM(p.amount) FROM postings p WHERE p.account_id = a.id), 0)),
		  (SELECT count(*) FROM transfers t WHERE NOT EXISTS (SELECT 1 FROM outbox o WHERE o.key = t.id::text)),
		  (SELECT count(*) FROM outbox WHERE published_at IS NULL),
		  (SELECT count(*) FROM outbox o WHERE o.published_at IS NOT NULL AND NOT EXISTS (SELECT 1 FROM processed_events p WHERE p.event_id = o.event_id))
	`).Scan(&r.TotalPostings, &r.UnbalancedTransfers, &r.BalanceDrift, &r.TransfersWithoutEvent, &r.UnpublishedEvents, &r.UnprocessedEvents)
	return r, err
}
