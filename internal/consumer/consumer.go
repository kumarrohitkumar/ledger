// Package consumer builds account statements from transfer events.
//
// Each event's effect and its row in processed_events commit in one database
// transaction, and the Kafka offset is committed only afterwards. A redelivered
// event (after a relay or consumer crash) finds its id already processed and
// changes nothing: at-least-once delivery, exactly-once effect.
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kumarrohitkumar/ledger/internal/ledger"
)

type Consumer struct {
	pool   *pgxpool.Pool
	client *kgo.Client
	// AfterHandle runs after a batch is stored and before its offsets are committed.
	// Tests return an error to simulate a crash that forces redelivery.
	AfterHandle func() error
}

func New(pool *pgxpool.Pool, client *kgo.Client) *Consumer { return &Consumer{pool: pool, client: client} }

type event struct {
	Type     string          `json:"type"`
	Transfer ledger.Transfer `json:"transfer"`
}

// Handle applies one record. It returns applied=false for a duplicate.
func (c *Consumer) Handle(ctx context.Context, rec *kgo.Record) (applied bool, err error) {
	var eventID string
	for _, h := range rec.Headers {
		if h.Key == "event_id" {
			eventID = string(h.Value)
		}
	}
	if eventID == "" {
		return false, errors.New("record has no event_id header")
	}
	var ev event
	if err := json.Unmarshal(rec.Value, &ev); err != nil {
		return false, fmt.Errorf("decode event %s: %w", eventID, err)
	}

	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	tag, err := tx.Exec(ctx, `INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`, eventID)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, nil // seen before: the effect is already in the database
	}
	if ev.Type == "transfer.created" {
		t := ev.Transfer
		if _, err := tx.Exec(ctx, `
			INSERT INTO statement_lines (account_id, transfer_id, amount, created_at)
			VALUES ($1, $3, $4, $5), ($2, $3, $6, $5)`,
			t.From, t.To, t.ID, -t.Amount, t.CreatedAt, t.Amount); err != nil {
			return false, err
		}
	}
	return true, tx.Commit(ctx)
}

// PollOnce fetches one batch, applies it, then commits the offsets.
func (c *Consumer) PollOnce(ctx context.Context) (applied, duplicates int, err error) {
	fetches := c.client.PollFetches(ctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		for _, e := range errs {
			if !errors.Is(e.Err, context.DeadlineExceeded) && !errors.Is(e.Err, context.Canceled) {
				return 0, 0, e.Err
			}
		}
	}
	records := fetches.Records()
	for _, rec := range records {
		ok, err := c.Handle(ctx, rec)
		if err != nil {
			return applied, duplicates, err
		}
		if ok {
			applied++
		} else {
			duplicates++
		}
	}
	if len(records) == 0 {
		return 0, 0, nil
	}
	if c.AfterHandle != nil {
		if err := c.AfterHandle(); err != nil {
			return applied, duplicates, err
		}
	}
	return applied, duplicates, c.client.CommitRecords(ctx, records...)
}

// Run consumes until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if _, _, err := c.PollOnce(ctx); err != nil && ctx.Err() == nil {
			slog.Error("consumer", "err", err)
		}
	}
}
