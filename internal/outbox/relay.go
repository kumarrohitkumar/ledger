// Package outbox publishes events written by the ledger to Kafka.
//
// Delivery is at-least-once: an event is marked published only after Kafka has
// acknowledged it, so a crash in between publishes it again on the next run.
// Consumers deduplicate by event id, which turns that into an effect-once pipeline.
package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kumarrohitkumar/ledger/internal/metrics"
)

type Relay struct {
	pool      *pgxpool.Pool
	client    *kgo.Client
	BatchSize int
	// AfterPublish runs after Kafka acknowledges a batch and before it is marked
	// published. Tests return an error here to simulate a crash at the worst moment.
	AfterPublish func() error
}

func NewRelay(pool *pgxpool.Pool, client *kgo.Client) *Relay {
	return &Relay{pool: pool, client: client, BatchSize: 200}
}

// RunOnce publishes up to one batch and returns how many events it marked.
// SKIP LOCKED lets several relay instances run side by side without sending the
// same row twice in the normal case.
func (r *Relay) RunOnce(ctx context.Context) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	rows, err := tx.Query(ctx, `
		SELECT id, event_id::text, topic, key, payload
		FROM outbox WHERE published_at IS NULL
		ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`, r.BatchSize)
	if err != nil {
		return 0, err
	}
	var ids []int64
	var records []*kgo.Record
	for rows.Next() {
		var id int64
		var eventID, topic, key string
		var payload []byte
		if err := rows.Scan(&id, &eventID, &topic, &key, &payload); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
		records = append(records, &kgo.Record{
			Topic: topic, Key: []byte(key), Value: payload,
			Headers: []kgo.RecordHeader{{Key: "event_id", Value: []byte(eventID)}},
		})
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(records) == 0 {
		return 0, nil
	}
	// Records with the same key (transfer id) go to the same partition, in order.
	if err := r.client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return 0, fmt.Errorf("publish: %w", err)
	}
	if r.AfterPublish != nil {
		if err := r.AfterPublish(); err != nil {
			return 0, err
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	metrics.OutboxPublished.Add(float64(len(ids)))
	return len(ids), nil
}

// Run drains the outbox continuously until ctx is cancelled.
func (r *Relay) Run(ctx context.Context, idle time.Duration) {
	for {
		n, err := r.RunOnce(ctx)
		if err != nil && ctx.Err() == nil {
			slog.Error("outbox relay", "err", err)
		}
		if n > 0 && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(idle):
		}
	}
}

