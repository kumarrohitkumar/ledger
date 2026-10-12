// Package testdb starts one real PostgreSQL container per test binary, so the
// ledger is tested against the database it runs on, not a mock.
package testdb

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/kumarrohitkumar/ledger/internal/db"
)

// Start launches Postgres (or uses TEST_DATABASE_URL if set), applies the
// migrations, and returns a pool plus a function that stops the container.
func Start(ctx context.Context) (*pgxpool.Pool, func(), error) {
	url := os.Getenv("TEST_DATABASE_URL")
	stop := func() {}
	if url == "" {
		c, err := postgres.Run(ctx, "postgres:17-alpine",
			postgres.WithDatabase("ledger"),
			postgres.WithUsername("ledger"),
			postgres.WithPassword("ledger"),
			testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second)),
		)
		if err != nil {
			return nil, nil, fmt.Errorf("start postgres: %w", err)
		}
		stop = func() { _ = c.Terminate(context.Background()) }
		if url, err = c.ConnectionString(ctx, "sslmode=disable"); err != nil {
			stop()
			return nil, nil, err
		}
	}
	pool, err := db.Connect(ctx, url, 50)
	if err != nil {
		stop()
		return nil, nil, err
	}
	if err := db.Migrate(ctx, pool); err != nil {
		pool.Close()
		stop()
		return nil, nil, err
	}
	return pool, func() { pool.Close(); stop() }, nil
}

// Reset empties every table between tests.
func Reset(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`TRUNCATE accounts, transfers, postings, idempotency_keys, outbox, processed_events, statement_lines RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatalf("reset: %v", err)
	}
}
