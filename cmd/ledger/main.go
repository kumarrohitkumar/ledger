// Command ledger runs the HTTP API, the outbox relay and the statement consumer.
// ROLE selects which (default "all"), so they can also run as separate deployments.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kumarrohitkumar/ledger/internal/api"
	"github.com/kumarrohitkumar/ledger/internal/consumer"
	"github.com/kumarrohitkumar/ledger/internal/db"
	"github.com/kumarrohitkumar/ledger/internal/ledger"
	"github.com/kumarrohitkumar/ledger/internal/outbox"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	role := env("ROLE", "all")
	brokers := strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",")
	pool, err := db.Connect(ctx, env("DATABASE_URL", "postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable"), 40)
	if err != nil {
		slog.Error("database", "err", err)
		os.Exit(1)
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool); err != nil {
		slog.Error("migrate", "err", err)
		os.Exit(1)
	}

	var wg sync.WaitGroup
	run := func(name string, f func()) {
		wg.Add(1)
		go func() { defer wg.Done(); slog.Info("started", "component", name); f() }()
	}

	if role == "all" || role == "relay" {
		prod, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.AllowAutoTopicCreation())
		if err != nil {
			slog.Error("kafka producer", "err", err)
			os.Exit(1)
		}
		defer prod.Close()
		run("relay", func() { outbox.NewRelay(pool, prod).Run(ctx, 200*time.Millisecond) })
	}
	if role == "all" || role == "consumer" {
		cons, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumerGroup("ledger-statements"),
			kgo.ConsumeTopics(ledger.TopicTransfers), kgo.DisableAutoCommit(), kgo.AllowAutoTopicCreation(),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
		if err != nil {
			slog.Error("kafka consumer", "err", err)
			os.Exit(1)
		}
		defer cons.Close()
		run("consumer", func() { consumer.New(pool, cons).Run(ctx) })
	}
	if role == "all" || role == "api" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.Handle("/", api.New(ledger.New(pool)).Routes())
		srv := &http.Server{Addr: env("ADDR", ":8080"), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		run("api", func() {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("http", "err", err)
				stop()
			}
		})
		go func() {
			<-ctx.Done()
			shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdown) // finish in-flight requests before exiting
		}()
	}
	wg.Wait()
	slog.Info("stopped")
}
