// Package metrics defines the Prometheus series the service exposes on /metrics.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	Transfers = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ledger_transfers_total", Help: "Transfer requests by outcome: created, replayed or refused.",
	}, []string{"outcome"})
	OutboxPublished = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ledger_outbox_published_total", Help: "Events the relay marked published.",
	})
	ConsumerEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ledger_consumer_events_total", Help: "Events the consumer saw: applied or duplicate.",
	}, []string{"result"})
	httpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "http_request_duration_seconds", Help: "HTTP latency by route and status.",
		Buckets: []float64{.002, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
	}, []string{"route", "code"})
)

type recorder struct {
	http.ResponseWriter
	code int
}

func (r *recorder) WriteHeader(code int) { r.code = code; r.ResponseWriter.WriteHeader(code) }

// Instrument records latency per route pattern (not per raw URL, to keep cardinality low).
func Instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &recorder{ResponseWriter: w, code: 200}
		next.ServeHTTP(rec, r)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		httpDuration.WithLabelValues(route, strconv.Itoa(rec.code)).Observe(time.Since(start).Seconds())
	})
}
