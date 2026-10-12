// End-to-end tests of ledger -> outbox -> Kafka -> consumer, including crashes at
// the two moments that usually cause double processing.
package pipeline_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kumarrohitkumar/ledger/internal/consumer"
	"github.com/kumarrohitkumar/ledger/internal/ledger"
	"github.com/kumarrohitkumar/ledger/internal/outbox"
	"github.com/kumarrohitkumar/ledger/internal/testdb"
)

var (
	pool    *pgxpool.Pool
	brokers string
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	p, stop, err := testdb.Start(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	pool = p
	rp, err := redpanda.Run(ctx, "docker.redpanda.com/redpandadata/redpanda:v24.2.7")
	if err != nil {
		fmt.Fprintln(os.Stderr, "start redpanda:", err)
		stop()
		os.Exit(1)
	}
	if brokers, err = rp.KafkaSeedBroker(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	_ = rp.Terminate(ctx)
	stop()
	os.Exit(code)
}

// freshTopic gives each test its own topic and group, so tests cannot see each other's records.
func freshTopic(t *testing.T) (topic, group string) {
	t.Helper()
	topic = fmt.Sprintf("transfers-%d", time.Now().UnixNano())
	adm, err := kgo.NewClient(kgo.SeedBrokers(brokers))
	if err != nil {
		t.Fatal(err)
	}
	defer adm.Close()
	if _, err := kadm.NewClient(adm).CreateTopic(context.Background(), 3, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
	return topic, topic + "-statements"
}

func producer(t *testing.T, topic string) *kgo.Client {
	t.Helper()
	c, err := kgo.NewClient(kgo.SeedBrokers(brokers), kgo.DefaultProduceTopic(topic), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func consumerClient(t *testing.T, topic, group string) *kgo.Client {
	t.Helper()
	c, err := kgo.NewClient(kgo.SeedBrokers(brokers), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// makeTransfers creates n transfers and points their outbox rows at this test's topic.
func makeTransfers(t *testing.T, n int, topic string) {
	t.Helper()
	testdb.Reset(t, pool)
	s, ctx := ledger.New(pool), context.Background()
	bank, _ := s.CreateAccount(ctx, ledger.NewAccount{Name: "bank", Currency: "INR", AllowNegative: true})
	alice, _ := s.CreateAccount(ctx, ledger.NewAccount{Name: "alice", Currency: "INR"})
	for i := 0; i < n; i++ {
		if _, _, err := s.Transfer(ctx, fmt.Sprintf("t%d", i), ledger.TransferRequest{From: bank.ID, To: alice.ID, Amount: int64(100 + i)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE outbox SET topic = $1`, topic); err != nil {
		t.Fatal(err)
	}
}

// drain polls until `want` events are processed and at least `seen` records have
// been delivered to this consumer (a consumer joining a group can get an empty
// first poll, so "nothing new" alone does not mean "caught up").
func drain(t *testing.T, c *consumer.Consumer, want, seen int) (applied, duplicates int) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		a, d, err := c.PollOnce(ctx)
		cancel()
		if err != nil {
			t.Fatalf("poll: %v", err)
		}
		applied, duplicates = applied+a, duplicates+d
		if count(t, "processed_events") >= want && applied+duplicates >= seen && a == 0 && d == 0 {
			return
		}
	}
	t.Fatalf("timed out: processed %d of %d", count(t, "processed_events"), want)
	return
}

func count(t *testing.T, table string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func reconcileOK(t *testing.T) {
	t.Helper()
	rep, err := ledger.New(pool).Reconcile(context.Background())
	if err != nil || !rep.OK() || rep.UnpublishedEvents != 0 || rep.UnprocessedEvents != 0 {
		t.Fatalf("reconcile: %+v err %v", rep, err)
	}
}

func TestEveryTransferReachesTheStatementsOnce(t *testing.T) {
	topic, group := freshTopic(t)
	makeTransfers(t, 50, topic)

	relay := outbox.NewRelay(pool, producer(t, topic))
	for {
		n, err := relay.RunOnce(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
	}
	cc := consumerClient(t, topic, group)
	defer cc.Close()
	drain(t, consumer.New(pool, cc), 50, 50)

	if got := count(t, "statement_lines"); got != 100 {
		t.Fatalf("statement lines = %d, want 100 (two per transfer)", got)
	}
	reconcileOK(t)
}

func TestRelayCrashAfterPublishingIsHarmless(t *testing.T) {
	topic, group := freshTopic(t)
	makeTransfers(t, 30, topic)

	// The relay dies after Kafka has the events but before it marks them published.
	crashing := outbox.NewRelay(pool, producer(t, topic))
	crashing.AfterPublish = func() error { return errors.New("simulated crash") }
	if _, err := crashing.RunOnce(context.Background()); err == nil {
		t.Fatal("expected the simulated crash")
	}
	// On restart it publishes the same 30 events again.
	relay := outbox.NewRelay(pool, producer(t, topic))
	if n, err := relay.RunOnce(context.Background()); err != nil || n != 30 {
		t.Fatalf("restart: n=%d err=%v", n, err)
	}

	cc := consumerClient(t, topic, group)
	defer cc.Close()
	applied, duplicates := drain(t, consumer.New(pool, cc), 30, 60)
	if applied != 30 || duplicates != 30 {
		t.Fatalf("applied %d, duplicates %d; want 30 and 30 (every event arrived twice)", applied, duplicates)
	}
	if got := count(t, "statement_lines"); got != 60 {
		t.Fatalf("statement lines = %d, want 60: duplicates must change nothing", got)
	}
	reconcileOK(t)
}

func TestConsumerCrashBeforeCommittingOffsetsIsHarmless(t *testing.T) {
	topic, group := freshTopic(t)
	makeTransfers(t, 20, topic)
	relay := outbox.NewRelay(pool, producer(t, topic))
	if _, err := relay.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	// The first consumer stores the effects, then dies before committing offsets.
	first := consumerClient(t, topic, group)
	c1 := consumer.New(pool, first)
	c1.AfterHandle = func() error { return errors.New("simulated crash") }
	deadline := time.Now().Add(30 * time.Second)
	for count(t, "processed_events") < 20 && time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _, _ = c1.PollOnce(ctx)
		cancel()
	}
	first.Close()

	// A replacement in the same group is handed the same records again.
	second := consumerClient(t, topic, group)
	defer second.Close()
	_, duplicates := drain(t, consumer.New(pool, second), 20, 20)
	if duplicates == 0 {
		t.Fatal("expected redelivered records after the crash")
	}
	if got := count(t, "statement_lines"); got != 40 {
		t.Fatalf("statement lines = %d, want 40", got)
	}
	reconcileOK(t)
}

func TestTwoRelaysSideBySideDoNotDoubleSend(t *testing.T) {
	topic, _ := freshTopic(t)
	makeTransfers(t, 400, topic)
	a := outbox.NewRelay(pool, producer(t, topic))
	b := outbox.NewRelay(pool, producer(t, topic))
	a.BatchSize, b.BatchSize = 50, 50

	done := make(chan int, 2)
	for _, r := range []*outbox.Relay{a, b} {
		go func(r *outbox.Relay) {
			total := 0
			for {
				n, err := r.RunOnce(context.Background())
				if err != nil {
					t.Error(err)
				}
				if n == 0 {
					done <- total
					return
				}
				total += n
			}
		}(r)
	}
	if got := <-done + <-done; got != 400 {
		t.Fatalf("relays marked %d events, want exactly 400", got)
	}
}
