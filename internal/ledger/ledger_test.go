package ledger_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kumarrohitkumar/ledger/internal/ledger"
	"github.com/kumarrohitkumar/ledger/internal/testdb"
)

var pool *pgxpool.Pool

func TestMain(m *testing.M) {
	ctx := context.Background()
	p, stop, err := testdb.Start(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	pool = p
	code := m.Run()
	stop()
	os.Exit(code)
}

func setup(t *testing.T) (*ledger.Service, context.Context) {
	t.Helper()
	testdb.Reset(t, pool)
	return ledger.New(pool), context.Background()
}

func mustAccount(t *testing.T, s *ledger.Service, name string, opening int64) ledger.Account {
	t.Helper()
	ctx := context.Background()
	acc, err := s.CreateAccount(ctx, ledger.NewAccount{Name: name, Currency: "INR"})
	if err != nil {
		t.Fatal(err)
	}
	if opening > 0 {
		// Money enters through a funding account that may go negative, like a bank's own book.
		fund, err := s.CreateAccount(ctx, ledger.NewAccount{Name: "funding-" + name, Currency: "INR", AllowNegative: true})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.Transfer(ctx, "open-"+acc.ID.String(), ledger.TransferRequest{From: fund.ID, To: acc.ID, Amount: opening}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.GetAccount(ctx, acc.ID)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func balance(t *testing.T, s *ledger.Service, a ledger.Account) int64 {
	t.Helper()
	got, err := s.GetAccount(context.Background(), a.ID)
	if err != nil {
		t.Fatal(err)
	}
	return got.Balance
}

func TestTransferMovesMoneyAndWritesBalancedPostings(t *testing.T) {
	s, ctx := setup(t)
	alice := mustAccount(t, s, "alice", 10_000)
	bob := mustAccount(t, s, "bob", 0)

	tr, replayed, err := s.Transfer(ctx, "k1", ledger.TransferRequest{From: alice.ID, To: bob.ID, Amount: 2_500, Reference: "rent"})
	if err != nil || replayed {
		t.Fatalf("transfer: %v replayed=%v", err, replayed)
	}
	if tr.Amount != 2_500 || tr.Reference != "rent" {
		t.Fatalf("unexpected transfer %+v", tr)
	}
	if got := balance(t, s, alice); got != 7_500 {
		t.Fatalf("alice = %d, want 7500", got)
	}
	if got := balance(t, s, bob); got != 2_500 {
		t.Fatalf("bob = %d, want 2500", got)
	}
	entries, err := s.Entries(ctx, bob.ID, 10)
	if err != nil || len(entries) != 1 || entries[0].Amount != 2_500 || entries[0].TransferID != tr.ID {
		t.Fatalf("bob entries = %+v, err %v", entries, err)
	}
}

func TestSameIdempotencyKeyReturnsTheSameTransferAndMovesMoneyOnce(t *testing.T) {
	s, ctx := setup(t)
	alice := mustAccount(t, s, "alice", 10_000)
	bob := mustAccount(t, s, "bob", 0)
	req := ledger.TransferRequest{From: alice.ID, To: bob.ID, Amount: 1_000}

	first, replayed, err := s.Transfer(ctx, "pay-42", req)
	if err != nil || replayed {
		t.Fatal(err, replayed)
	}
	second, replayed, err := s.Transfer(ctx, "pay-42", req)
	if err != nil || !replayed {
		t.Fatalf("second call: err=%v replayed=%v", err, replayed)
	}
	if second.ID != first.ID {
		t.Fatalf("replay returned a different transfer: %s vs %s", second.ID, first.ID)
	}
	if got := balance(t, s, alice); got != 9_000 {
		t.Fatalf("alice = %d, want 9000 (charged once)", got)
	}
}

func TestReusingAKeyForADifferentRequestIsRejected(t *testing.T) {
	s, ctx := setup(t)
	alice := mustAccount(t, s, "alice", 10_000)
	bob := mustAccount(t, s, "bob", 0)
	if _, _, err := s.Transfer(ctx, "k", ledger.TransferRequest{From: alice.ID, To: bob.ID, Amount: 100}); err != nil {
		t.Fatal(err)
	}
	_, _, err := s.Transfer(ctx, "k", ledger.TransferRequest{From: alice.ID, To: bob.ID, Amount: 999})
	if !errors.Is(err, ledger.ErrIdempotencyKeyReused) {
		t.Fatalf("err = %v, want ErrIdempotencyKeyReused", err)
	}
}

func TestInsufficientFundsIsRejectedAndRememberedUnderTheKey(t *testing.T) {
	s, ctx := setup(t)
	alice := mustAccount(t, s, "alice", 500)
	bob := mustAccount(t, s, "bob", 0)
	req := ledger.TransferRequest{From: alice.ID, To: bob.ID, Amount: 501}

	_, _, err := s.Transfer(ctx, "k", req)
	if !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Fatalf("err = %v, want ErrInsufficientFunds", err)
	}
	// A retry with the same key gets the same answer, even after the account is topped up:
	// the client asked once, so it gets one decision.
	if _, _, err := s.Transfer(ctx, "topup", ledger.TransferRequest{From: bob.ID, To: alice.ID, Amount: 0}); !errors.Is(err, ledger.ErrInvalidAmount) {
		t.Fatalf("zero amount err = %v", err)
	}
	_, replayed, err := s.Transfer(ctx, "k", req)
	if !errors.Is(err, ledger.ErrInsufficientFunds) || !replayed {
		t.Fatalf("retry: err=%v replayed=%v", err, replayed)
	}
	if got := balance(t, s, alice); got != 500 {
		t.Fatalf("alice = %d, want 500", got)
	}
}

func TestValidation(t *testing.T) {
	s, ctx := setup(t)
	alice := mustAccount(t, s, "alice", 1_000)
	usd, _ := s.CreateAccount(ctx, ledger.NewAccount{Name: "usd", Currency: "USD"})
	cases := []struct {
		name string
		req  ledger.TransferRequest
		want error
	}{
		{"same account", ledger.TransferRequest{From: alice.ID, To: alice.ID, Amount: 1}, ledger.ErrSameAccount},
		{"negative", ledger.TransferRequest{From: alice.ID, To: usd.ID, Amount: -5}, ledger.ErrInvalidAmount},
		{"currency mismatch", ledger.TransferRequest{From: alice.ID, To: usd.ID, Amount: 5}, ledger.ErrCurrencyMismatch},
		{"unknown account", ledger.TransferRequest{From: alice.ID, To: ledger.NewID(), Amount: 5}, ledger.ErrAccountNotFound},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, _, err := s.Transfer(ctx, fmt.Sprintf("v%d", i), c.req)
			if !errors.Is(err, c.want) {
				t.Fatalf("err = %v, want %v", err, c.want)
			}
		})
	}
	if _, _, err := s.Transfer(ctx, "", ledger.TransferRequest{From: alice.ID, To: usd.ID, Amount: 1}); !errors.Is(err, ledger.ErrMissingIdempotencyKey) {
		t.Fatalf("empty key err = %v", err)
	}
}

func TestConcurrentTransfersConserveMoneyAndNeverOverdraw(t *testing.T) {
	s, ctx := setup(t)
	const n = 5
	accounts := make([]ledger.Account, n)
	for i := range accounts {
		accounts[i] = mustAccount(t, s, fmt.Sprintf("acc%d", i), 1_000)
	}

	// 200 transfers in random directions, 50 at a time. Some must fail for lack of
	// funds; none may leave an account negative or create or destroy money.
	var wg sync.WaitGroup
	sem := make(chan struct{}, 50)
	for i := 0; i < 200; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			from, to := accounts[i%n], accounts[(i*7+3)%n]
			if from.ID == to.ID {
				return
			}
			_, _, err := s.Transfer(ctx, fmt.Sprintf("c%d", i), ledger.TransferRequest{From: from.ID, To: to.ID, Amount: int64(50 + i%400)})
			if err != nil && !errors.Is(err, ledger.ErrInsufficientFunds) {
				t.Errorf("transfer %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	var total int64
	for _, a := range accounts {
		b := balance(t, s, a)
		if b < 0 {
			t.Fatalf("%s went negative: %d", a.Name, b)
		}
		total += b
	}
	if total != n*1_000 {
		t.Fatalf("money not conserved: total %d, want %d", total, n*1_000)
	}
	report, err := s.Reconcile(ctx)
	if err != nil || !report.OK() {
		t.Fatalf("reconcile: %+v err %v", report, err)
	}
}

func TestTheSameKeyFiredConcurrentlyCreatesExactlyOneTransfer(t *testing.T) {
	s, ctx := setup(t)
	alice := mustAccount(t, s, "alice", 10_000)
	bob := mustAccount(t, s, "bob", 0)
	req := ledger.TransferRequest{From: alice.ID, To: bob.ID, Amount: 300}

	var wg sync.WaitGroup
	ids := make(chan ledger.TransferID, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tr, _, err := s.Transfer(ctx, "double-click", req)
			if err != nil {
				t.Errorf("transfer: %v", err)
				return
			}
			ids <- tr.ID
		}()
	}
	wg.Wait()
	close(ids)
	seen := map[ledger.TransferID]bool{}
	for id := range ids {
		seen[id] = true
	}
	if len(seen) != 1 {
		t.Fatalf("got %d different transfers, want 1", len(seen))
	}
	if got := balance(t, s, bob); got != 300 {
		t.Fatalf("bob = %d, want 300 (paid once)", got)
	}
}

func TestEveryTransferWritesExactlyOneOutboxEvent(t *testing.T) {
	s, ctx := setup(t)
	alice := mustAccount(t, s, "alice", 1_000)
	bob := mustAccount(t, s, "bob", 0)
	before := outboxCount(t)
	if _, _, err := s.Transfer(ctx, "k", ledger.TransferRequest{From: alice.ID, To: bob.ID, Amount: 10}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Transfer(ctx, "k", ledger.TransferRequest{From: alice.ID, To: bob.ID, Amount: 10}); err != nil {
		t.Fatal(err)
	}
	if got := outboxCount(t) - before; got != 1 {
		t.Fatalf("outbox events = %d, want 1 (the replay must not emit again)", got)
	}
}

func outboxCount(t *testing.T) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
