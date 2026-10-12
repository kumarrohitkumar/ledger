package ledger

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const TopicTransfers = "ledger.transfers"

type Service struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Service { return &Service{pool: pool} }

var currencyRe = regexp.MustCompile(`^[A-Z]{3}$`)

func (s *Service) CreateAccount(ctx context.Context, in NewAccount) (Account, error) {
	in.Name = strings.TrimSpace(in.Name)
	in.Currency = strings.ToUpper(strings.TrimSpace(in.Currency))
	if in.Name == "" || len(in.Name) > 100 || !currencyRe.MatchString(in.Currency) {
		return Account{}, ErrInvalidAccount
	}
	a := Account{ID: NewID(), Name: in.Name, Currency: in.Currency, AllowNegative: in.AllowNegative}
	err := s.pool.QueryRow(ctx,
		`INSERT INTO accounts (id, name, currency, allow_negative) VALUES ($1, $2, $3, $4) RETURNING created_at`,
		a.ID, a.Name, a.Currency, a.AllowNegative).Scan(&a.CreatedAt)
	return a, err
}

func (s *Service) GetAccount(ctx context.Context, id AccountID) (Account, error) {
	var a Account
	err := s.pool.QueryRow(ctx,
		`SELECT id, name, currency, allow_negative, balance, created_at FROM accounts WHERE id = $1`, id).
		Scan(&a.ID, &a.Name, &a.Currency, &a.AllowNegative, &a.Balance, &a.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Account{}, ErrAccountNotFound
	}
	return a, err
}

// Entries lists an account's postings, newest first.
func (s *Service) Entries(ctx context.Context, id AccountID, limit int) ([]Entry, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, transfer_id, amount, created_at FROM postings WHERE account_id = $1 ORDER BY id DESC LIMIT $2`, id, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Entry, error) {
		var e Entry
		return e, r.Scan(&e.ID, &e.TransferID, &e.Amount, &e.CreatedAt)
	})
}

func (s *Service) GetTransfer(ctx context.Context, id TransferID) (Transfer, error) {
	var t Transfer
	err := s.pool.QueryRow(ctx,
		`SELECT id, from_account, to_account, amount, currency, reference, created_at FROM transfers WHERE id = $1`, id).
		Scan(&t.ID, &t.From, &t.To, &t.Amount, &t.Currency, &t.Reference, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Transfer{}, ErrTransferNotFound
	}
	return t, err
}

var ErrTransferNotFound = errors.New("transfer not found")

// Transfer moves money exactly once per idempotency key.
//
// The key row is inserted first, in the same transaction as the money movement.
// A concurrent request with the same key blocks on that insert until the first
// commits, then finds the row and replays its outcome. A refusal (insufficient
// funds, unknown account) is also stored, inside a savepoint, so a retry gets
// the same decision. The bool reports whether this call was a replay.
func (s *Service) Transfer(ctx context.Context, key string, req TransferRequest) (Transfer, bool, error) {
	key = strings.TrimSpace(key)
	if key == "" || len(key) > 255 {
		return Transfer{}, false, ErrMissingIdempotencyKey
	}
	if err := req.validate(); err != nil {
		return Transfer{}, false, err
	}
	hash := req.hash()

	for attempt := 0; ; attempt++ {
		t, replayed, err := s.transferOnce(ctx, key, req, hash)
		if err != nil && isRetryable(err) && attempt < 4 {
			continue
		}
		return t, replayed, err
	}
}

func (s *Service) transferOnce(ctx context.Context, key string, req TransferRequest, hash []byte) (Transfer, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Transfer{}, false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	tag, err := tx.Exec(ctx,
		`INSERT INTO idempotency_keys (key, request_hash) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING`, key, hash)
	if err != nil {
		return Transfer{}, false, err
	}
	if tag.RowsAffected() == 0 {
		_ = tx.Rollback(ctx)
		return s.replay(ctx, key, hash)
	}

	sp, err := tx.Begin(ctx) // savepoint: a refusal undoes the movement but keeps the key
	if err != nil {
		return Transfer{}, false, err
	}
	t, moveErr := move(ctx, sp, req)
	var out outcome
	switch code, refusal := codeOf(moveErr); {
	case moveErr == nil:
		if err := sp.Commit(ctx); err != nil {
			return Transfer{}, false, err
		}
		out.Transfer = &t
	case refusal:
		if err := sp.Rollback(ctx); err != nil {
			return Transfer{}, false, err
		}
		out.Error = code
	default:
		return Transfer{}, false, moveErr
	}

	body, _ := json.Marshal(out)
	if _, err := tx.Exec(ctx, `UPDATE idempotency_keys SET outcome = $2 WHERE key = $1`, key, body); err != nil {
		return Transfer{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Transfer{}, false, err
	}
	return t, false, moveErr
}

func (s *Service) replay(ctx context.Context, key string, hash []byte) (Transfer, bool, error) {
	var stored []byte
	var raw []byte
	err := s.pool.QueryRow(ctx, `SELECT request_hash, outcome FROM idempotency_keys WHERE key = $1`, key).Scan(&stored, &raw)
	if err != nil {
		return Transfer{}, false, err
	}
	if !bytes.Equal(stored, hash) {
		return Transfer{}, false, ErrIdempotencyKeyReused
	}
	var out outcome
	if err := json.Unmarshal(raw, &out); err != nil {
		return Transfer{}, false, fmt.Errorf("stored outcome for %q: %w", key, err)
	}
	if out.Error != "" {
		return Transfer{}, true, errorFromCode(out.Error)
	}
	return *out.Transfer, true, nil
}

// move does the double-entry work inside tx: lock both accounts in id order (so two
// opposite transfers cannot deadlock), check them, write the transfer, two postings
// that sum to zero, the new balances, and the outbox event.
func move(ctx context.Context, tx pgx.Tx, req TransferRequest) (Transfer, error) {
	rows, err := tx.Query(ctx,
		`SELECT id, currency, balance, allow_negative FROM accounts WHERE id = ANY($1) ORDER BY id FOR UPDATE`,
		[]AccountID{req.From, req.To})
	if err != nil {
		return Transfer{}, err
	}
	type locked struct {
		currency      string
		balance       int64
		allowNegative bool
	}
	accs := map[AccountID]locked{}
	for rows.Next() {
		var id AccountID
		var l locked
		if err := rows.Scan(&id, &l.currency, &l.balance, &l.allowNegative); err != nil {
			rows.Close()
			return Transfer{}, err
		}
		accs[id] = l
	}
	if err := rows.Err(); err != nil {
		return Transfer{}, err
	}
	from, okFrom := accs[req.From]
	to, okTo := accs[req.To]
	switch {
	case !okFrom || !okTo:
		return Transfer{}, ErrAccountNotFound
	case from.currency != to.currency:
		return Transfer{}, ErrCurrencyMismatch
	case !from.allowNegative && from.balance < req.Amount:
		return Transfer{}, ErrInsufficientFunds
	}

	t := Transfer{ID: NewID(), From: req.From, To: req.To, Amount: req.Amount, Currency: from.currency, Reference: req.Reference}
	if err := tx.QueryRow(ctx,
		`INSERT INTO transfers (id, from_account, to_account, amount, currency, reference) VALUES ($1, $2, $3, $4, $5, $6) RETURNING created_at`,
		t.ID, t.From, t.To, t.Amount, t.Currency, t.Reference).Scan(&t.CreatedAt); err != nil {
		return Transfer{}, err
	}
	batch := &pgx.Batch{}
	batch.Queue(`INSERT INTO postings (transfer_id, account_id, amount) VALUES ($1, $2, $3), ($1, $4, $5)`, t.ID, t.From, -t.Amount, t.To, t.Amount)
	batch.Queue(`UPDATE accounts SET balance = balance - $2 WHERE id = $1`, t.From, t.Amount)
	batch.Queue(`UPDATE accounts SET balance = balance + $2 WHERE id = $1`, t.To, t.Amount)
	event, _ := json.Marshal(map[string]any{"type": "transfer.created", "transfer": t})
	batch.Queue(`INSERT INTO outbox (event_id, topic, key, payload) VALUES ($1, $2, $3, $4)`, NewID(), TopicTransfers, t.ID.String(), event)
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return Transfer{}, err
	}
	return t, nil
}

// isRetryable reports deadlocks and serialization failures, which are safe to retry
// because the whole transaction rolled back.
func isRetryable(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && (pg.Code == "40P01" || pg.Code == "40001")
}
