// Package ledger is a double-entry ledger with idempotent transfers and a
// transactional outbox. Amounts are int64 minor units (paise, cents).
package ledger

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

type (
	AccountID  = uuid.UUID
	TransferID = uuid.UUID
)

// NewID returns a time-ordered UUID (v7), which keeps primary-key inserts index-friendly.
func NewID() uuid.UUID { return uuid.Must(uuid.NewV7()) }

type Account struct {
	ID            AccountID `json:"id"`
	Name          string    `json:"name"`
	Currency      string    `json:"currency"`
	AllowNegative bool      `json:"allow_negative"`
	Balance       int64     `json:"balance"`
	CreatedAt     time.Time `json:"created_at"`
}

type NewAccount struct {
	Name          string `json:"name"`
	Currency      string `json:"currency"`
	AllowNegative bool   `json:"allow_negative"`
}

type TransferRequest struct {
	From      AccountID `json:"from"`
	To        AccountID `json:"to"`
	Amount    int64     `json:"amount"`
	Reference string    `json:"reference,omitempty"`
}

type Transfer struct {
	ID        TransferID `json:"id"`
	From      AccountID  `json:"from"`
	To        AccountID  `json:"to"`
	Amount    int64      `json:"amount"`
	Currency  string     `json:"currency"`
	Reference string     `json:"reference"`
	CreatedAt time.Time  `json:"created_at"`
}

// Entry is one side of a transfer as seen from one account: negative out, positive in.
type Entry struct {
	ID         int64      `json:"id"`
	TransferID TransferID `json:"transfer_id"`
	Amount     int64      `json:"amount"`
	CreatedAt  time.Time  `json:"created_at"`
}

// hash fingerprints a request so a reused idempotency key with a different body is caught.
func (r TransferRequest) hash() []byte {
	b, _ := json.Marshal(r)
	sum := sha256.Sum256(b)
	return sum[:]
}

func (r TransferRequest) validate() error {
	switch {
	case r.Amount <= 0:
		return ErrInvalidAmount
	case r.From == r.To:
		return ErrSameAccount
	case len(r.Reference) > 200:
		return ErrInvalidReference
	}
	return nil
}

// Errors a caller can act on. Each has a stable code so it can be stored with an
// idempotency key and replayed exactly.
var (
	ErrMissingIdempotencyKey = errors.New("an Idempotency-Key is required")
	ErrIdempotencyKeyReused  = errors.New("this Idempotency-Key was already used for a different request")
	ErrInvalidAmount         = errors.New("amount must be a positive number of minor units")
	ErrInvalidReference      = errors.New("reference must be at most 200 characters")
	ErrSameAccount           = errors.New("cannot transfer to the same account")
	ErrAccountNotFound       = errors.New("account not found")
	ErrCurrencyMismatch      = errors.New("both accounts must use the same currency")
	ErrInsufficientFunds     = errors.New("insufficient funds")
	ErrInvalidAccount        = errors.New("name is required and currency must be a 3-letter ISO code")
)

var errorCodes = map[error]string{
	ErrAccountNotFound:   "account_not_found",
	ErrCurrencyMismatch:  "currency_mismatch",
	ErrInsufficientFunds: "insufficient_funds",
}

func codeOf(err error) (string, bool) {
	for e, code := range errorCodes {
		if errors.Is(err, e) {
			return code, true
		}
	}
	return "", false
}

func errorFromCode(code string) error {
	for e, c := range errorCodes {
		if c == code {
			return e
		}
	}
	return errors.New(code)
}

// outcome is what an idempotency key remembers: the transfer, or why it was refused.
type outcome struct {
	Transfer *Transfer `json:"transfer,omitempty"`
	Error    string    `json:"error,omitempty"`
}
