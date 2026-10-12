// Package api exposes the ledger over JSON/HTTP using only the standard library.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/kumarrohitkumar/ledger/internal/ledger"
)

type API struct {
	svc *ledger.Service
}

func New(svc *ledger.Service) *API { return &API{svc: svc} }

func (a *API) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("POST /v1/accounts", a.createAccount)
	mux.HandleFunc("GET /v1/accounts/{id}", a.getAccount)
	mux.HandleFunc("GET /v1/accounts/{id}/entries", a.entries)
	mux.HandleFunc("POST /v1/transfers", a.createTransfer)
	mux.HandleFunc("GET /v1/transfers/{id}", a.getTransfer)
	mux.HandleFunc("GET /v1/reconcile", a.reconcile)
	return withTimeout(mux, 10*time.Second)
}

func (a *API) createAccount(w http.ResponseWriter, r *http.Request) {
	var in ledger.NewAccount
	if !decode(w, r, &in) {
		return
	}
	acc, err := a.svc.CreateAccount(r.Context(), in)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, acc)
}

func (a *API) getAccount(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	acc, err := a.svc.GetAccount(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, acc)
}

func (a *API) entries(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if _, err := a.svc.GetAccount(r.Context(), id); err != nil {
		writeError(w, err)
		return
	}
	list, err := a.svc.Entries(r.Context(), id, limit)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": list})
}

// createTransfer answers 201 for a new transfer and 200 with Idempotent-Replayed: true
// when the key was seen before, so a client can tell a retry from a fresh charge.
func (a *API) createTransfer(w http.ResponseWriter, r *http.Request) {
	var in ledger.TransferRequest
	if !decode(w, r, &in) {
		return
	}
	t, replayed, err := a.svc.Transfer(r.Context(), r.Header.Get("Idempotency-Key"), in)
	if replayed {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	if err != nil {
		writeError(w, err)
		return
	}
	status := http.StatusCreated
	if replayed {
		status = http.StatusOK
	}
	writeJSON(w, status, t)
}

func (a *API) getTransfer(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	t, err := a.svc.GetTransfer(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (a *API) reconcile(w http.ResponseWriter, r *http.Request) {
	rep, err := a.svc.Reconcile(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": rep.OK(), "report": rep})
}

// ---------------------------------------------------------------- helpers

var errNotFound = errors.New("not found")

var errorStatus = []struct {
	err    error
	status int
	code   string
}{
	{ledger.ErrMissingIdempotencyKey, 400, "missing_idempotency_key"},
	{ledger.ErrInvalidAccount, 400, "invalid_account"},
	{ledger.ErrIdempotencyKeyReused, 422, "idempotency_key_reused"},
	{ledger.ErrInvalidAmount, 422, "invalid_amount"},
	{ledger.ErrInvalidReference, 422, "invalid_reference"},
	{ledger.ErrSameAccount, 422, "same_account"},
	{ledger.ErrCurrencyMismatch, 422, "currency_mismatch"},
	{ledger.ErrInsufficientFunds, 422, "insufficient_funds"},
	{ledger.ErrAccountNotFound, 404, "account_not_found"},
	{ledger.ErrTransferNotFound, 404, "transfer_not_found"},
	{errNotFound, 404, "not_found"},
}

func writeError(w http.ResponseWriter, err error) {
	for _, e := range errorStatus {
		if errors.Is(err, e.err) {
			writeJSON(w, e.status, map[string]string{"error": e.code, "message": e.err.Error()})
			return
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "timeout", "message": "the request took too long; retry with the same Idempotency-Key"})
		return
	}
	slog.Error("request failed", "err", err)
	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal", "message": "something went wrong; retrying with the same Idempotency-Key is safe"})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_request", "message": "request body must be a JSON object with the documented fields"})
		return false
	}
	return true
}

func pathID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, errNotFound)
		return uuid.Nil, false
	}
	return id, true
}

func withTimeout(h http.Handler, d time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), d)
		defer cancel()
		h.ServeHTTP(w, r.WithContext(ctx))
	})
}
