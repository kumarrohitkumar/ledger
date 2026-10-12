package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kumarrohitkumar/ledger/internal/api"
	"github.com/kumarrohitkumar/ledger/internal/ledger"
	"github.com/kumarrohitkumar/ledger/internal/testdb"
)

var pool *pgxpool.Pool

func TestMain(m *testing.M) {
	p, stop, err := testdb.Start(context.Background())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	pool = p
	code := m.Run()
	stop()
	os.Exit(code)
}

type client struct {
	t   *testing.T
	srv *httptest.Server
}

func newClient(t *testing.T) *client {
	testdb.Reset(t, pool)
	srv := httptest.NewServer(api.New(ledger.New(pool)).Routes())
	t.Cleanup(srv.Close)
	return &client{t: t, srv: srv}
}

func (c *client) do(method, path string, body any, headers map[string]string) (int, http.Header, map[string]any) {
	c.t.Helper()
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req, _ := http.NewRequest(method, c.srv.URL+path, &buf)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer res.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(res.Body).Decode(&out)
	return res.StatusCode, res.Header, out
}

func (c *client) account(name string, allowNegative bool) string {
	c.t.Helper()
	code, _, body := c.do("POST", "/v1/accounts", map[string]any{"name": name, "currency": "INR", "allow_negative": allowNegative}, nil)
	if code != http.StatusCreated {
		c.t.Fatalf("create account: %d %v", code, body)
	}
	return body["id"].(string)
}

func (c *client) transfer(key, from, to string, amount int64) (int, http.Header, map[string]any) {
	return c.do("POST", "/v1/transfers", map[string]any{"from": from, "to": to, "amount": amount}, map[string]string{"Idempotency-Key": key})
}

func TestTransferLifecycleOverHTTP(t *testing.T) {
	c := newClient(t)
	bank := c.account("bank", true)
	alice := c.account("alice", false)

	code, hdr, body := c.transfer("fund-1", bank, alice, 50_000)
	if code != http.StatusCreated || hdr.Get("Idempotent-Replayed") != "" {
		t.Fatalf("first transfer: %d %v", code, body)
	}
	id := body["id"].(string)

	code, hdr, body = c.transfer("fund-1", bank, alice, 50_000)
	if code != http.StatusOK || hdr.Get("Idempotent-Replayed") != "true" || body["id"] != id {
		t.Fatalf("replay: %d %s %v", code, hdr.Get("Idempotent-Replayed"), body)
	}

	code, _, body = c.do("GET", "/v1/accounts/"+alice, nil, nil)
	if code != http.StatusOK || body["balance"].(float64) != 50_000 {
		t.Fatalf("alice: %d %v", code, body)
	}
	code, _, body = c.do("GET", "/v1/transfers/"+id, nil, nil)
	if code != http.StatusOK || body["amount"].(float64) != 50_000 {
		t.Fatalf("get transfer: %d %v", code, body)
	}
	code, _, body = c.do("GET", "/v1/accounts/"+alice+"/entries", nil, nil)
	if code != http.StatusOK || len(body["entries"].([]any)) != 1 {
		t.Fatalf("entries: %d %v", code, body)
	}
	code, _, body = c.do("GET", "/v1/reconcile", nil, nil)
	if code != http.StatusOK || body["ok"] != true {
		t.Fatalf("reconcile: %d %v", code, body)
	}
}

func TestErrorsAreClearAndUseTheRightStatus(t *testing.T) {
	c := newClient(t)
	bank := c.account("bank", true)
	alice := c.account("alice", false)
	bob := c.account("bob", false)

	cases := []struct {
		name   string
		code   int
		errKey string
		run    func() (int, http.Header, map[string]any)
	}{
		{"missing key", 400, "missing_idempotency_key", func() (int, http.Header, map[string]any) {
			return c.do("POST", "/v1/transfers", map[string]any{"from": bank, "to": alice, "amount": 1}, nil)
		}},
		{"bad json", 400, "invalid_request", func() (int, http.Header, map[string]any) {
			return c.do("POST", "/v1/transfers", "not an object", map[string]string{"Idempotency-Key": "x"})
		}},
		{"zero amount", 422, "invalid_amount", func() (int, http.Header, map[string]any) { return c.transfer("z", bank, alice, 0) }},
		{"insufficient", 422, "insufficient_funds", func() (int, http.Header, map[string]any) { return c.transfer("i", alice, bob, 1) }},
		{"unknown account", 404, "account_not_found", func() (int, http.Header, map[string]any) {
			return c.transfer("u", bank, "0198f2a0-0000-7000-8000-000000000000", 1)
		}},
		{"malformed id", 404, "not_found", func() (int, http.Header, map[string]any) { return c.do("GET", "/v1/accounts/nope", nil, nil) }},
		{"bad currency", 400, "invalid_account", func() (int, http.Header, map[string]any) {
			return c.do("POST", "/v1/accounts", map[string]any{"name": "x", "currency": "rupees"}, nil)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, _, body := tc.run()
			if code != tc.code || body["error"] != tc.errKey || body["message"] == "" {
				t.Fatalf("got %d %v, want %d %s", code, body, tc.code, tc.errKey)
			}
		})
	}

	c.transfer("k", bank, alice, 10)
	code, _, body := c.transfer("k", bank, alice, 99)
	if code != 422 || body["error"] != "idempotency_key_reused" {
		t.Fatalf("reused key: %d %v", code, body)
	}
}

func TestHealth(t *testing.T) {
	c := newClient(t)
	if code, _, _ := c.do("GET", "/healthz", nil, nil); code != 200 {
		t.Fatalf("healthz %d", code)
	}
}
