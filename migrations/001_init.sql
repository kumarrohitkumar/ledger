-- Money is stored in minor units (paise, cents) as BIGINT: no floating point anywhere.

CREATE TABLE accounts (
    id             UUID PRIMARY KEY,
    name           TEXT NOT NULL,
    currency       CHAR(3) NOT NULL,
    -- Only internal accounts (funding, fees) may go below zero.
    allow_negative BOOLEAN NOT NULL DEFAULT FALSE,
    -- A cached sum of this account's postings, updated in the same transaction.
    -- Reconciliation proves it still equals the sum.
    balance        BIGINT NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT no_overdraft CHECK (allow_negative OR balance >= 0)
);

CREATE TABLE transfers (
    id           UUID PRIMARY KEY,
    from_account UUID NOT NULL REFERENCES accounts(id),
    to_account   UUID NOT NULL REFERENCES accounts(id),
    amount       BIGINT NOT NULL CHECK (amount > 0),
    currency     CHAR(3) NOT NULL,
    reference    TEXT NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (from_account <> to_account)
);

-- Double entry: every transfer writes one negative and one positive posting
-- that sum to zero. Postings are append-only.
CREATE TABLE postings (
    id          BIGSERIAL PRIMARY KEY,
    transfer_id UUID NOT NULL REFERENCES transfers(id),
    account_id  UUID NOT NULL REFERENCES accounts(id),
    amount      BIGINT NOT NULL CHECK (amount <> 0),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX postings_account_idx ON postings (account_id, id DESC);
CREATE INDEX postings_transfer_idx ON postings (transfer_id);

-- One row per client request key. It commits together with the transfer (or
-- with the recorded refusal), so a retry can only ever see the final outcome.
CREATE TABLE idempotency_keys (
    key          TEXT PRIMARY KEY,
    request_hash BYTEA NOT NULL,
    outcome      JSONB,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Transactional outbox: events are written in the same transaction as the money
-- movement and published to Kafka afterwards, so neither can happen without the other.
CREATE TABLE outbox (
    id           BIGSERIAL PRIMARY KEY,
    event_id     UUID NOT NULL UNIQUE,
    topic        TEXT NOT NULL,
    key          TEXT NOT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

-- Consumer side: an event's effect and its "processed" mark commit together,
-- so a redelivered event changes nothing the second time.
CREATE TABLE processed_events (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A read model built by the consumer: one statement line per account per transfer.
CREATE TABLE statement_lines (
    id          BIGSERIAL PRIMARY KEY,
    account_id  UUID NOT NULL,
    transfer_id UUID NOT NULL,
    amount      BIGINT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (account_id, transfer_id)
);
