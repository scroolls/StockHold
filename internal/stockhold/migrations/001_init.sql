CREATE TABLE IF NOT EXISTS items (
    sku text PRIMARY KEY,
    name text NOT NULL CHECK (length(name) BETWEEN 1 AND 120),
    on_hand bigint NOT NULL CHECK (on_hand >= 0),
    available bigint NOT NULL CHECK (available >= 0 AND available <= on_hand),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS reservations (
    id text PRIMARY KEY,
    idempotency_key text NOT NULL UNIQUE,
    request_hash bytea NOT NULL,
    sku text NOT NULL REFERENCES items(sku),
    quantity bigint NOT NULL CHECK (quantity > 0),
    status text NOT NULL CHECK (status IN ('pending', 'confirmed', 'cancelled', 'expired')),
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS reservations_expiry_idx
    ON reservations (expires_at) WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS stock_receipts (
    idempotency_key text PRIMARY KEY,
    request_hash bytea NOT NULL,
    sku text NOT NULL REFERENCES items(sku),
    quantity bigint NOT NULL CHECK (quantity > 0),
    created_at timestamptz NOT NULL DEFAULT now()
);
