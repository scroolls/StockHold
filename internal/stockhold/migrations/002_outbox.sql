CREATE TABLE IF NOT EXISTS outbox_events (
    id text PRIMARY KEY,
    topic text NOT NULL,
    aggregate_key text NOT NULL,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    available_at timestamptz NOT NULL DEFAULT now(),
    locked_until timestamptz,
    lock_token text,
    attempts integer NOT NULL DEFAULT 0,
    last_error text
);

CREATE INDEX IF NOT EXISTS outbox_due_idx
    ON outbox_events (available_at, created_at)
    WHERE published_at IS NULL;
