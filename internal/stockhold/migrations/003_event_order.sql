ALTER TABLE items ADD COLUMN IF NOT EXISTS event_seq bigint NOT NULL DEFAULT 0;
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS aggregate_seq bigint;

WITH numbered AS (
    SELECT id, row_number() OVER (PARTITION BY aggregate_key ORDER BY created_at, id) AS seq
    FROM outbox_events
)
UPDATE outbox_events AS o
SET aggregate_seq = numbered.seq
FROM numbered
WHERE o.id = numbered.id AND o.aggregate_seq IS NULL;

UPDATE outbox_events
SET payload = jsonb_set(payload, '{aggregate_sequence}', to_jsonb(aggregate_seq))
WHERE NOT (payload ? 'aggregate_sequence');

UPDATE items AS i
SET event_seq = GREATEST(i.event_seq, COALESCE((
    SELECT max(e.aggregate_seq) FROM outbox_events AS e WHERE e.aggregate_key = i.sku
), 0));

ALTER TABLE outbox_events ALTER COLUMN aggregate_seq SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS outbox_aggregate_seq_idx
    ON outbox_events (aggregate_key, aggregate_seq);
