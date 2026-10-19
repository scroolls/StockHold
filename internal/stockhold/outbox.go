package stockhold

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const EventsTopic = "stockhold.events.v1"

type OutboxEvent struct {
	ID       string
	Topic    string
	Key      string
	Type     string
	Sequence int64
	Payload  []byte
	Attempts int
	Token    string
}

type eventEnvelope struct {
	SchemaVersion int       `json:"schema_version"`
	EventID       string    `json:"event_id"`
	Type          string    `json:"type"`
	Sequence      int64     `json:"aggregate_sequence"`
	ReservationID string    `json:"reservation_id,omitempty"`
	SKU           string    `json:"sku"`
	Quantity      int64     `json:"quantity"`
	Status        string    `json:"status,omitempty"`
	OccurredAt    time.Time `json:"occurred_at"`
}

func appendEvent(ctx context.Context, tx pgx.Tx, eventType, reservationID, sku string, quantity int64, state string, at time.Time) error {
	var sequence int64
	if err := tx.QueryRow(ctx, `UPDATE items SET event_seq=event_seq+1 WHERE sku=$1 RETURNING event_seq`, sku).Scan(&sequence); err != nil {
		return err
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return err
	}
	id := newID(random)
	payload, err := json.Marshal(eventEnvelope{
		SchemaVersion: 1, EventID: id, Type: eventType, Sequence: sequence, ReservationID: reservationID,
		SKU: sku, Quantity: quantity, Status: state, OccurredAt: at,
	})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox_events
		(id, topic, aggregate_key, aggregate_seq, event_type, payload, created_at)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)`, id, EventsTopic, sku, sequence, eventType, string(payload), at)
	return err
}

// ClaimOutbox leases due events so multiple dispatchers can work concurrently.
func (s *Store) ClaimOutbox(ctx context.Context, now time.Time, limit int, lease time.Duration) ([]OutboxEvent, error) {
	if limit < 1 || limit > 100 || lease < time.Second || lease > time.Minute {
		return nil, ErrInvalid
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, err
	}
	token := newID(random)
	rows, err := s.pool.Query(ctx, `WITH due AS (
		SELECT e.id FROM outbox_events AS e
		WHERE e.published_at IS NULL AND e.available_at <= $1
			AND (e.locked_until IS NULL OR e.locked_until < $1)
			AND NOT EXISTS (
				SELECT 1 FROM outbox_events AS older
				WHERE older.aggregate_key=e.aggregate_key AND older.aggregate_seq<e.aggregate_seq
					AND older.published_at IS NULL
			)
		ORDER BY e.created_at, e.id LIMIT $2 FOR UPDATE OF e SKIP LOCKED
	)
	UPDATE outbox_events AS o
	SET lock_token=$3, locked_until=$4, attempts=attempts+1
	FROM due WHERE o.id=due.id
	RETURNING o.id, o.topic, o.aggregate_key, o.event_type, o.aggregate_seq, o.payload, o.attempts`,
		now, limit, token, now.Add(lease))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := make([]OutboxEvent, 0, limit)
	for rows.Next() {
		var event OutboxEvent
		if err := rows.Scan(&event.ID, &event.Topic, &event.Key, &event.Type, &event.Sequence, &event.Payload, &event.Attempts); err != nil {
			return nil, err
		}
		event.Token = token
		events = append(events, event)
	}
	return events, rows.Err()
}

func (s *Store) MarkPublished(ctx context.Context, id, token string, now time.Time) error {
	tag, err := s.pool.Exec(ctx, `UPDATE outbox_events
		SET published_at=$3, locked_until=NULL, lock_token=NULL, last_error=NULL
		WHERE id=$1 AND lock_token=$2 AND published_at IS NULL`, id, token, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: outbox lease lost", ErrConflict)
	}
	return nil
}

func (s *Store) MarkFailed(ctx context.Context, id, token string, next time.Time, cause error) error {
	message := cause.Error()
	if len(message) > 500 {
		message = message[:500]
	}
	tag, err := s.pool.Exec(ctx, `UPDATE outbox_events
		SET available_at=$3, locked_until=NULL, lock_token=NULL, last_error=$4
		WHERE id=$1 AND lock_token=$2 AND published_at IS NULL`, id, token, next, message)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: outbox lease lost", ErrConflict)
	}
	return nil
}

func (s *Store) PendingOutbox(ctx context.Context) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`).Scan(&n)
	return n, err
}
