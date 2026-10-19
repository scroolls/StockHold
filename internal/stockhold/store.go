package stockhold

import (
	"bytes"
	"context"
	"crypto/rand"
	"embed"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migration embed.FS

type Store struct{ pool *pgxpool.Pool }

func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// Migrate serializes startup migrations across service instances.
func (s *Store) Migrate(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock(738254911)"); err != nil {
		return err
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = conn.Exec(unlockCtx, "SELECT pg_advisory_unlock(738254911)")
	}()
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version integer PRIMARY KEY,
		applied_at timestamptz NOT NULL DEFAULT now()
	)`); err != nil {
		return err
	}
	for _, step := range []struct {
		version int
		file    string
	}{
		{1, "migrations/001_init.sql"},
		{2, "migrations/002_outbox.sql"},
		{3, "migrations/003_event_order.sql"},
	} {
		var applied bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version=$1)`, step.version).Scan(&applied); err != nil {
			return err
		}
		if applied {
			continue
		}
		sql, err := migration.ReadFile(step.file)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(sql), pgx.QueryExecModeSimpleProtocol); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, step.version); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) CreateItem(ctx context.Context, sku, name string, quantity int64) (Item, error) {
	var item Item
	var err error
	if sku, err = NormalizeSKU(sku); err != nil {
		return item, err
	}
	name = strings.TrimSpace(name)
	if len(name) < 1 || len(name) > 120 {
		return item, fmt.Errorf("%w: name must be 1-120 bytes", ErrInvalid)
	}
	if err = ValidateQuantity(quantity); err != nil {
		return item, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Item{}, err
	}
	defer tx.Rollback(ctx)
	err = tx.QueryRow(ctx, `INSERT INTO items (sku, name, on_hand, available)
		VALUES ($1, $2, $3, $3)
		RETURNING sku, name, on_hand, available, created_at`, sku, name, quantity).
		Scan(&item.SKU, &item.Name, &item.OnHand, &item.Available, &item.CreatedAt)
	if isUnique(err) {
		return Item{}, ErrExists
	}
	if err != nil {
		return Item{}, err
	}
	if err := appendEvent(ctx, tx, "stock.item_created", "", sku, quantity, "", item.CreatedAt); err != nil {
		return Item{}, err
	}
	return item, tx.Commit(ctx)
}

func (s *Store) GetItem(ctx context.Context, sku string) (Item, error) {
	var item Item
	var err error
	if sku, err = NormalizeSKU(sku); err != nil {
		return item, err
	}
	err = s.pool.QueryRow(ctx, `SELECT sku, name, on_hand, available, created_at
		FROM items WHERE sku=$1`, sku).
		Scan(&item.SKU, &item.Name, &item.OnHand, &item.Available, &item.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Item{}, ErrNotFound
	}
	return item, err
}

func (s *Store) Restock(ctx context.Context, sku string, quantity int64, key string) (Item, bool, error) {
	var err error
	if sku, err = NormalizeSKU(sku); err != nil {
		return Item{}, false, err
	}
	if err = ValidateQuantity(quantity); err != nil {
		return Item{}, false, err
	}
	if err = ValidateKey(key); err != nil {
		return Item{}, false, err
	}
	hash := requestHash("restock", sku, quantity)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Item{}, false, err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `INSERT INTO stock_receipts (idempotency_key, request_hash, sku, quantity)
		VALUES ($1, $2, $3, $4)`, key, hash, sku, quantity)
	if isUnique(err) {
		_ = tx.Rollback(ctx)
		var previous []byte
		err = s.pool.QueryRow(ctx, `SELECT request_hash FROM stock_receipts WHERE idempotency_key=$1`, key).Scan(&previous)
		if err != nil {
			return Item{}, false, err
		}
		if !bytes.Equal(previous, hash) {
			return Item{}, false, ErrConflict
		}
		item, err := s.GetItem(ctx, sku)
		return item, true, err
	}
	if isForeignKey(err) {
		return Item{}, false, ErrNotFound
	}
	if err != nil {
		return Item{}, false, err
	}
	var item Item
	err = tx.QueryRow(ctx, `UPDATE items SET on_hand=on_hand+$2, available=available+$2
		WHERE sku=$1 AND on_hand <= $3::bigint-$2::bigint AND available <= $3::bigint-$2::bigint
		RETURNING sku, name, on_hand, available, created_at`, sku, quantity, int64(math.MaxInt64)).
		Scan(&item.SKU, &item.Name, &item.OnHand, &item.Available, &item.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Item{}, false, fmt.Errorf("%w: stock quantity overflow", ErrConflict)
	}
	if err != nil {
		return Item{}, false, err
	}
	if err := appendEvent(ctx, tx, "stock.restocked", "", sku, quantity, "", time.Now().UTC()); err != nil {
		return Item{}, false, err
	}
	return item, false, tx.Commit(ctx)
}

func (s *Store) Reserve(ctx context.Context, sku string, quantity int64, ttl time.Duration, key string, now time.Time) (Reservation, bool, error) {
	var err error
	if sku, err = NormalizeSKU(sku); err != nil {
		return Reservation{}, false, err
	}
	if err = ValidateQuantity(quantity); err != nil {
		return Reservation{}, false, err
	}
	if ttl < time.Second || ttl > 15*time.Minute || ttl%time.Second != 0 {
		return Reservation{}, false, fmt.Errorf("%w: ttl must be 1-900 whole seconds", ErrInvalid)
	}
	if err = ValidateKey(key); err != nil {
		return Reservation{}, false, err
	}
	hash := requestHash("reserve", sku, quantity, int64(ttl/time.Second))
	var random [16]byte
	if _, err = rand.Read(random[:]); err != nil {
		return Reservation{}, false, err
	}
	r := Reservation{ID: newID(random), SKU: sku, Quantity: quantity, Status: "pending", ExpiresAt: now.Add(ttl)}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Reservation{}, false, err
	}
	defer tx.Rollback(ctx)
	err = tx.QueryRow(ctx, `INSERT INTO reservations
		(id, idempotency_key, request_hash, sku, quantity, status, expires_at)
		VALUES ($1, $2, $3, $4, $5, 'pending', $6)
		RETURNING created_at, updated_at`, r.ID, key, hash, sku, quantity, r.ExpiresAt).
		Scan(&r.CreatedAt, &r.UpdatedAt)
	if isUnique(err) {
		_ = tx.Rollback(ctx)
		var previous []byte
		err = s.pool.QueryRow(ctx, `SELECT request_hash, id, sku, quantity, status,
			expires_at, created_at, updated_at FROM reservations WHERE idempotency_key=$1`, key).
			Scan(&previous, &r.ID, &r.SKU, &r.Quantity, &r.Status, &r.ExpiresAt, &r.CreatedAt, &r.UpdatedAt)
		if err != nil {
			return Reservation{}, false, err
		}
		if !bytes.Equal(previous, hash) {
			return Reservation{}, false, ErrConflict
		}
		return r, true, nil
	}
	if isForeignKey(err) {
		return Reservation{}, false, ErrNotFound
	}
	if err != nil {
		return Reservation{}, false, err
	}
	tag, err := tx.Exec(ctx, `UPDATE items SET available=available-$2
		WHERE sku=$1 AND available >= $2`, sku, quantity)
	if err != nil {
		return Reservation{}, false, err
	}
	if tag.RowsAffected() == 0 {
		return Reservation{}, false, ErrInsufficient
	}
	if err := appendEvent(ctx, tx, "reservation.created", r.ID, sku, quantity, "pending", now); err != nil {
		return Reservation{}, false, err
	}
	return r, false, tx.Commit(ctx)
}

func (s *Store) GetReservation(ctx context.Context, id string, now time.Time) (Reservation, error) {
	r, err := scanReservation(s.pool.QueryRow(ctx, `SELECT id, sku, quantity, status,
		expires_at, created_at, updated_at FROM reservations WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Reservation{}, ErrNotFound
	}
	if err != nil {
		return Reservation{}, err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if r.Status == "pending" && !now.Before(r.ExpiresAt) {
		return s.Transition(ctx, id, "expire", time.Time{})
	}
	return r, nil
}

func (s *Store) Transition(ctx context.Context, id, action string, now time.Time) (Reservation, error) {
	if action != "confirm" && action != "cancel" && action != "expire" {
		return Reservation{}, ErrInvalid
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Reservation{}, err
	}
	defer tx.Rollback(ctx)
	r, err := scanReservation(tx.QueryRow(ctx, `SELECT id, sku, quantity, status,
		expires_at, created_at, updated_at FROM reservations WHERE id=$1 FOR UPDATE`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Reservation{}, ErrNotFound
	}
	if err != nil {
		return Reservation{}, err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	wanted := map[string]string{"confirm": "confirmed", "cancel": "cancelled", "expire": "expired"}[action]
	if r.Status == wanted {
		return r, tx.Commit(ctx)
	}
	if r.Status != "pending" {
		if action == "expire" {
			return r, tx.Commit(ctx)
		}
		return r, ErrConflict
	}
	if !now.Before(r.ExpiresAt) {
		wanted = "expired"
	} else if action == "expire" {
		return r, tx.Commit(ctx)
	}
	if wanted == "confirmed" {
		_, err = tx.Exec(ctx, `UPDATE items SET on_hand=on_hand-$2 WHERE sku=$1`, r.SKU, r.Quantity)
	} else {
		_, err = tx.Exec(ctx, `UPDATE items SET available=available+$2 WHERE sku=$1`, r.SKU, r.Quantity)
	}
	if err != nil {
		return Reservation{}, err
	}
	r.Status = wanted
	r.UpdatedAt = now
	_, err = tx.Exec(ctx, `UPDATE reservations SET status=$2, updated_at=$3 WHERE id=$1`, id, wanted, now)
	if err != nil {
		return Reservation{}, err
	}
	if err := appendEvent(ctx, tx, "reservation."+wanted, r.ID, r.SKU, r.Quantity, wanted, now); err != nil {
		return Reservation{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Reservation{}, err
	}
	if wanted == "expired" && action != "expire" {
		return r, ErrExpired
	}
	return r, nil
}

func (s *Store) ExpirePending(ctx context.Context, now time.Time, limit int) (int, error) {
	if limit < 1 || limit > 1000 {
		return 0, ErrInvalid
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT id, sku, quantity FROM reservations
		WHERE status='pending' AND expires_at <= $1
		ORDER BY expires_at LIMIT $2 FOR UPDATE SKIP LOCKED`, now, limit)
	if err != nil {
		return 0, err
	}
	type expired struct {
		id, sku  string
		quantity int64
	}
	var batch []expired
	for rows.Next() {
		var e expired
		if err := rows.Scan(&e.id, &e.sku, &e.quantity); err != nil {
			rows.Close()
			return 0, err
		}
		batch = append(batch, e)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	for _, e := range batch {
		if _, err := tx.Exec(ctx, `UPDATE reservations SET status='expired', updated_at=$2 WHERE id=$1`, e.id, now); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(ctx, `UPDATE items SET available=available+$2 WHERE sku=$1`, e.sku, e.quantity); err != nil {
			return 0, err
		}
		if err := appendEvent(ctx, tx, "reservation.expired", e.id, e.sku, e.quantity, "expired", now); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(batch), nil
}

type scanner interface{ Scan(...any) error }

func scanReservation(row scanner) (Reservation, error) {
	var r Reservation
	err := row.Scan(&r.ID, &r.SKU, &r.Quantity, &r.Status, &r.ExpiresAt, &r.CreatedAt, &r.UpdatedAt)
	return r, err
}

func isUnique(err error) bool {
	var pgerr *pgconn.PgError
	return errors.As(err, &pgerr) && pgerr.Code == "23505"
}

func isForeignKey(err error) bool {
	var pgerr *pgconn.PgError
	return errors.As(err, &pgerr) && pgerr.Code == "23503"
}
