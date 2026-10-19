package stockhold_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	stockholdv1 "stockhold/gen/stockhold/v1"
	"stockhold/internal/grpcapi"
	"stockhold/internal/outbox"
	"stockhold/internal/stockhold"
)

func testStore(t *testing.T) *stockhold.Store {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to a dedicated database ending in _test")
	}
	u, err := url.Parse(dsn)
	if err != nil || !strings.HasSuffix(strings.TrimPrefix(u.Path, "/"), "_test") {
		t.Fatal("TEST_DATABASE_URL must point to a dedicated *_test database")
	}
	ctx := context.Background()
	s, err := stockhold.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, "TRUNCATE outbox_events, reservations, stock_receipts, items"); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestReservationLifecycle(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if _, err := s.CreateItem(ctx, "CAM-01", "Camera", 10); err != nil {
		t.Fatal(err)
	}
	r, replay, err := s.Reserve(ctx, "CAM-01", 4, time.Minute, "order-0001", now)
	if err != nil || replay {
		t.Fatalf("reserve: %v, replay=%v", err, replay)
	}
	again, replay, err := s.Reserve(ctx, "cam-01", 4, time.Minute, "order-0001", now)
	if err != nil || !replay || again.ID != r.ID {
		t.Fatalf("replay: %+v, %v", again, err)
	}
	if _, _, err := s.Reserve(ctx, "CAM-01", 5, time.Minute, "order-0001", now); !errors.Is(err, stockhold.ErrConflict) {
		t.Fatalf("key conflict: %v", err)
	}
	item, err := s.GetItem(ctx, "CAM-01")
	if err != nil || item.Available != 6 || item.OnHand != 10 {
		t.Fatalf("after hold: %+v, %v", item, err)
	}
	confirmed, err := s.Transition(ctx, r.ID, "confirm", now)
	if err != nil || confirmed.Status != "confirmed" {
		t.Fatalf("confirm: %+v, %v", confirmed, err)
	}
	if _, err := s.Transition(ctx, r.ID, "confirm", now); err != nil {
		t.Fatalf("repeat confirm: %v", err)
	}
	if _, err := s.Transition(ctx, r.ID, "cancel", now); !errors.Is(err, stockhold.ErrConflict) {
		t.Fatalf("cancel confirmed: %v", err)
	}
	item, _ = s.GetItem(ctx, "CAM-01")
	if item.Available != 6 || item.OnHand != 6 {
		t.Fatalf("after confirm: %+v", item)
	}
	if _, _, err := s.Reserve(ctx, "CAM-01", 7, time.Minute, "order-0002", now); !errors.Is(err, stockhold.ErrInsufficient) {
		t.Fatalf("oversell: %v", err)
	}
	item, replay, err = s.Restock(ctx, "CAM-01", 3, "receipt-0001")
	if err != nil || replay || item.Available != 9 || item.OnHand != 9 {
		t.Fatalf("restock: %+v, replay=%v, %v", item, replay, err)
	}
	_, replay, err = s.Restock(ctx, "CAM-01", 3, "receipt-0001")
	if err != nil || !replay {
		t.Fatalf("restock replay: %v, replay=%v", err, replay)
	}
	if _, _, err := s.Restock(ctx, "CAM-01", 4, "receipt-0001"); !errors.Is(err, stockhold.ErrConflict) {
		t.Fatalf("restock key conflict: %v", err)
	}
	r2, _, err := s.Reserve(ctx, "CAM-01", 2, time.Minute, "order-0003", now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Transition(ctx, r2.ID, "cancel", now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Transition(ctx, r2.ID, "cancel", now); err != nil {
		t.Fatal(err)
	}
	item, _ = s.GetItem(ctx, "CAM-01")
	if item.Available != 9 || item.OnHand != 9 {
		t.Fatalf("after cancel: %+v", item)
	}
}

func TestConcurrentReservationsAndExpiry(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if _, err := s.CreateItem(ctx, "SKU-01", "Widget", 100); err != nil {
		t.Fatal(err)
	}
	const attempts = 40
	var wg sync.WaitGroup
	results := make(chan error, attempts)
	for i := range attempts {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, err := s.Reserve(ctx, "SKU-01", 3, 10*time.Minute, fmt.Sprintf("hold-%08d", i), now)
			results <- err
		}(i)
	}
	wg.Wait()
	close(results)
	accepted, rejected := 0, 0
	for err := range results {
		switch {
		case err == nil:
			accepted++
		case errors.Is(err, stockhold.ErrInsufficient):
			rejected++
		default:
			t.Fatalf("unexpected concurrent error: %v", err)
		}
	}
	if accepted != 33 || rejected != 7 {
		t.Fatalf("accepted=%d rejected=%d", accepted, rejected)
	}
	item, err := s.GetItem(ctx, "SKU-01")
	if err != nil || item.Available != 1 || item.OnHand != 100 {
		t.Fatalf("stock after race: %+v, %v", item, err)
	}
	n, err := s.ExpirePending(ctx, now.Add(11*time.Minute), 100)
	if err != nil || n != 33 {
		t.Fatalf("expired=%d, %v", n, err)
	}
	item, _ = s.GetItem(ctx, "SKU-01")
	if item.Available != 100 || item.OnHand != 100 {
		t.Fatalf("stock after expiry: %+v", item)
	}
}

func TestConcurrentSameKeyAndExpiredConfirm(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if _, err := s.CreateItem(ctx, "SKU-02", "Widget", 3); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	ids := make(chan string, 20)
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, _, err := s.Reserve(ctx, "SKU-02", 2, time.Minute, "shared-key-001", now)
			if err != nil {
				ids <- "ERROR:" + err.Error()
				return
			}
			ids <- r.ID
		}()
	}
	wg.Wait()
	close(ids)
	first := ""
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatalf("different results for same key: %s vs %s", first, id)
		}
	}
	item, _ := s.GetItem(ctx, "SKU-02")
	if item.Available != 1 {
		t.Fatalf("same key reserved more than once: %+v", item)
	}
	if _, err := s.Transition(ctx, first, "confirm", now.Add(2*time.Minute)); !errors.Is(err, stockhold.ErrExpired) {
		t.Fatalf("late confirm: %v", err)
	}
	item, _ = s.GetItem(ctx, "SKU-02")
	if item.Available != 3 {
		t.Fatalf("expired stock not restored: %+v", item)
	}
}

func TestReadExpiryAndMissingResources(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if _, err := s.CreateItem(ctx, "READ-01", "Reader", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateItem(ctx, "READ-01", "Reader", 2); !errors.Is(err, stockhold.ErrExists) {
		t.Fatalf("duplicate item: %v", err)
	}
	r, _, err := s.Reserve(ctx, "READ-01", 1, time.Second, "read-expire-001", now.Add(-2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	current, err := s.GetReservation(ctx, r.ID, now)
	if err != nil || current.Status != "expired" {
		t.Fatalf("expired read: %+v, %v", current, err)
	}
	current, err = s.GetReservation(ctx, r.ID, now)
	if err != nil || current.Status != "expired" {
		t.Fatalf("repeated read: %+v, %v", current, err)
	}
	item, _ := s.GetItem(ctx, "READ-01")
	if item.Available != 2 {
		t.Fatalf("stock not returned: %+v", item)
	}
	if _, err := s.GetReservation(ctx, "00000000-0000-4000-8000-000000000000", now); !errors.Is(err, stockhold.ErrNotFound) {
		t.Fatalf("missing reservation: %v", err)
	}
	if _, err := s.GetItem(ctx, "NONE-01"); !errors.Is(err, stockhold.ErrNotFound) {
		t.Fatalf("missing item: %v", err)
	}
	if _, _, err := s.Restock(ctx, "NONE-01", 1, "missing-restock-001"); !errors.Is(err, stockhold.ErrNotFound) {
		t.Fatalf("missing restock: %v", err)
	}
	if _, _, err := s.Reserve(ctx, "NONE-01", 1, time.Minute, "missing-reserve-001", now); !errors.Is(err, stockhold.ErrNotFound) {
		t.Fatalf("missing reserve: %v", err)
	}
}

func TestConfirmationWaitsPastDeadline(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if _, err := s.CreateItem(ctx, "LOCK-01", "Locked item", 1); err != nil {
		t.Fatal(err)
	}
	r, _, err := s.Reserve(ctx, "LOCK-01", 1, time.Second, "lock-deadline-001", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	lock, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(ctx)
	if _, err := lock.Exec(ctx, `SELECT id FROM reservations WHERE id=$1 FOR UPDATE`, r.ID); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := s.Transition(ctx, r.ID, "confirm", time.Time{}); done <- err }()
	if wait := time.Until(r.ExpiresAt.Add(100 * time.Millisecond)); wait > 0 {
		time.Sleep(wait)
	}
	if err := lock.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, stockhold.ErrExpired) {
			t.Fatalf("confirmation after deadline: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("confirmation remained blocked")
	}
	item, _ := s.GetItem(ctx, "LOCK-01")
	if item.Available != 1 || item.OnHand != 1 {
		t.Fatalf("stock after deadline: %+v", item)
	}
}

func TestGRPCFlowAndValidation(t *testing.T) {
	s := testStore(t)
	listener := bufconn.Listen(1 << 20)
	server := grpcapi.New(s, "local-test-api-key-123", slog.New(slog.NewTextHandler(io.Discard, nil)))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	connection, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client := stockholdv1.NewStockholdServiceClient(connection)
	ctx := context.Background()
	_, err = client.GetItem(ctx, &stockholdv1.GetItemRequest{Sku: "RPC-01"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("missing auth: %v", err)
	}
	health, err := grpc_health_v1.NewHealthClient(connection).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil || health.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("health: %+v, %v", health, err)
	}
	ctx = metadata.AppendToOutgoingContext(ctx, "x-api-key", "local-test-api-key-123")
	item, err := client.CreateItem(ctx, &stockholdv1.CreateItemRequest{Sku: "rpc-01", Name: "Sensor", Quantity: 2})
	if err != nil || item.GetSku() != "RPC-01" {
		t.Fatalf("create: %+v, %v", item, err)
	}
	_, err = client.Reserve(ctx, &stockholdv1.ReserveRequest{Sku: "RPC-01", Quantity: 2, TtlSeconds: 0, IdempotencyKey: "rpc-invalid-001"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid TTL: %v", err)
	}
	_, err = client.Reserve(ctx, &stockholdv1.ReserveRequest{Sku: "RPC-01", Quantity: 3, TtlSeconds: 60, IdempotencyKey: "rpc-overstock-001"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("oversell: %v", err)
	}
	request := &stockholdv1.ReserveRequest{Sku: "RPC-01", Quantity: 2, TtlSeconds: 60, IdempotencyKey: "rpc-order-001"}
	reserved, err := client.Reserve(ctx, request)
	if err != nil || reserved.GetReplayed() {
		t.Fatalf("reserve: %+v, %v", reserved, err)
	}
	again, err := client.Reserve(ctx, request)
	if err != nil || !again.GetReplayed() || again.GetReservation().GetId() != reserved.GetReservation().GetId() {
		t.Fatalf("replay: %+v, %v", again, err)
	}
	id := reserved.GetReservation().GetId()
	read, err := client.GetReservation(ctx, &stockholdv1.GetReservationRequest{Id: id})
	if err != nil || read.GetStatus() != stockholdv1.ReservationStatus_RESERVATION_STATUS_PENDING {
		t.Fatalf("read: %+v, %v", read, err)
	}
	confirmed, err := client.Confirm(ctx, &stockholdv1.ChangeReservationRequest{Id: id})
	if err != nil || confirmed.GetStatus() != stockholdv1.ReservationStatus_RESERVATION_STATUS_CONFIRMED {
		t.Fatalf("confirm: %+v, %v", confirmed, err)
	}
	item, err = client.GetItem(ctx, &stockholdv1.GetItemRequest{Sku: "RPC-01"})
	if err != nil || item.GetOnHand() != 0 || item.GetAvailable() != 0 {
		t.Fatalf("stock after confirm: %+v, %v", item, err)
	}
	restocked, err := client.Restock(ctx, &stockholdv1.RestockRequest{Sku: "RPC-01", Quantity: 3, IdempotencyKey: "rpc-restock-001"})
	if err != nil || restocked.GetReplayed() {
		t.Fatalf("restock: %+v, %v", restocked, err)
	}
	restocked, err = client.Restock(ctx, &stockholdv1.RestockRequest{Sku: "RPC-01", Quantity: 3, IdempotencyKey: "rpc-restock-001"})
	if err != nil || !restocked.GetReplayed() || restocked.GetItem().GetAvailable() != 3 {
		t.Fatalf("restock replay: %+v, %v", restocked, err)
	}
	_, err = client.GetReservation(ctx, &stockholdv1.GetReservationRequest{Id: "bad-id"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid ID: %v", err)
	}
}

func TestOutboxEventsAndLeases(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if _, err := s.CreateItem(ctx, "EVENT-01", "Event item", 5); err != nil {
		t.Fatal(err)
	}
	r, _, err := s.Reserve(ctx, "EVENT-01", 2, time.Minute, "event-order-001", now)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Reserve(ctx, "EVENT-01", 2, time.Minute, "event-order-001", now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Transition(ctx, r.ID, "confirm", now); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Restock(ctx, "EVENT-01", 1, "event-restock-001"); err != nil {
		t.Fatal(err)
	}
	pending, err := s.PendingOutbox(ctx)
	if err != nil || pending != 4 {
		t.Fatalf("pending=%d, %v", pending, err)
	}
	seen := map[string]bool{}
	for sequence := int64(1); sequence <= 4; sequence++ {
		events, err := s.ClaimOutbox(ctx, now.Add(time.Second), 10, 30*time.Second)
		if err != nil || len(events) != 1 {
			t.Fatalf("claimed=%d at sequence %d, %v", len(events), sequence, err)
		}
		event := events[0]
		if event.Sequence != sequence {
			t.Fatalf("sequence=%d want=%d", event.Sequence, sequence)
		}
		if sequence == 1 {
			blocked, err := s.ClaimOutbox(ctx, now.Add(time.Second), 10, 30*time.Second)
			if err != nil || len(blocked) != 0 {
				t.Fatalf("later event bypassed first lease: %d, %v", len(blocked), err)
			}
		}
		var payload map[string]any
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if payload["event_id"] != event.ID || payload["type"] != event.Type || payload["aggregate_sequence"] != float64(sequence) {
			t.Fatalf("bad event payload: %+v", payload)
		}
		seen[event.Type] = true
		if err := s.MarkPublished(ctx, event.ID, event.Token, now.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	for _, eventType := range []string{"stock.item_created", "reservation.created", "reservation.confirmed", "stock.restocked"} {
		if !seen[eventType] {
			t.Fatalf("missing %s event", eventType)
		}
	}
	pending, err = s.PendingOutbox(ctx)
	if err != nil || pending != 0 {
		t.Fatalf("pending after ack=%d, %v", pending, err)
	}
}

type flakyPublisher struct {
	failures int
	ids      []string
}

func (p *flakyPublisher) Publish(_ context.Context, event stockhold.OutboxEvent) error {
	p.ids = append(p.ids, event.ID)
	if p.failures > 0 {
		p.failures--
		return errors.New("broker unavailable")
	}
	return nil
}

func TestOutboxRetryAndLeaseRecovery(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if _, err := s.CreateItem(ctx, "RETRY-01", "Retry item", 2); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Reserve(ctx, "RETRY-01", 1, time.Minute, "retry-order-001", now); err != nil {
		t.Fatal(err)
	}
	publisher := &flakyPublisher{failures: 1}
	dispatcher := &outbox.Dispatcher{Store: s, Publisher: publisher, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	n, err := dispatcher.DispatchOnce(ctx)
	if err != nil || n != 1 {
		t.Fatalf("failed dispatch: n=%d err=%v", n, err)
	}
	n, err = dispatcher.DispatchOnce(ctx)
	if err != nil || n != 0 {
		t.Fatalf("retry ignored delay: n=%d err=%v", n, err)
	}
	pool, err := pgxpool.New(ctx, os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `UPDATE outbox_events SET available_at=now()-interval '1 second' WHERE published_at IS NULL`); err != nil {
		t.Fatal(err)
	}
	n, err = dispatcher.DispatchOnce(ctx)
	if err != nil || n != 1 {
		t.Fatalf("retried dispatch: n=%d err=%v", n, err)
	}
	if len(publisher.ids) != 2 || publisher.ids[0] != publisher.ids[1] {
		t.Fatalf("event ID changed across retry: %+v", publisher.ids)
	}
	n, err = dispatcher.DispatchOnce(ctx)
	if err != nil || n != 1 {
		t.Fatalf("next event did not follow retry: n=%d err=%v", n, err)
	}
	pending, err := s.PendingOutbox(ctx)
	if err != nil || pending != 0 {
		t.Fatalf("pending=%d, %v", pending, err)
	}
	if _, _, err := s.Reserve(ctx, "RETRY-01", 1, time.Minute, "retry-order-002", now); err != nil {
		t.Fatal(err)
	}
	first, err := s.ClaimOutbox(ctx, now.Add(time.Second), 1, time.Second)
	if err != nil || len(first) != 1 {
		t.Fatalf("first lease: %d, %v", len(first), err)
	}
	second, err := s.ClaimOutbox(ctx, now.Add(3*time.Second), 1, time.Second)
	if err != nil || len(second) != 1 || second[0].ID != first[0].ID {
		t.Fatalf("reclaimed lease: %+v, %v", second, err)
	}
	if err := s.MarkPublished(ctx, first[0].ID, first[0].Token, now.Add(3*time.Second)); !errors.Is(err, stockhold.ErrConflict) {
		t.Fatalf("old lease accepted: %v", err)
	}
	if err := s.MarkPublished(ctx, second[0].ID, second[0].Token, now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
}

func TestKafkaDelivery(t *testing.T) {
	brokers := os.Getenv("TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("set TEST_KAFKA_BROKERS to verify delivery through a Kafka-compatible broker")
	}
	s := testStore(t)
	ctx := context.Background()
	sku := fmt.Sprintf("KAFKA-%d", time.Now().UnixNano())
	if _, err := s.CreateItem(ctx, sku, "Broker test item", 4); err != nil {
		t.Fatal(err)
	}
	publisher := outbox.NewKafkaPublisher(brokers)
	defer publisher.Close()
	dispatcher := &outbox.Dispatcher{Store: s, Publisher: publisher, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if n, err := dispatcher.DispatchOnce(ctx); err != nil || n != 1 {
		t.Fatalf("dispatch: n=%d err=%v", n, err)
	}
	if pending, err := s.PendingOutbox(ctx); err != nil || pending != 0 {
		t.Fatalf("pending after broker acknowledgement: %d, %v", pending, err)
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: strings.Split(brokers, ","), Topic: stockhold.EventsTopic,
		GroupID:     fmt.Sprintf("stockhold-test-%d", time.Now().UnixNano()),
		StartOffset: kafka.FirstOffset, MinBytes: 1, MaxBytes: 1 << 20,
	})
	defer reader.Close()
	readCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for {
		message, err := reader.ReadMessage(readCtx)
		if err != nil {
			t.Fatalf("read published event: %v", err)
		}
		if string(message.Key) != sku {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal(message.Value, &payload); err != nil {
			t.Fatal(err)
		}
		if payload["type"] != "stock.item_created" || payload["sku"] != sku || payload["aggregate_sequence"] != float64(1) {
			t.Fatalf("unexpected broker payload: %+v", payload)
		}
		if payload["event_id"] == "" {
			t.Fatal("event ID missing from broker payload")
		}
		return
	}
}
