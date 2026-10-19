package outbox

import (
	"context"
	"log/slog"
	"time"

	"stockhold/internal/stockhold"
)

type Publisher interface {
	Publish(context.Context, stockhold.OutboxEvent) error
}

type Dispatcher struct {
	Store     *stockhold.Store
	Publisher Publisher
	Log       *slog.Logger
	Interval  time.Duration
}

func (d *Dispatcher) Run(ctx context.Context) {
	interval := d.Interval
	if interval == 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		for range 100 {
			n, err := d.DispatchOnce(ctx)
			if err != nil {
				if ctx.Err() == nil {
					d.Log.Error("outbox dispatch failed", "error", err)
				}
				break
			}
			if n == 0 {
				break
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// DispatchOnce claims one event. The lease is longer than the publish timeout.
func (d *Dispatcher) DispatchOnce(ctx context.Context) (int, error) {
	events, err := d.Store.ClaimOutbox(ctx, time.Now().UTC(), 1, 30*time.Second)
	if err != nil || len(events) == 0 {
		return 0, err
	}
	event := events[0]
	publishCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err = d.Publisher.Publish(publishCtx, event)
	cancel()
	if err != nil {
		next := time.Now().UTC().Add(retryDelay(event.Attempts))
		if markErr := d.Store.MarkFailed(ctx, event.ID, event.Token, next, err); markErr != nil {
			return 1, markErr
		}
		d.Log.Warn("event publication deferred", "event_id", event.ID, "attempt", event.Attempts, "error", err)
		return 1, nil
	}
	if err := d.Store.MarkPublished(ctx, event.ID, event.Token, time.Now().UTC()); err != nil {
		return 1, err
	}
	d.Log.Info("event published", "event_id", event.ID, "type", event.Type)
	return 1, nil
}

func retryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 6 {
		return time.Minute
	}
	return time.Second << (attempt - 1)
}
