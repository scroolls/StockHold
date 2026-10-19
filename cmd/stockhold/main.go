package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"stockhold/internal/grpcapi"
	"stockhold/internal/outbox"
	"stockhold/internal/stockhold"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("service stopped", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	dsn := os.Getenv("DATABASE_URL")
	key := os.Getenv("API_KEY")
	if dsn == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	if len(key) < 16 {
		return fmt.Errorf("API_KEY must contain at least 16 characters")
	}
	addr := os.Getenv("GRPC_LISTEN_ADDR")
	if addr == "" {
		addr = "127.0.0.1:9090"
	}
	sweepInterval, err := readInterval("SWEEP_INTERVAL", 2*time.Second)
	if err != nil {
		return err
	}
	outboxInterval, err := readInterval("OUTBOX_INTERVAL", time.Second)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	store, err := stockhold.Open(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer listener.Close()
	var options []grpc.ServerOption
	certFile, keyFile := os.Getenv("TLS_CERT_FILE"), os.Getenv("TLS_KEY_FILE")
	if (certFile == "") != (keyFile == "") {
		return fmt.Errorf("TLS_CERT_FILE and TLS_KEY_FILE must be set together")
	}
	if certFile != "" {
		creds, err := credentials.NewServerTLSFromFile(certFile, keyFile)
		if err != nil {
			return fmt.Errorf("load TLS certificate: %w", err)
		}
		options = append(options, grpc.Creds(creds))
	}
	server := grpcapi.New(store, key, log, options...)
	go sweep(ctx, store, sweepInterval, log)
	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		publisher := outbox.NewKafkaPublisher(brokers)
		defer publisher.Close()
		dispatcher := &outbox.Dispatcher{Store: store, Publisher: publisher, Log: log, Interval: outboxInterval}
		go dispatcher.Run(ctx)
	} else {
		log.Warn("Kafka publisher disabled; events remain in the outbox")
	}
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(listener) }()
	log.Info("service started", "grpc_addr", addr, "sweep_interval", sweepInterval.String())
	select {
	case err := <-serveResult:
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			stop()
			server.Stop()
			return err
		}
	case <-ctx.Done():
	}
	stopped := make(chan struct{})
	go func() { server.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		server.Stop()
		<-stopped
	}
	log.Info("service stopped")
	return nil
}

func readInterval(name string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	interval, err := time.ParseDuration(raw)
	if err != nil || interval < time.Second || interval > time.Hour {
		return 0, fmt.Errorf("%s must be between 1s and 1h", name)
	}
	return interval, nil
}

func sweep(ctx context.Context, store *stockhold.Store, interval time.Duration, log *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for {
				n, err := store.ExpirePending(ctx, time.Now().UTC(), 100)
				if err != nil {
					if ctx.Err() == nil && !errors.Is(err, context.Canceled) {
						log.Error("expiry sweep failed", "error", err)
					}
					break
				}
				if n > 0 {
					log.Info("expired reservations", "count", n)
				}
				if n < 100 {
					break
				}
			}
		}
	}
}
