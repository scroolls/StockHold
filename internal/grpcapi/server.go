package grpcapi

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"log/slog"
	"time"

	stockholdv1 "stockhold/gen/stockhold/v1"
	"stockhold/internal/stockhold"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Service struct {
	stockholdv1.UnimplementedStockholdServiceServer
	store *stockhold.Store
	log   *slog.Logger
}

func New(store *stockhold.Store, key string, log *slog.Logger, extra ...grpc.ServerOption) *grpc.Server {
	secret := sha256.Sum256([]byte(key))
	options := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(64 << 10),
		grpc.ChainUnaryInterceptor(auth(secret), logCalls(log)),
	}
	options = append(options, extra...)
	server := grpc.NewServer(options...)
	stockholdv1.RegisterStockholdServiceServer(server, &Service{store: store, log: log})
	grpc_health_v1.RegisterHealthServer(server, &health{store: store})
	return server
}

func auth(secret [32]byte) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod == grpc_health_v1.Health_Check_FullMethodName {
			return handler(ctx, req)
		}
		md, _ := metadata.FromIncomingContext(ctx)
		values := md.Get("x-api-key")
		if len(values) != 1 {
			return nil, status.Error(codes.Unauthenticated, "invalid API key")
		}
		given := sha256.Sum256([]byte(values[0]))
		if subtle.ConstantTimeCompare(given[:], secret[:]) != 1 {
			return nil, status.Error(codes.Unauthenticated, "invalid API key")
		}
		return handler(ctx, req)
	}
}

func logCalls(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		result, err := handler(ctx, req)
		log.Info("rpc", "method", info.FullMethod, "code", status.Code(err).String(), "duration_ms", time.Since(start).Milliseconds())
		return result, err
	}
}

func (s *Service) CreateItem(ctx context.Context, req *stockholdv1.CreateItemRequest) (*stockholdv1.Item, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	item, err := s.store.CreateItem(ctx, req.GetSku(), req.GetName(), req.GetQuantity())
	if err != nil {
		return nil, s.mapError(err)
	}
	return itemMessage(item), nil
}

func (s *Service) GetItem(ctx context.Context, req *stockholdv1.GetItemRequest) (*stockholdv1.Item, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	item, err := s.store.GetItem(ctx, req.GetSku())
	if err != nil {
		return nil, s.mapError(err)
	}
	return itemMessage(item), nil
}

func (s *Service) Restock(ctx context.Context, req *stockholdv1.RestockRequest) (*stockholdv1.RestockResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	item, replay, err := s.store.Restock(ctx, req.GetSku(), req.GetQuantity(), req.GetIdempotencyKey())
	if err != nil {
		return nil, s.mapError(err)
	}
	return &stockholdv1.RestockResponse{Item: itemMessage(item), Replayed: replay}, nil
}

func (s *Service) Reserve(ctx context.Context, req *stockholdv1.ReserveRequest) (*stockholdv1.ReserveResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.GetTtlSeconds() < 1 || req.GetTtlSeconds() > 900 {
		return nil, status.Error(codes.InvalidArgument, "ttl_seconds must be between 1 and 900")
	}
	r, replay, err := s.store.Reserve(ctx, req.GetSku(), req.GetQuantity(), time.Duration(req.GetTtlSeconds())*time.Second, req.GetIdempotencyKey(), time.Now().UTC())
	if err != nil {
		return nil, s.mapError(err)
	}
	return &stockholdv1.ReserveResponse{Reservation: reservationMessage(r), Replayed: replay}, nil
}

func (s *Service) GetReservation(ctx context.Context, req *stockholdv1.GetReservationRequest) (*stockholdv1.Reservation, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := stockhold.ValidateID(req.GetId()); err != nil {
		return nil, s.mapError(err)
	}
	r, err := s.store.GetReservation(ctx, req.GetId(), time.Time{})
	if err != nil {
		return nil, s.mapError(err)
	}
	return reservationMessage(r), nil
}

func (s *Service) Confirm(ctx context.Context, req *stockholdv1.ChangeReservationRequest) (*stockholdv1.Reservation, error) {
	return s.transition(ctx, req, "confirm")
}

func (s *Service) Cancel(ctx context.Context, req *stockholdv1.ChangeReservationRequest) (*stockholdv1.Reservation, error) {
	return s.transition(ctx, req, "cancel")
}

func (s *Service) transition(ctx context.Context, req *stockholdv1.ChangeReservationRequest, action string) (*stockholdv1.Reservation, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := stockhold.ValidateID(req.GetId()); err != nil {
		return nil, s.mapError(err)
	}
	r, err := s.store.Transition(ctx, req.GetId(), action, time.Time{})
	if err != nil {
		return nil, s.mapError(err)
	}
	return reservationMessage(r), nil
}

func (s *Service) mapError(err error) error {
	switch {
	case errors.Is(err, stockhold.ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, stockhold.ErrNotFound):
		return status.Error(codes.NotFound, "resource not found")
	case errors.Is(err, stockhold.ErrExists):
		return status.Error(codes.AlreadyExists, "item already exists")
	case errors.Is(err, stockhold.ErrInsufficient):
		return status.Error(codes.FailedPrecondition, "insufficient available stock")
	case errors.Is(err, stockhold.ErrExpired):
		return status.Error(codes.FailedPrecondition, "reservation has expired")
	case errors.Is(err, stockhold.ErrConflict):
		return status.Error(codes.Aborted, "idempotency key or reservation state conflicts")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	default:
		s.log.Error("rpc failed", "error", err)
		return status.Error(codes.Internal, "internal server error")
	}
}

func itemMessage(item stockhold.Item) *stockholdv1.Item {
	return &stockholdv1.Item{Sku: item.SKU, Name: item.Name, OnHand: item.OnHand, Available: item.Available,
		CreatedAt: timestamppb.New(item.CreatedAt)}
}

func reservationMessage(r stockhold.Reservation) *stockholdv1.Reservation {
	statuses := map[string]stockholdv1.ReservationStatus{
		"pending":   stockholdv1.ReservationStatus_RESERVATION_STATUS_PENDING,
		"confirmed": stockholdv1.ReservationStatus_RESERVATION_STATUS_CONFIRMED,
		"cancelled": stockholdv1.ReservationStatus_RESERVATION_STATUS_CANCELLED,
		"expired":   stockholdv1.ReservationStatus_RESERVATION_STATUS_EXPIRED,
	}
	return &stockholdv1.Reservation{Id: r.ID, Sku: r.SKU, Quantity: r.Quantity, Status: statuses[r.Status],
		ExpiresAt: timestamppb.New(r.ExpiresAt), CreatedAt: timestamppb.New(r.CreatedAt), UpdatedAt: timestamppb.New(r.UpdatedAt)}
}

type health struct {
	grpc_health_v1.UnimplementedHealthServer
	store *stockhold.Store
}

func (h *health) Check(ctx context.Context, _ *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := h.store.Ping(checkCtx); err != nil {
		return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_NOT_SERVING}, nil
	}
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}
