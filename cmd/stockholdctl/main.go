package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	stockholdv1 "stockhold/gen/stockhold/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	address := flag.String("addr", "127.0.0.1:9090", "gRPC server address")
	key := flag.String("key", os.Getenv("API_KEY"), "API key; defaults to API_KEY")
	useTLS := flag.Bool("tls", false, "use TLS")
	caFile := flag.String("ca", "", "CA certificate for TLS")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		return usage()
	}
	if *caFile != "" && !*useTLS {
		return fmt.Errorf("-ca requires -tls")
	}
	var transport credentials.TransportCredentials = insecure.NewCredentials()
	if *useTLS {
		transport = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
		if *caFile != "" {
			var err error
			transport, err = credentials.NewClientTLSFromFile(*caFile, "")
			if err != nil {
				return err
			}
		}
	}
	connection, err := grpc.NewClient(*address, grpc.WithTransportCredentials(transport))
	if err != nil {
		return err
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if args[0] == "health" {
		response, err := grpc_health_v1.NewHealthClient(connection).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
		if err != nil {
			return err
		}
		if response.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
			return fmt.Errorf("service is %s", response.GetStatus())
		}
		return printJSON(map[string]string{"status": response.GetStatus().String()})
	}
	if *key == "" {
		return fmt.Errorf("API_KEY or -key is required")
	}
	ctx = metadata.AppendToOutgoingContext(ctx, "x-api-key", *key)
	client := stockholdv1.NewStockholdServiceClient(connection)
	var result proto.Message
	switch args[0] {
	case "create-item":
		if len(args) != 4 {
			return usage()
		}
		quantity, err := number(args[3])
		if err != nil {
			return err
		}
		result, err = client.CreateItem(ctx, &stockholdv1.CreateItemRequest{Sku: args[1], Name: args[2], Quantity: quantity})
		if err != nil {
			return err
		}
	case "get-item":
		if len(args) != 2 {
			return usage()
		}
		result, err = client.GetItem(ctx, &stockholdv1.GetItemRequest{Sku: args[1]})
		if err != nil {
			return err
		}
	case "restock":
		if len(args) != 4 {
			return usage()
		}
		quantity, err := number(args[2])
		if err != nil {
			return err
		}
		result, err = client.Restock(ctx, &stockholdv1.RestockRequest{Sku: args[1], Quantity: quantity, IdempotencyKey: args[3]})
		if err != nil {
			return err
		}
	case "reserve":
		if len(args) != 5 {
			return usage()
		}
		quantity, err := number(args[2])
		if err != nil {
			return err
		}
		ttl, err := number(args[3])
		if err != nil {
			return err
		}
		result, err = client.Reserve(ctx, &stockholdv1.ReserveRequest{Sku: args[1], Quantity: quantity, TtlSeconds: ttl, IdempotencyKey: args[4]})
		if err != nil {
			return err
		}
	case "get-reservation":
		if len(args) != 2 {
			return usage()
		}
		result, err = client.GetReservation(ctx, &stockholdv1.GetReservationRequest{Id: args[1]})
		if err != nil {
			return err
		}
	case "confirm", "cancel":
		if len(args) != 2 {
			return usage()
		}
		request := &stockholdv1.ChangeReservationRequest{Id: args[1]}
		if args[0] == "confirm" {
			result, err = client.Confirm(ctx, request)
		} else {
			result, err = client.Cancel(ctx, request)
		}
		if err != nil {
			return err
		}
	default:
		return usage()
	}
	data, err := (protojson.MarshalOptions{Multiline: true, Indent: "  "}).Marshal(result)
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

func number(raw string) (int64, error) {
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number %q: %w", raw, err)
	}
	return n, nil
}

func printJSON(value any) error {
	return json.NewEncoder(os.Stdout).Encode(value)
}

func usage() error {
	return fmt.Errorf("usage: stockholdctl [-addr host:port] [-key key] [-tls] [-ca cert.pem] <health | create-item SKU NAME QTY | get-item SKU | restock SKU QTY KEY | reserve SKU QTY TTL_SECONDS KEY | get-reservation ID | confirm ID | cancel ID>")
}
