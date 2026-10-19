package outbox

import (
	"context"
	"strings"
	"time"

	"stockhold/internal/stockhold"

	"github.com/segmentio/kafka-go"
)

type KafkaPublisher struct{ writer *kafka.Writer }

func NewKafkaPublisher(brokers string) *KafkaPublisher {
	addresses := strings.Split(brokers, ",")
	for i := range addresses {
		addresses[i] = strings.TrimSpace(addresses[i])
	}
	return &KafkaPublisher{writer: &kafka.Writer{
		Addr:                   kafka.TCP(addresses...),
		Topic:                  stockhold.EventsTopic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		MaxAttempts:            1,
		BatchSize:              1,
		WriteTimeout:           10 * time.Second,
		AllowAutoTopicCreation: true,
	}}
}

func (p *KafkaPublisher) Publish(ctx context.Context, event stockhold.OutboxEvent) error {
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.Key),
		Value: event.Payload,
		Headers: []kafka.Header{
			{Key: "event-id", Value: []byte(event.ID)},
			{Key: "event-type", Value: []byte(event.Type)},
		},
	})
}

func (p *KafkaPublisher) Close() error { return p.writer.Close() }
