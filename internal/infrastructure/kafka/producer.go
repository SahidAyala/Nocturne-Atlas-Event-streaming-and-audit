package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/config"
	"github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/domain/event"
	"github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/pkg/trace"
)

// Producer publishes events to Kafka, routing each message to the topic
// returned by the TopicResolver. Partitioning within the topic is consistent
// by StreamID (Hash balancer), so all events for a stream land on the same
// partition and are delivered in order.
type Producer struct {
	writer   *kafkago.Writer
	resolver TopicResolver
}

func NewProducer(cfg config.KafkaConfig, resolver TopicResolver) *Producer {
	return &Producer{
		resolver: resolver,
		writer: &kafkago.Writer{
			Addr: kafkago.TCP(cfg.Brokers...),
			// Topic is intentionally empty: each message carries its own topic
			// from resolver.Resolve(). This enables per-message routing without
			// any changes to the writer after construction.
			//
			// kafka-go contract: when Writer.Topic == "" every Message.Topic
			// must be non-empty, or WriteMessages returns an error.
			Topic:        "",
			Balancer:     &kafkago.Hash{}, // consistent partition per StreamID
			RequiredAcks: kafkago.RequireOne,
			WriteTimeout: 10 * time.Second,
			Async:        false,
		},
	}
}

// Publish serialises the event to JSON and writes it to the topic resolved
// for this event. The message key is StreamID for consistent partitioning.
func (p *Producer) Publish(ctx context.Context, e *event.Event) error {
	payload, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	headers := []kafkago.Header{
		{Key: "event_id", Value: []byte(e.ID.String())},
		{Key: "event_type", Value: []byte(e.Type)},
		{Key: "source", Value: []byte(e.Source)},
	}
	// Propagate W3C TraceContext so consumers can correlate Kafka processing with the
	// originating HTTP request span (ADR-014). The producer generates a fresh span ID
	// to represent the "publish" act as a logical child of the ingest handler span.
	if e.TraceID != "" {
		tc := trace.TraceContext{TraceID: e.TraceID, Sampled: true}
		headers = append(headers, kafkago.Header{
			Key:   "traceparent",
			Value: []byte(tc.Traceparent(trace.NewSpanID())),
		})
	}
	if e.CorrelationID != "" {
		headers = append(headers, kafkago.Header{
			Key:   "correlation_id",
			Value: []byte(e.CorrelationID),
		})
	}

	msg := kafkago.Message{
		Topic:   p.resolver.Resolve(e),
		Key:     []byte(e.StreamID),
		Value:   payload,
		Time:    e.OccurredAt,
		Headers: headers,
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("write to kafka: %w", err)
	}
	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
