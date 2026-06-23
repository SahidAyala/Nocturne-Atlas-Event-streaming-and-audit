package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/application/consume"
	"github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/config"
)

// DLQProducer writes dead-letter messages to a dedicated Kafka topic.
// It implements consume.DLQPublisher.
//
// Design notes:
//   - Separate writer from the main events Producer: different topic, message
//     schema, and partitioning key (event UUID instead of stream_id).
//   - WriteTimeout is shorter than the main producer (5s vs 10s): DLQ writes
//     are best-effort; if the DLQ broker is also down we log and move on.
type DLQProducer struct {
	writer *kafkago.Writer
}

func NewDLQProducer(cfg config.KafkaConfig) *DLQProducer {
	return &DLQProducer{
		writer: &kafkago.Writer{
			Addr:         kafkago.TCP(cfg.Brokers...),
			Topic:        cfg.DLQTopicName(),
			RequiredAcks: kafkago.RequireOne,
			WriteTimeout: 5 * time.Second,
			Async:        false,
		},
	}
}

// Publish serialises the DLQMessage to JSON and writes it to the DLQ topic.
// The message key is the event UUID so each failed event lands on a
// consistent partition and duplicate DLQ entries can be deduplicated downstream.
func (p *DLQProducer) Publish(ctx context.Context, msg consume.DLQMessage) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal dlq message: %w", err)
	}

	km := kafkago.Message{
		Key:   []byte(msg.Event.ID.String()),
		Value: payload,
		Time:  msg.FailedAt,
	}

	if err := p.writer.WriteMessages(ctx, km); err != nil {
		return fmt.Errorf("write to dlq topic: %w", err)
	}
	return nil
}

func (p *DLQProducer) Close() error {
	return p.writer.Close()
}
