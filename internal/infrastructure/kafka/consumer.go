package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/SheykoWk/event-streaming-and-audit/internal/config"
	"github.com/SheykoWk/event-streaming-and-audit/internal/domain/event"
)

// Consumer reads events from a Kafka topic using consumer-group offset management.
// Offsets are committed only after the handler returns nil, giving at-least-once delivery.
type Consumer struct {
	reader *kafkago.Reader
	log    *slog.Logger
}

func NewConsumer(cfg config.KafkaConfig, log *slog.Logger) *Consumer {
	return &Consumer{
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers:     cfg.Brokers,
			Topic:       cfg.Topic,
			GroupID:     cfg.GroupID,
			MinBytes:    1,
			MaxBytes:    10 << 20, // 10 MiB
			MaxWait:     500 * time.Millisecond,
			StartOffset: kafkago.FirstOffset,
		}),
		log: log,
	}
}

// Run loops over incoming Kafka messages and calls handle for each deserialized event.
//
// Lifecycle:
//   - Malformed messages (JSON decode failure) are logged and skipped — offset is
//     committed so the poison pill does not block the partition.
//   - If handle returns an error the offset is NOT committed and Run returns immediately,
//     allowing the process to restart and retry from the same offset.
//   - When ctx is cancelled (graceful shutdown) Run returns nil.
func (c *Consumer) Run(ctx context.Context, handle func(context.Context, *event.Event) error) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // context cancelled — normal shutdown path
			}
			return fmt.Errorf("fetch message: %w", err)
		}

		var e event.Event
		if err := json.Unmarshal(msg.Value, &e); err != nil {
			c.log.Warn("skipping malformed message",
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
				"message_key", string(msg.Key),
				"payload_preview", payloadPreview(msg.Value, 200),
				"error", err,
			)
			// Commit to advance past the poison pill; don't halt the consumer.
			if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
				return fmt.Errorf("commit after skip: %w", commitErr)
			}
			continue
		}

		if err := handle(ctx, &e); err != nil {
			// Do not commit — restart will reprocess from this offset.
			// Log structured fields here because the caller (main.go) only
			// receives the wrapped error string and loses all context.
			c.log.Error("handler rejected event — consumer stopping",
				"event_id", e.ID,
				"stream_id", e.StreamID,
				"version", e.Version,
				"correlation_id", e.CorrelationID,
				"error", err,
			)
			return fmt.Errorf("handle event %s: %w", e.ID, err)
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			return fmt.Errorf("commit message: %w", err)
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

// payloadPreview returns a safe, truncated string of raw bytes for log diagnostics.
// Limits output to maxBytes to prevent log flooding from large or binary payloads.
func payloadPreview(b []byte, maxBytes int) string {
	if len(b) <= maxBytes {
		return string(b)
	}
	return string(b[:maxBytes]) + "…"
}
