package consume

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/SheykoWk/event-streaming-and-audit/internal/domain/event"
	"github.com/SheykoWk/event-streaming-and-audit/internal/pkg/trace"
)

// Subscriber is satisfied by any message source that can deliver a stream of events.
// kafka.Consumer implements this interface; define it here so the application layer
// has no import dependency on the kafka infrastructure package.
type Subscriber interface {
	Run(ctx context.Context, handle func(context.Context, *event.Event) error) error
}

// DLQMessage is the envelope written to the dead-letter queue when an event
// fails processing. It carries the original event so it can be reprocessed,
// plus diagnostic metadata for alerting and root-cause analysis.
// CorrelationID is promoted to the top level so DLQ consumers can filter and
// trace failures without deserialising the nested event.
type DLQMessage struct {
	Event         *event.Event `json:"event"`
	Reason        string       `json:"reason"`
	FailedAt      time.Time    `json:"failed_at"`
	CorrelationID string       `json:"correlation_id"`
}

// DLQPublisher is the outbound port for the dead-letter queue.
// Implementations must be safe to call concurrently.
type DLQPublisher interface {
	Publish(ctx context.Context, msg DLQMessage) error
	Close() error
}

// Service orchestrates the consume loop: receive from Subscriber → index via Indexer.
// If indexing fails the event is routed to the DLQ and the offset is still committed,
// preventing infinite retry of a persistently-failing event.
type Service struct {
	sub     Subscriber
	indexer event.Indexer
	dlq     DLQPublisher
	log     *slog.Logger
}

func NewService(sub Subscriber, indexer event.Indexer, dlq DLQPublisher, log *slog.Logger) *Service {
	return &Service{sub: sub, indexer: indexer, dlq: dlq, log: log}
}

// Run blocks until ctx is cancelled or a non-recoverable error occurs.
func (s *Service) Run(ctx context.Context) error {
	return s.sub.Run(ctx, s.handle)
}

// handle indexes one event. On indexer failure it routes to DLQ and returns nil
// so the caller (Subscriber) commits the Kafka offset and moves forward.
// If DLQ publish also fails the failure is logged and nil is still returned —
// the event remains safe in PostgreSQL and can be recovered via the replay engine.
func (s *Service) handle(ctx context.Context, e *event.Event) error {
	if err := s.indexer.Index(ctx, e); err != nil {
		s.routeToDLQ(ctx, e, err)
		return nil // always commit the offset; never retry through Kafka
	}
	s.log.Info("event indexed",
		"event_id", e.ID,
		"stream_id", e.StreamID,
		"type", e.Type,
		"version", e.Version,
		"tenant_id", e.TenantID,
		"correlation_id", trace.Coalesce(ctx, e.CorrelationID),
	)
	return nil
}

func (s *Service) routeToDLQ(ctx context.Context, e *event.Event, indexErr error) {
	correlationID := trace.Coalesce(ctx, e.CorrelationID)
	msg := DLQMessage{
		Event:         e,
		Reason:        indexErr.Error(),
		FailedAt:      time.Now().UTC(),
		CorrelationID: correlationID,
	}
	if dlqErr := s.dlq.Publish(ctx, msg); dlqErr != nil {
		// Double failure: both the indexer and the DLQ are unavailable.
		// Log both errors so on-call can correlate them; do not block the consumer.
		// The event is durable in PostgreSQL and can be recovered via replay.
		s.log.Error("failed to publish to DLQ — event will be skipped",
			"event_id", e.ID,
			"stream_id", e.StreamID,
			"tenant_id", e.TenantID,
			"correlation_id", correlationID,
			"index_error", indexErr,
			"dlq_error", fmt.Errorf("dlq publish: %w", dlqErr),
		)
		return
	}
	s.log.Warn("event sent to DLQ",
		"event_id", e.ID,
		"stream_id", e.StreamID,
		"version", e.Version,
		"tenant_id", e.TenantID,
		"correlation_id", correlationID,
		"reason", indexErr.Error(),
	)
}
