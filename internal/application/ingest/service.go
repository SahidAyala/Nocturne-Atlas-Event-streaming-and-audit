package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	appauth "github.com/SheykoWk/event-streaming-and-audit/internal/application/auth"
	"github.com/SheykoWk/event-streaming-and-audit/internal/domain/event"
	"github.com/SheykoWk/event-streaming-and-audit/internal/pkg/trace"
)

// Command carries the data needed to ingest a new event.
type Command struct {
	StreamID string
	Type     string
	Source   string
	Payload  json.RawMessage
	Metadata map[string]string
	// CorrelationID is the request-scoped trace identifier propagated from the
	// HTTP ingress. It is sourced (in priority order) from the X-Correlation-ID
	// request header, the chi-generated X-Request-Id, or a freshly generated UUID.
	// It is always non-empty when produced by handler.ingest.
	CorrelationID string
}

// Service orchestrates event ingestion: store first, then publish.
type Service struct {
	store     event.Store
	publisher event.Publisher
	log       *slog.Logger
}

func NewService(store event.Store, publisher event.Publisher, log *slog.Logger) *Service {
	return &Service{store: store, publisher: publisher, log: log}
}

// Ingest appends the event to the store and publishes it to Kafka.
// Requires an Identity in ctx for tenant scoping — returns an error if absent.
// A Kafka publish failure is logged but does not fail the request —
// the event is already durable in PostgreSQL.
func (s *Service) Ingest(ctx context.Context, cmd Command) (*event.Event, error) {
	identity, ok := appauth.IdentityFromContext(ctx)
	if !ok || identity.TenantID == "" {
		return nil, fmt.Errorf("unauthenticated: identity with tenant_id is required")
	}

	if cmd.StreamID == "" || cmd.Type == "" || cmd.Source == "" {
		return nil, fmt.Errorf("stream_id, type, and source are required")
	}
	// Guarantee a non-empty correlation_id so every persisted event is traceable.
	// The HTTP handler always provides one, but callers that bypass HTTP (admin
	// tooling, batch imports, tests) might not — a generated UUID is always better
	// than an empty string that silently breaks the index.
	if cmd.CorrelationID == "" {
		cmd.CorrelationID = uuid.NewString()
	}

	e := event.New(cmd.StreamID, cmd.Type, cmd.Source, cmd.CorrelationID, cmd.Payload, cmd.Metadata)
	e.TenantID = identity.TenantID

	if err := s.store.Append(ctx, e); err != nil {
		return nil, fmt.Errorf("append to store: %w", err)
	}

	if err := s.publisher.Publish(ctx, e); err != nil {
		s.log.Warn("failed to publish event to kafka",
			"event_id", e.ID,
			"stream_id", e.StreamID,
			"version", e.Version,
			"tenant_id", e.TenantID,
			"correlation_id", trace.Coalesce(ctx, e.CorrelationID),
			"error", err,
		)
	}

	s.log.Info("event ingested",
		"event_id", e.ID,
		"stream_id", e.StreamID,
		"type", e.Type,
		"version", e.Version,
		"tenant_id", e.TenantID,
		"correlation_id", trace.Coalesce(ctx, e.CorrelationID),
	)
	return e, nil
}
