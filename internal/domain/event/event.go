package event

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Event is the core domain entity representing an immutable fact.
// Version is set by the store on Append; callers should treat it as
// zero until after a successful Append.
// TenantID scopes the event to a tenant; set by the ingest service from the caller's Identity.
// CorrelationID is a request-scoped trace identifier set at HTTP ingress and carried
// through the full event lifecycle. It is never generated here — the domain only
// accepts it from the application layer.
type Event struct {
	ID            uuid.UUID         `json:"id"`
	TenantID      string            `json:"tenant_id"`
	StreamID      string            `json:"stream_id"`
	Type          string            `json:"type"`
	Source        string            `json:"source"`
	Version       int64             `json:"version"`
	OccurredAt    time.Time         `json:"occurred_at"`
	// CorrelationID is immutable after creation and is derived from HTTP ingress.
	// It must never be overwritten by transport layers (Kafka, gRPC, etc).
	CorrelationID string `json:"correlation_id,omitempty"`
	Payload       json.RawMessage   `json:"payload"`
	Metadata      map[string]string `json:"metadata"`
}

// New builds an Event with a fresh ID and timestamp.
// Version is intentionally left as 0; the store assigns it on Append.
// correlationID must be supplied by the caller — the domain never generates it.
// Pass an empty string only in tests that do not exercise tracing behaviour.
func New(streamID, eventType, source, correlationID string, payload json.RawMessage, metadata map[string]string) *Event {
	if len(payload) == 0 {
		payload = json.RawMessage("null") // normalize absent payload to JSON null — never let nil reach the store
	}
	if metadata == nil {
		metadata = make(map[string]string)
	}
	return &Event{
		ID:            uuid.New(),
		StreamID:      streamID,
		Type:          eventType,
		Source:        source,
		OccurredAt:    time.Now().UTC(),
		CorrelationID: correlationID,
		Payload:       payload,
		Metadata:      metadata,
	}
}
