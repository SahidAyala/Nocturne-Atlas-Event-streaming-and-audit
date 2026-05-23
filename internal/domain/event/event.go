package event

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Event is the core domain entity representing an immutable fact.
//
// Field semantics (identity and causation):
//
//	Version       — monotonically increasing sequence number per stream, assigned
//	                by the store on Append. Callers treat it as 0 until after
//	                a successful Append. NOT the same as EventVersion.
//	EventVersion  — schema/contract version of this event type (e.g. 1, 2).
//	                Incremented when the payload shape changes in a breaking way.
//	TenantID      — scopes the event to a tenant; set by the ingest service.
//	CorrelationID — request-scoped trace ID; immutable after creation, never
//	                overwritten by transport layers (Kafka, gRPC, etc).
//	CausationID   — ID of the event that directly caused this one; empty when
//	                the event originated from a user action rather than a reaction.
//	ActorID       — who triggered the action (user UUID, "system", service name).
//	TraceID       — W3C distributed trace ID (32 lowercase hex chars, no dashes).
//	                Extracted from the `traceparent` HTTP/Kafka header (ADR-014).
//	SourceVersion — semantic version of the originating service (e.g. "1.4.2").
//
// Field semantics (replay — see ADR-015):
//
//	IsReplay            — true when this event was created by POST /replay.
//	ReplayID            — UUID of the replay batch (groups events from one replay call).
//	ReplayedAt          — UTC timestamp when the replay was initiated.
//	ReplayReason        — human-readable reason (permanent audit trail).
//	ReplaySourceEventID — ID of the original event this was replayed from.
type Event struct {
	ID           uuid.UUID `json:"id"`
	TenantID     string    `json:"tenant_id"`
	StreamID     string    `json:"stream_id"`
	Type         string    `json:"type"`
	Source       string    `json:"source"`
	// Version is the per-stream monotonic sequence number assigned by the store.
	// See EventVersion for the schema/contract version.
	Version int64 `json:"version"`
	// EventVersion is the schema/contract version of this event type.
	// Distinct from Version (stream sequence). Defaults to 1.
	EventVersion  int       `json:"event_version"`
	OccurredAt    time.Time `json:"occurred_at"`
	CorrelationID string    `json:"correlation_id,omitempty"`
	CausationID   string    `json:"causation_id,omitempty"`
	ActorID       string    `json:"actor_id,omitempty"`
	// TraceID is the W3C trace ID (32 lowercase hex chars, no dashes).
	TraceID       string `json:"trace_id,omitempty"`
	SourceVersion string `json:"source_version,omitempty"`

	// Replay fields — zero/nil on original events; populated by the replay engine (ADR-015).
	IsReplay            bool       `json:"is_replay,omitempty"`
	ReplayID            string     `json:"replay_id,omitempty"`
	ReplayedAt          *time.Time `json:"replayed_at,omitempty"`
	ReplayReason        string     `json:"replay_reason,omitempty"`
	ReplaySourceEventID string     `json:"replay_source_event_id,omitempty"`

	Payload  json.RawMessage   `json:"payload"`
	Metadata map[string]string `json:"metadata"`
}

// New builds an Event with a fresh ID and timestamp.
// Version is intentionally left as 0; the store assigns it on Append.
// correlationID must be supplied by the caller — the domain never generates it.
// Pass an empty string only in tests that do not exercise tracing behaviour.
func New(streamID, eventType, source, correlationID string, payload json.RawMessage, metadata map[string]string) *Event {
	if len(payload) == 0 {
		payload = json.RawMessage("null") // normalize absent payload to JSON null
	}
	if metadata == nil {
		metadata = make(map[string]string)
	}
	return &Event{
		ID:            uuid.New(),
		StreamID:      streamID,
		Type:          eventType,
		Source:        source,
		EventVersion:  1,
		OccurredAt:    time.Now().UTC(),
		CorrelationID: correlationID,
		Payload:       payload,
		Metadata:      metadata,
	}
}
