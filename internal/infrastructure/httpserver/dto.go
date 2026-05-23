package httpserver

import (
	"encoding/json"
	"time"
)

// HealthResponse is the response body for GET /health.
type HealthResponse struct {
	Status string `json:"status" example:"ok"`
}

// ErrorResponse is the response body for all 4xx and 5xx errors.
type ErrorResponse struct {
	Error string `json:"error" example:"stream_id is required"`
}

// IngestRequest is the request body for POST /events.
type IngestRequest struct {
	// StreamID is the target stream. Created implicitly if it does not exist.
	StreamID string `json:"stream_id" example:"order:1"`
	// Type is the event type in dot-notation.
	Type string `json:"type" example:"order.created"`
	// Source is the originating service name.
	Source string `json:"source" example:"orders-svc"`
	// Payload is an arbitrary JSON value attached to the event.
	// Accepted as raw JSON bytes to avoid float64 precision loss during decode.
	Payload json.RawMessage `json:"payload,omitempty" swaggertype:"object"`
	// Metadata is optional key-value metadata for routing or tracing.
	Metadata map[string]string `json:"metadata,omitempty" example:"region:us-east-1,trace_id:abc-123"`

	// Canonical envelope fields — all optional for backward compatibility.
	// When present they are stored as first-class columns (not inside metadata)
	// and indexed for cross-service correlation queries.

	// EventVersion is the schema/contract version of this event type (default 1).
	// Distinct from the stream version assigned by the store.
	EventVersion int `json:"event_version,omitempty" example:"1"`
	// CausationID is the ID of the upstream event that caused this one.
	CausationID string `json:"causation_id,omitempty" example:"01906c2e-4a3b-7000-8000-abc123def456"`
	// ActorID identifies who triggered the action (user UUID, "system", service name).
	ActorID string `json:"actor_id,omitempty" example:"user-uuid"`
	// TraceID is an optional W3C/B3 distributed trace ID.
	TraceID string `json:"trace_id,omitempty" example:"4bf92f3577b34da6a3ce929d0e0e4736"`
	// SourceVersion is the semantic version of the originating service.
	SourceVersion string `json:"source_version,omitempty" example:"1.4.2"`
}

// ReplayFilterRequest is the filter part of a replay request body.
type ReplayFilterRequest struct {
	TenantID      string   `json:"tenant_id,omitempty"`
	StreamID      string   `json:"stream_id,omitempty"`
	CorrelationID string   `json:"correlation_id,omitempty"`
	EventType     string   `json:"event_type,omitempty"`
	ActorID       string   `json:"actor_id,omitempty"`
	FromTime      string   `json:"from_time,omitempty"` // ISO-8601
	ToTime        string   `json:"to_time,omitempty"`   // ISO-8601
	EventIDs      []string `json:"event_ids,omitempty"`
}

// ReplayOptionsRequest is the options part of a replay request body.
type ReplayOptionsRequest struct {
	DryRun       bool   `json:"dry_run,omitempty"`
	ReplayReason string `json:"replay_reason,omitempty"`
	SafetyLimit  int    `json:"safety_limit,omitempty"`
}

// ReplayRequest is the request body for POST /replay.
type ReplayRequest struct {
	Filter  ReplayFilterRequest  `json:"filter"`
	Options ReplayOptionsRequest `json:"options"`
}

// ReplayResponse is the response body for POST /replay.
type ReplayResponse struct {
	ReplayID      string          `json:"replay_id,omitempty"`
	DryRun        bool            `json:"dry_run"`
	MatchedCount  int             `json:"matched_count"`
	ReplayedCount int             `json:"replayed_count"`
	Events        []EventResponse `json:"events"`
}

// CausationResponse is the response body for GET /events/{id}/causes.
type CausationResponse struct {
	// SourceEventID is the event whose causation children are listed.
	SourceEventID string          `json:"source_event_id"`
	Events        []EventResponse `json:"events"`
	Total         int64           `json:"total"`
	Limit         int             `json:"limit"`
	Offset        int             `json:"offset"`
}

// TimelineResponse is the response body for GET /events/timeline.
type TimelineResponse struct {
	TenantID string          `json:"tenant_id"`
	Events   []EventResponse `json:"events"`
	Total    int64           `json:"total"`
	Limit    int             `json:"limit"`
	Offset   int             `json:"offset"`
	FromTime string          `json:"from_time,omitempty"`
	ToTime   string          `json:"to_time,omitempty"`
}

// EventResponse is the full representation of a persisted event.
type EventResponse struct {
	// ID is the globally unique event UUID.
	ID string `json:"id" example:"01906c2e-4a3b-7000-8000-abc123def456"`
	// TenantID scopes the event to a tenant; derived from the caller's Identity.
	TenantID string `json:"tenant_id" example:"default"`
	// StreamID is the logical stream this event belongs to.
	StreamID string `json:"stream_id" example:"order:1"`
	// Type is the event type in dot-notation.
	Type string `json:"type" example:"order.created"`
	// Source is the service that produced the event.
	Source string `json:"source" example:"orders-svc"`
	// Version is monotonically increasing within the stream, assigned by the store.
	// This is the stream sequence number — see EventVersion for the schema version.
	Version int64 `json:"version" example:"1"`
	// EventVersion is the schema/contract version of the event type (e.g. 1, 2).
	EventVersion int `json:"event_version" example:"1"`
	// OccurredAt is the UTC timestamp when the event occurred.
	OccurredAt time.Time `json:"occurred_at" example:"2026-04-21T10:00:00Z"`
	// CorrelationID is the request-scoped trace identifier from the original ingest request.
	CorrelationID string `json:"correlation_id,omitempty" example:"01906c2e-4a3b-7000-8000-abc123def456"`
	// CausationID is the ID of the upstream event that caused this one.
	CausationID string `json:"causation_id,omitempty"`
	// ActorID identifies who triggered the action.
	ActorID string `json:"actor_id,omitempty"`
	// TraceID is the W3C trace ID (32 lowercase hex chars). Maps to an OTel trace ID.
	TraceID string `json:"trace_id,omitempty"`
	// SourceVersion is the semantic version of the originating service.
	SourceVersion string `json:"source_version,omitempty"`
	// Payload is the arbitrary JSON payload, returned as raw JSON.
	Payload json.RawMessage `json:"payload,omitempty" swaggertype:"object"`
	// Metadata is the key-value metadata attached to the event.
	Metadata map[string]string `json:"metadata,omitempty"`

	// Replay fields — only present on events created by POST /replay (ADR-015).
	IsReplay            bool       `json:"is_replay,omitempty"`
	ReplayID            string     `json:"replay_id,omitempty"`
	ReplayedAt          *time.Time `json:"replayed_at,omitempty"`
	ReplayReason        string     `json:"replay_reason,omitempty"`
	ReplaySourceEventID string     `json:"replay_source_event_id,omitempty"`
}

// CreateTenantRequest is the request body for POST /tenants.
type CreateTenantRequest struct {
	// TenantID is the unique identifier for the tenant (e.g. "acme-corp").
	TenantID string `json:"tenant_id" example:"acme-corp"`
	// SubjectID identifies the principal within the tenant (e.g. "admin", "service-account").
	SubjectID string `json:"subject_id" example:"acme-admin"`
	// Roles assigned to the credential. Defaults to ["writer","reader"] if omitted.
	Roles []string `json:"roles,omitempty" example:"writer,reader"`
	// ExpiresIn is the JWT lifetime in seconds. Defaults to 86400 (24 h). Only applies in jwt mode.
	ExpiresIn int `json:"expires_in,omitempty" example:"86400"`
}

// TenantCredentialsResponse is the response for POST /tenants.
type TenantCredentialsResponse struct {
	// TenantID is the provisioned tenant identifier.
	TenantID string `json:"tenant_id" example:"acme-corp"`
	// SubjectID is the principal this credential belongs to.
	SubjectID string `json:"subject_id" example:"acme-admin"`
	// Mode reflects the active auth mode ("jwt" or "simple").
	Mode string `json:"mode" example:"jwt"`
	// Token is the signed JWT to use as "Authorization: Bearer <token>". Only present in jwt mode.
	Token string `json:"token,omitempty" example:"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."`
	// ExpiresAt is the UTC expiry of the issued token. Only present in jwt mode.
	ExpiresAt string `json:"expires_at,omitempty" example:"2026-04-22T21:57:00Z"`
	// Note is a human-readable hint returned in simple mode.
	Note string `json:"note,omitempty" example:"simple mode is single-tenant — use X-API-Key: <AUTH_API_KEY>"`
}

// IssueTokenRequest is the optional request body for POST /auth/issue.
type IssueTokenRequest struct {
	// ExpiresIn is the desired token lifetime in seconds. Defaults to 3600 (1 hour). Max 86400 (24 h).
	ExpiresIn int `json:"expires_in,omitempty" example:"3600"`
}

// IssuedTokenResponse is the response for POST /auth/issue.
type IssuedTokenResponse struct {
	// Token is the signed JWT to use in subsequent requests as "Authorization: Bearer <token>".
	Token string `json:"token" example:"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."`
	// ExpiresAt is the UTC timestamp when the token expires.
	ExpiresAt string `json:"expires_at" example:"2026-04-21T11:00:00Z"`
	// SubjectID identifies the principal this token was issued for.
	SubjectID string `json:"subject_id" example:"api-key"`
	// TenantID is the tenant scope embedded in the token.
	TenantID string `json:"tenant_id" example:"default"`
}

// TokenResponse is the response for POST /auth/token.
// It carries the authenticated caller's identity — useful to validate credentials
// and discover the tenant_id and roles associated with the current key/token.
type TokenResponse struct {
	// SubjectID identifies the authenticated principal.
	SubjectID string `json:"subject_id" example:"api-key"`
	// TenantID is the tenant scope associated with this credential.
	TenantID string `json:"tenant_id" example:"default"`
	// Roles lists the permissions granted to this credential.
	Roles []string `json:"roles" example:"writer,reader"`
}

// QueryResultResponse is the paginated read-model response for GET /events/{streamID}.
// Results come from Elasticsearch and are eventually consistent with PostgreSQL.
type QueryResultResponse struct {
	// StreamID is the queried stream identifier.
	StreamID string `json:"stream_id" example:"order:1"`
	// Events is the ordered (version ASC) page of events.
	Events []EventResponse `json:"events"`
	// Total is the total number of matching events, useful for pagination.
	Total int64 `json:"total" example:"42"`
	// Limit is the page size applied to this query.
	Limit int `json:"limit" example:"20"`
	// Offset is the number of events skipped.
	Offset int `json:"offset" example:"0"`
	// ReadModel identifies the data source ("elasticsearch").
	ReadModel string `json:"read_model" example:"elasticsearch"`
}

// ListEventsResponse is the paginated read-model response for GET /events.
// Returns events across all streams, sorted by occurred_at DESC (newest first).
// Results come from Elasticsearch and are eventually consistent with PostgreSQL.
type ListEventsResponse struct {
	// Events is the page of events sorted by occurred_at DESC.
	Events []EventResponse `json:"events"`
	// Total is the total number of events in the index (for pagination).
	Total int64 `json:"total" example:"128"`
	// Limit is the page size applied to this query.
	Limit int `json:"limit" example:"20"`
	// Offset is the number of events skipped.
	Offset int `json:"offset" example:"0"`
	// ReadModel identifies the data source ("elasticsearch").
	ReadModel string `json:"read_model" example:"elasticsearch"`
}
