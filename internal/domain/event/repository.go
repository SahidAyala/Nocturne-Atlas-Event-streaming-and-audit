package event

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

// ErrNotFound is returned by Store.GetByID when no event matches the given UUID.
var ErrNotFound = errors.New("event not found")

// Store is the outbound port for the append-only event store.
// Append must set e.Version to the DB-assigned value before returning.
type Store interface {
	Append(ctx context.Context, e *Event) error
	GetByStreamID(ctx context.Context, streamID string) ([]*Event, error)
	GetByID(ctx context.Context, id uuid.UUID) (*Event, error)
	// GetFromVersion returns all events for a stream with version >= fromVersion,
	// ordered by version ASC. fromVersion=0 returns all events from the beginning.
	GetFromVersion(ctx context.Context, streamID string, fromVersion int64) ([]*Event, error)
	// ListByCorrelationID returns a paginated, occurred_at DESC slice of events matching
	// tenantID and correlationID. total is the unfiltered count (for pagination).
	ListByCorrelationID(ctx context.Context, tenantID, correlationID string, limit, offset int) ([]*Event, int64, error)
}

// Publisher is the outbound port for the event bus.
// Implementations should treat Publish as best-effort: the event is
// already durable in the store before Publish is called.
type Publisher interface {
	Publish(ctx context.Context, e *Event) error
	Close() error
}

// Indexer is the outbound port for the search index.
// Index must be idempotent: calling it twice with the same event must
// produce the same result (use event.ID as the document key).
type Indexer interface {
	Index(ctx context.Context, e *Event) error
	Close() error
}

// SearchQuery carries parameters for reading from the read model.
// TenantID must be set by the application layer; the Searcher filters results to this tenant.
// StreamID is optional — omit it to query across all streams.
type SearchQuery struct {
	TenantID string
	StreamID string // empty = no stream filter (list all)
	Limit    int
	Offset   int

	// SortByOccurredAtDesc sorts by occurred_at DESC instead of version ASC.
	// Use for cross-stream queries where per-stream version ordering is meaningless.
	SortByOccurredAtDesc bool
}

// Searcher is the outbound port for the Elasticsearch read model.
// Results are eventually consistent with the PostgreSQL event store —
// recently ingested events may not yet be visible.
type Searcher interface {
	Search(ctx context.Context, q SearchQuery) ([]*Event, int64, error)
}
