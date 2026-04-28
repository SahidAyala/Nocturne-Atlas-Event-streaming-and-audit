package query

import (
	"context"
	"fmt"
	"log/slog"

	appauth "github.com/SheykoWk/event-streaming-and-audit/internal/application/auth"
	"github.com/SheykoWk/event-streaming-and-audit/internal/domain/event"
	"github.com/SheykoWk/event-streaming-and-audit/internal/pkg/trace"
)

const (
	defaultLimit = 20
	maxLimit     = 100
)

// Query carries the parameters for retrieving events from the read model.
type IdQuery struct {
	StreamID string
	Limit    int
	Offset   int
}

// Query carries the parameters for retrieving events from the read model.
type StreamQuery struct {
	StreamID string
	Limit    int
	Offset   int
}

// Result is the paginated response from QueryByStream.
// ReadModel signals the data source and its consistency guarantee.
type Result struct {
	StreamID  string         `json:"stream_id"`
	Events    []*event.Event `json:"events"`
	Total     int64          `json:"total"`
	Limit     int            `json:"limit"`
	Offset    int            `json:"offset"`
	ReadModel string         `json:"read_model"`
}

// ListQuery carries the parameters for listing events across all streams.
type ListQuery struct {
	Limit  int
	Offset int
}

// ListResult is the paginated response from ListAll.
// Unlike Result it has no StreamID since it spans multiple streams.
type ListResult struct {
	Events    []*event.Event `json:"events"`
	Total     int64          `json:"total"`
	Limit     int            `json:"limit"`
	Offset    int            `json:"offset"`
	ReadModel string         `json:"read_model"`
}

// Service handles event query use cases against the read model.
type Service struct {
	searcher event.Searcher
	log      *slog.Logger
}

func NewService(searcher event.Searcher, log *slog.Logger) *Service {
	return &Service{searcher: searcher, log: log}
}

// ListAll retrieves a paginated list of all events across all streams from the
// read model, sorted by occurred_at DESC (newest first — useful as an audit feed).
// Results are scoped to the caller's tenant.
func (s *Service) ListAll(ctx context.Context, q ListQuery) (*ListResult, error) {
	identity, ok := appauth.IdentityFromContext(ctx)
	if !ok || identity.TenantID == "" {
		return nil, fmt.Errorf("unauthenticated: identity with tenant_id is required")
	}

	if q.Limit <= 0 {
		q.Limit = defaultLimit
	}
	if q.Limit > maxLimit {
		q.Limit = maxLimit
	}
	if q.Offset < 0 {
		q.Offset = 0
	}

	events, total, err := s.searcher.Search(ctx, event.SearchQuery{
		TenantID:             identity.TenantID,
		// StreamID intentionally empty — no stream filter
		Limit:                q.Limit,
		Offset:               q.Offset,
		SortByOccurredAtDesc: true,
	})
	if err != nil {
		s.log.Error("read model list failed",
			"correlation_id", trace.FromContext(ctx),
			"tenant_id", identity.TenantID,
			"limit", q.Limit,
			"offset", q.Offset,
			"error", err,
		)
		return nil, fmt.Errorf("list events: %w", err)
	}

	if events == nil {
		events = []*event.Event{}
	}

	return &ListResult{
		Events:    events,
		Total:     total,
		Limit:     q.Limit,
		Offset:    q.Offset,
		ReadModel: "elasticsearch",
	}, nil
}

// QueryByStream retrieves a paginated, ordered page of events for a stream
// from the Elasticsearch read model. Results are eventually consistent with
// the PostgreSQL event store.
// Requires an Identity in ctx for tenant scoping — returns an error if absent.
func (s *Service) QueryByStream(ctx context.Context, q StreamQuery) (*Result, error) {
	identity, ok := appauth.IdentityFromContext(ctx)
	if !ok || identity.TenantID == "" {
		return nil, fmt.Errorf("unauthenticated: identity with tenant_id is required")
	}

	if q.StreamID == "" {
		return nil, fmt.Errorf("stream_id is required")
	}
	if q.Limit <= 0 {
		q.Limit = defaultLimit
	}
	if q.Limit > maxLimit {
		q.Limit = maxLimit
	}
	if q.Offset < 0 {
		q.Offset = 0
	}

	events, total, err := s.searcher.Search(ctx, event.SearchQuery{
		TenantID: identity.TenantID,
		StreamID: q.StreamID,
		Limit:    q.Limit,
		Offset:   q.Offset,
	})
	if err != nil {
		s.log.Error("read model search failed",
			"correlation_id", trace.FromContext(ctx),
			"stream_id", q.StreamID,
			"tenant_id", identity.TenantID,
			"limit", q.Limit,
			"offset", q.Offset,
			"error", err,
		)
		return nil, fmt.Errorf("search events: %w", err)
	}

	if events == nil {
		events = []*event.Event{}
	}

	return &Result{
		StreamID:  q.StreamID,
		Events:    events,
		Total:     total,
		Limit:     q.Limit,
		Offset:    q.Offset,
		ReadModel: "elasticsearch",
	}, nil
}
