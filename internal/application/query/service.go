package query

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	appauth "github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/application/auth"
	"github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/domain/event"
	"github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/pkg/trace"
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
// When CorrelationID is non-empty the query is served from PostgreSQL
// (source of truth) instead of Elasticsearch.
type ListQuery struct {
	Limit         int
	Offset        int
	CorrelationID string // optional; non-empty triggers a store lookup via idx_events_correlation_id
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

// Service handles event query use cases.
// store is used for authoritative PK lookups (PostgreSQL source of truth).
// searcher is used for paginated queries against the Elasticsearch read model.
type Service struct {
	store    event.Store
	searcher event.Searcher
	log      *slog.Logger
}

func NewService(store event.Store, searcher event.Searcher, log *slog.Logger) *Service {
	return &Service{store: store, searcher: searcher, log: log}
}

// ListAll retrieves a paginated list of all events across all streams, sorted by
// occurred_at DESC. Results are scoped to the caller's tenant.
//
// When q.CorrelationID is non-empty the query is served from PostgreSQL
// (source of truth, strong consistency) using idx_events_correlation_id.
// Otherwise results come from the Elasticsearch read model (eventual consistency).
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

	if q.CorrelationID != "" {
		events, total, err := s.store.ListByCorrelationID(ctx, identity.TenantID, q.CorrelationID, q.Limit, q.Offset)
		if err != nil {
			s.log.Error("store correlation_id filter failed",
				"correlation_id", trace.FromContext(ctx),
				"filter_correlation_id", q.CorrelationID,
				"tenant_id", identity.TenantID,
				"limit", q.Limit,
				"offset", q.Offset,
				"error", err,
			)
			return nil, fmt.Errorf("list events by correlation_id: %w", err)
		}
		if events == nil {
			events = []*event.Event{}
		}
		return &ListResult{
			Events:    events,
			Total:     total,
			Limit:     q.Limit,
			Offset:    q.Offset,
			ReadModel: "postgres",
		}, nil
	}

	events, total, err := s.searcher.Search(ctx, event.SearchQuery{
		TenantID:             identity.TenantID,
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

// CausationQuery carries parameters for listing events caused by a given event.
type CausationQuery struct {
	// EventID is the causation_id to look up — returns events whose causation_id equals this.
	EventID string
	Limit   int
	Offset  int
}

// CausationResult is the paginated response from ListByCausationID.
type CausationResult struct {
	SourceEventID string
	Events        []*event.Event
	Total         int64
	Limit         int
	Offset        int
}

// TimelineQuery carries parameters for tenant timeline reconstruction.
type TimelineQuery struct {
	FromTime time.Time // zero = no lower bound
	ToTime   time.Time // zero = no upper bound
	Limit    int
	Offset   int
}

// TimelineResult is the paginated response from GetTimeline.
type TimelineResult struct {
	TenantID string
	Events   []*event.Event
	Total    int64
	Limit    int
	Offset   int
}

// ListByCausationID returns events whose causation_id equals q.EventID, scoped to the
// caller's tenant, ordered by occurred_at ASC. Used to traverse causation trees (ADR-017).
func (s *Service) ListByCausationID(ctx context.Context, q CausationQuery) (*CausationResult, error) {
	identity, ok := appauth.IdentityFromContext(ctx)
	if !ok || identity.TenantID == "" {
		return nil, fmt.Errorf("unauthenticated: identity with tenant_id is required")
	}
	if q.EventID == "" {
		return nil, fmt.Errorf("event_id is required")
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

	events, total, err := s.store.ListByCausationID(ctx, identity.TenantID, q.EventID, q.Limit, q.Offset)
	if err != nil {
		s.log.Error("list by causation_id failed",
			"correlation_id", trace.FromContext(ctx),
			"event_id", q.EventID,
			"tenant_id", identity.TenantID,
			"error", err,
		)
		return nil, fmt.Errorf("list events by causation_id: %w", err)
	}
	if events == nil {
		events = []*event.Event{}
	}
	return &CausationResult{
		SourceEventID: q.EventID,
		Events:        events,
		Total:         total,
		Limit:         q.Limit,
		Offset:        q.Offset,
	}, nil
}

// GetTimeline returns a paginated, occurred_at DESC slice of all events for the caller's
// tenant within an optional time window. Used for incident reconstruction when a
// correlationId is unknown (ADR-017). Results come from PostgreSQL (source of truth).
func (s *Service) GetTimeline(ctx context.Context, q TimelineQuery) (*TimelineResult, error) {
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

	events, total, err := s.store.ListTimeline(ctx, identity.TenantID, q.FromTime, q.ToTime, q.Limit, q.Offset)
	if err != nil {
		s.log.Error("timeline query failed",
			"correlation_id", trace.FromContext(ctx),
			"tenant_id", identity.TenantID,
			"from_time", q.FromTime,
			"to_time", q.ToTime,
			"error", err,
		)
		return nil, fmt.Errorf("list timeline: %w", err)
	}
	if events == nil {
		events = []*event.Event{}
	}
	return &TimelineResult{
		TenantID: identity.TenantID,
		Events:   events,
		Total:    total,
		Limit:    q.Limit,
		Offset:   q.Offset,
	}, nil
}

// GetByID retrieves a single event by its UUID from PostgreSQL (source of truth).
// This is an O(1) PK lookup — use it when you need a specific event, not the stream history.
// Returns event.ErrNotFound when the ID does not exist or belongs to a different tenant.
func (s *Service) GetByID(ctx context.Context, id uuid.UUID) (*event.Event, error) {
	identity, ok := appauth.IdentityFromContext(ctx)
	if !ok || identity.TenantID == "" {
		return nil, fmt.Errorf("unauthenticated: identity with tenant_id is required")
	}

	e, err := s.store.GetByID(ctx, id)
	if err != nil {
		if errors.Is(err, event.ErrNotFound) {
			return nil, event.ErrNotFound
		}
		s.log.Error("store lookup failed",
			"correlation_id", trace.FromContext(ctx),
			"event_id", id,
			"tenant_id", identity.TenantID,
			"error", err,
		)
		return nil, fmt.Errorf("get event by id: %w", err)
	}

	// Enforce tenant isolation: treat a cross-tenant hit as not found to
	// avoid leaking the existence of events owned by other tenants.
	if e.TenantID != identity.TenantID {
		return nil, event.ErrNotFound
	}

	return e, nil
}
