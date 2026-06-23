package query

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	appauth "github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/application/auth"
	"github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/domain/event"
)

// ---------------------------------------------------------------------------
// mockStore — in-memory event.Store for query service tests.
// ---------------------------------------------------------------------------

type mockStore struct {
	mu     sync.Mutex
	events []*event.Event
	err    error
}

func (m *mockStore) Append(_ context.Context, e *event.Event) error { return nil }

func (m *mockStore) GetByStreamID(_ context.Context, streamID string) ([]*event.Event, error) {
	return nil, nil
}

func (m *mockStore) GetByID(_ context.Context, id uuid.UUID) (*event.Event, error) {
	if m.err != nil {
		return nil, m.err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.events {
		if e.ID == id {
			cp := *e
			return &cp, nil
		}
	}
	return nil, event.ErrNotFound
}

func (m *mockStore) GetFromVersion(_ context.Context, streamID string, fromVersion int64) ([]*event.Event, error) {
	return nil, nil
}

func (m *mockStore) ListByCorrelationID(_ context.Context, tenantID, correlationID string, limit, offset int) ([]*event.Event, int64, error) {
	if m.err != nil {
		return nil, 0, m.err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var matched []*event.Event
	for _, e := range m.events {
		if e.TenantID == tenantID && e.CorrelationID == correlationID {
			cp := *e
			matched = append(matched, &cp)
		}
	}
	total := int64(len(matched))
	if offset >= len(matched) {
		return []*event.Event{}, total, nil
	}
	return matched[offset:min(offset+limit, len(matched))], total, nil
}

func (m *mockStore) ListByCausationID(_ context.Context, _, _ string, _, _ int) ([]*event.Event, int64, error) {
	return nil, 0, nil
}

func (m *mockStore) ListTimeline(_ context.Context, _ string, _, _ time.Time, _, _ int) ([]*event.Event, int64, error) {
	return nil, 0, nil
}

func (m *mockStore) QueryForReplay(_ context.Context, _ event.ReplayFilter, _ int) ([]*event.Event, error) {
	return nil, nil
}

// ---------------------------------------------------------------------------
// mockSearcher — in-memory event.Searcher.
// ---------------------------------------------------------------------------

type mockSearcher struct {
	events []*event.Event
	err    error
}

func (m *mockSearcher) Search(_ context.Context, q event.SearchQuery) ([]*event.Event, int64, error) {
	if m.err != nil {
		return nil, 0, m.err
	}
	var result []*event.Event
	for _, e := range m.events {
		if e.TenantID != q.TenantID {
			continue
		}
		if q.StreamID != "" && e.StreamID != q.StreamID {
			continue
		}
		cp := *e
		result = append(result, &cp)
	}
	return result, int64(len(result)), nil
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func authedCtx(tenantID string) context.Context {
	return appauth.WithIdentity(context.Background(), appauth.Identity{
		SubjectID: "test-subject",
		TenantID:  tenantID,
		Roles:     []string{"reader"},
	})
}

func makeEvent(tenantID, correlationID string) *event.Event {
	return &event.Event{
		ID:            uuid.New(),
		TenantID:      tenantID,
		StreamID:      "order:1",
		Type:          "order.created",
		Source:        "orders-svc",
		Version:       1,
		OccurredAt:    time.Now().UTC(),
		CorrelationID: correlationID,
	}
}

func newSvc(store *mockStore, searcher *mockSearcher) *Service {
	return NewService(store, searcher, discardLogger())
}

// ---------------------------------------------------------------------------
// ListAll — correlation_id filter
// ---------------------------------------------------------------------------

func TestListAll_WithCorrelationID_ReturnsMatchingEvents(t *testing.T) {
	store := &mockStore{events: []*event.Event{
		makeEvent("tenant-a", "corr-123"),
		makeEvent("tenant-a", "corr-123"),
		makeEvent("tenant-a", "corr-other"),
	}}
	svc := newSvc(store, &mockSearcher{})

	result, err := svc.ListAll(authedCtx("tenant-a"), ListQuery{
		Limit:         20,
		CorrelationID: "corr-123",
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Events) != 2 {
		t.Errorf("expected 2 events, got %d", len(result.Events))
	}
	if result.Total != 2 {
		t.Errorf("expected total=2, got %d", result.Total)
	}
	if result.ReadModel != "postgres" {
		t.Errorf("correlation_id filter must use postgres, got %q", result.ReadModel)
	}
}

func TestListAll_WithCorrelationID_RespectsTenanIsolation(t *testing.T) {
	store := &mockStore{events: []*event.Event{
		makeEvent("tenant-a", "corr-123"),
		makeEvent("tenant-b", "corr-123"), // different tenant, same correlation_id
	}}
	svc := newSvc(store, &mockSearcher{})

	result, err := svc.ListAll(authedCtx("tenant-a"), ListQuery{
		Limit:         20,
		CorrelationID: "corr-123",
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Events) != 1 {
		t.Errorf("expected 1 event for tenant-a, got %d", len(result.Events))
	}
	if result.Events[0].TenantID != "tenant-a" {
		t.Errorf("event must belong to tenant-a, got %q", result.Events[0].TenantID)
	}
}

func TestListAll_WithCorrelationID_NoMatches_ReturnsEmptySlice(t *testing.T) {
	store := &mockStore{events: []*event.Event{
		makeEvent("tenant-a", "corr-other"),
	}}
	svc := newSvc(store, &mockSearcher{})

	result, err := svc.ListAll(authedCtx("tenant-a"), ListQuery{
		Limit:         20,
		CorrelationID: "corr-no-match",
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Events == nil {
		t.Error("Events must be a non-nil empty slice, not nil")
	}
	if len(result.Events) != 0 {
		t.Errorf("expected 0 events, got %d", len(result.Events))
	}
}

func TestListAll_WithCorrelationID_PaginationApplied(t *testing.T) {
	events := make([]*event.Event, 5)
	for i := range events {
		events[i] = makeEvent("tenant-a", "corr-123")
	}
	store := &mockStore{events: events}
	svc := newSvc(store, &mockSearcher{})

	result, err := svc.ListAll(authedCtx("tenant-a"), ListQuery{
		Limit:         2,
		Offset:        1,
		CorrelationID: "corr-123",
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Events) != 2 {
		t.Errorf("expected page of 2, got %d", len(result.Events))
	}
	if result.Total != 5 {
		t.Errorf("expected total=5, got %d", result.Total)
	}
	if result.Limit != 2 {
		t.Errorf("expected limit=2, got %d", result.Limit)
	}
	if result.Offset != 1 {
		t.Errorf("expected offset=1, got %d", result.Offset)
	}
}

func TestListAll_WithCorrelationID_StoreError_Propagated(t *testing.T) {
	store := &mockStore{err: errors.New("connection lost")}
	svc := newSvc(store, &mockSearcher{})

	_, err := svc.ListAll(authedCtx("tenant-a"), ListQuery{
		Limit:         20,
		CorrelationID: "corr-123",
	})

	if err == nil {
		t.Fatal("store error must be returned to caller")
	}
}

// ---------------------------------------------------------------------------
// ListAll — without correlation_id (elasticsearch path)
// ---------------------------------------------------------------------------

func TestListAll_WithoutCorrelationID_UsesSearcher(t *testing.T) {
	searcher := &mockSearcher{events: []*event.Event{
		makeEvent("tenant-a", ""),
		makeEvent("tenant-a", ""),
	}}
	svc := newSvc(&mockStore{}, searcher)

	result, err := svc.ListAll(authedCtx("tenant-a"), ListQuery{Limit: 20})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Events) != 2 {
		t.Errorf("expected 2 events, got %d", len(result.Events))
	}
	if result.ReadModel != "elasticsearch" {
		t.Errorf("no correlation_id must use elasticsearch, got %q", result.ReadModel)
	}
}

func TestListAll_NoIdentity_ReturnsError(t *testing.T) {
	svc := newSvc(&mockStore{}, &mockSearcher{})

	_, err := svc.ListAll(context.Background(), ListQuery{Limit: 20, CorrelationID: "corr-123"})

	if err == nil {
		t.Fatal("missing identity must return an error")
	}
}

func TestListAll_LimitClamped(t *testing.T) {
	searcher := &mockSearcher{}
	svc := newSvc(&mockStore{}, searcher)

	result, err := svc.ListAll(authedCtx("tenant-a"), ListQuery{Limit: 9999})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Limit != maxLimit {
		t.Errorf("limit must be clamped to %d, got %d", maxLimit, result.Limit)
	}
}

// ---------------------------------------------------------------------------
// GetByID
// ---------------------------------------------------------------------------

func TestGetByID_ReturnsEvent(t *testing.T) {
	e := makeEvent("tenant-a", "corr-1")
	store := &mockStore{events: []*event.Event{e}}
	svc := newSvc(store, &mockSearcher{})

	got, err := svc.GetByID(authedCtx("tenant-a"), e.ID)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.ID != e.ID {
		t.Errorf("ID = %v, want %v", got.ID, e.ID)
	}
}

func TestGetByID_CrossTenantReturnsNotFound(t *testing.T) {
	e := makeEvent("tenant-b", "corr-1")
	store := &mockStore{events: []*event.Event{e}}
	svc := newSvc(store, &mockSearcher{})

	_, err := svc.GetByID(authedCtx("tenant-a"), e.ID)

	if !errors.Is(err, event.ErrNotFound) {
		t.Errorf("cross-tenant access must return ErrNotFound, got: %v", err)
	}
}

func TestGetByID_UnknownIDReturnsNotFound(t *testing.T) {
	svc := newSvc(&mockStore{}, &mockSearcher{})

	_, err := svc.GetByID(authedCtx("tenant-a"), uuid.New())

	if !errors.Is(err, event.ErrNotFound) {
		t.Errorf("unknown id must return ErrNotFound, got: %v", err)
	}
}
