package ingest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	appauth "github.com/SheykoWk/event-streaming-and-audit/internal/application/auth"
	"github.com/SheykoWk/event-streaming-and-audit/internal/domain/event"
)

// ---------------------------------------------------------------------------
// mockStore — stateful in-memory event.Store.
//
// Assigns versions the same way PostgreSQL does: MAX(version)+1 per stream.
// This means the test for monotonic versioning is a real invariant check,
// not a trivially scripted one.
// ---------------------------------------------------------------------------

type mockStore struct {
	mu       sync.Mutex
	events   []*event.Event
	failNext bool
}

func (m *mockStore) Append(_ context.Context, e *event.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failNext {
		m.failNext = false
		return errors.New("store unavailable")
	}
	var maxV int64
	for _, s := range m.events {
		if s.StreamID == e.StreamID && s.Version > maxV {
			maxV = s.Version
		}
	}
	e.Version = maxV + 1 // mutates the caller's event, mirroring real Postgres behaviour
	cp := *e
	m.events = append(m.events, &cp)
	return nil
}

func (m *mockStore) GetByStreamID(_ context.Context, streamID string) ([]*event.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*event.Event
	for _, e := range m.events {
		if e.StreamID == streamID {
			cp := *e
			result = append(result, &cp)
		}
	}
	return result, nil
}

func (m *mockStore) GetByID(_ context.Context, id uuid.UUID) (*event.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.events {
		if e.ID == id {
			cp := *e
			return &cp, nil
		}
	}
	return nil, errors.New("event not found")
}

func (m *mockStore) GetFromVersion(_ context.Context, streamID string, fromVersion int64) ([]*event.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*event.Event
	for _, e := range m.events {
		if e.StreamID == streamID && e.Version >= fromVersion {
			cp := *e
			result = append(result, &cp)
		}
	}
	return result, nil
}

func (m *mockStore) ListByCorrelationID(_ context.Context, tenantID, correlationID string, limit, offset int) ([]*event.Event, int64, error) {
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
	end := offset + limit
	if end > len(matched) {
		end = len(matched)
	}
	return matched[offset:end], total, nil
}

func (m *mockStore) ListByCausationID(_ context.Context, tenantID, causationID string, limit, offset int) ([]*event.Event, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var matched []*event.Event
	for _, e := range m.events {
		if e.TenantID == tenantID && e.CausationID == causationID {
			cp := *e
			matched = append(matched, &cp)
		}
	}
	total := int64(len(matched))
	if offset >= len(matched) {
		return []*event.Event{}, total, nil
	}
	end := offset + limit
	if end > len(matched) {
		end = len(matched)
	}
	return matched[offset:end], total, nil
}

func (m *mockStore) ListTimeline(_ context.Context, tenantID string, _, _ time.Time, limit, offset int) ([]*event.Event, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var matched []*event.Event
	for _, e := range m.events {
		if e.TenantID == tenantID {
			cp := *e
			matched = append(matched, &cp)
		}
	}
	total := int64(len(matched))
	if offset >= len(matched) {
		return []*event.Event{}, total, nil
	}
	end := offset + limit
	if end > len(matched) {
		end = len(matched)
	}
	return matched[offset:end], total, nil
}

func (m *mockStore) QueryForReplay(_ context.Context, f event.ReplayFilter, safetyLimit int) ([]*event.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var matched []*event.Event
	for _, e := range m.events {
		if f.TenantID != "" && e.TenantID != f.TenantID {
			continue
		}
		matched = append(matched, e)
		if len(matched) > safetyLimit {
			return nil, event.ErrReplayLimitExceeded
		}
	}
	return matched, nil
}

// count returns how many events are stored for the given stream.
func (m *mockStore) count(streamID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, e := range m.events {
		if e.StreamID == streamID {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// mockPublisher — stateful in-memory event.Publisher.
//
// failNext causes the next Publish call to return an error.
// This lets us verify the "best-effort publish" contract without scripting
// per-call responses.
// ---------------------------------------------------------------------------

type mockPublisher struct {
	mu        sync.Mutex
	published []*event.Event
	failNext  bool
}

func (m *mockPublisher) Publish(_ context.Context, e *event.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failNext {
		m.failNext = false
		return errors.New("kafka broker unreachable")
	}
	cp := *e
	m.published = append(m.published, &cp)
	return nil
}

func (m *mockPublisher) Close() error { return nil }

func (m *mockPublisher) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.published)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newSvc(store *mockStore, pub *mockPublisher) *Service {
	return NewService(store, pub, discardLogger())
}

// authedCtx returns a context carrying a test Identity with the given tenantID.
func authedCtx(tenantID string) context.Context {
	return appauth.WithIdentity(context.Background(), appauth.Identity{
		SubjectID: "test-subject",
		TenantID:  tenantID,
		Roles:     []string{"writer"},
	})
}

// ---------------------------------------------------------------------------
// ingest.Service — behavior tests
// ---------------------------------------------------------------------------

func TestIngest_EventPersistedAndPublished(t *testing.T) {
	store := &mockStore{}
	pub := &mockPublisher{}
	svc := newSvc(store, pub)

	e, err := svc.Ingest(authedCtx("tenant-a"), Command{
		StreamID: "order:1",
		Type:     "order.created",
		Source:   "orders-svc",
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store.count("order:1") != 1 {
		t.Error("event must be persisted in the store")
	}
	if pub.count() != 1 {
		t.Error("event must be published to the bus")
	}
	// The returned event reflects the version assigned by the store.
	if e.Version != 1 {
		t.Errorf("first event in stream must have version 1, got %d", e.Version)
	}
	if e.TenantID != "tenant-a" {
		t.Errorf("event TenantID = %q, want %q", e.TenantID, "tenant-a")
	}
}

func TestIngest_TenantIDSetFromIdentity(t *testing.T) {
	// TenantID on the stored event must come from the Identity in context,
	// not from any caller-supplied field.
	store := &mockStore{}
	svc := newSvc(store, &mockPublisher{})

	e, err := svc.Ingest(authedCtx("acme-corp"), Command{
		StreamID: "order:1", Type: "order.created", Source: "svc",
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.TenantID != "acme-corp" {
		t.Errorf("TenantID = %q, want %q", e.TenantID, "acme-corp")
	}
}

func TestIngest_RejectsRequestWithNoIdentity(t *testing.T) {
	svc := newSvc(&mockStore{}, &mockPublisher{})

	_, err := svc.Ingest(context.Background(), Command{
		StreamID: "order:1", Type: "order.created", Source: "svc",
	})

	if err == nil {
		t.Fatal("Ingest without Identity in context must return an error")
	}
}

func TestIngest_VersionsAreMonotonic(t *testing.T) {
	store := &mockStore{}
	svc := newSvc(store, &mockPublisher{})

	var versions []int64
	for i := 0; i < 3; i++ {
		e, err := svc.Ingest(authedCtx("tenant-a"), Command{
			StreamID: "order:1",
			Type:     "order.updated",
			Source:   "orders-svc",
		})
		if err != nil {
			t.Fatalf("ingest %d failed: %v", i, err)
		}
		versions = append(versions, e.Version)
	}

	for i, v := range versions {
		if want := int64(i + 1); v != want {
			t.Errorf("event[%d] version = %d, want %d", i, v, want)
		}
	}
}

func TestIngest_MultipleStreamsHaveIndependentVersions(t *testing.T) {
	// Streams A and B must start at version 1 independently.
	// Inserting into A must not affect B's next version.
	store := &mockStore{}
	svc := newSvc(store, &mockPublisher{})

	ingestFn := func(streamID string) *event.Event {
		e, err := svc.Ingest(authedCtx("tenant-a"), Command{
			StreamID: streamID, Type: "evt", Source: "svc",
		})
		if err != nil {
			t.Fatalf("ingest into %s failed: %v", streamID, err)
		}
		return e
	}

	a1 := ingestFn("stream:A")
	ingestFn("stream:B")
	a2 := ingestFn("stream:A")
	b2 := ingestFn("stream:B")

	if a1.Version != 1 || a2.Version != 2 {
		t.Errorf("stream:A versions must be [1,2], got [%d,%d]", a1.Version, a2.Version)
	}
	if b2.Version != 2 {
		t.Errorf("stream:B second event must be version 2, got %d", b2.Version)
	}
}

func TestIngest_KafkaFailureDoesNotRollbackPersistence(t *testing.T) {
	// This is the most critical ingest invariant:
	// "store first, publish best-effort".
	// A Kafka outage must never cause an event to be lost from PostgreSQL.
	store := &mockStore{}
	pub := &mockPublisher{failNext: true}
	svc := newSvc(store, pub)

	e, err := svc.Ingest(authedCtx("tenant-a"), Command{
		StreamID: "order:1", Type: "order.created", Source: "svc",
	})

	if err != nil {
		t.Fatalf("Kafka failure must not fail Ingest, got: %v", err)
	}
	if e == nil {
		t.Fatal("Ingest must return the persisted event even when publish fails")
	}
	if store.count("order:1") != 1 {
		t.Error("event must be in the store even when Kafka is down")
	}
	if pub.count() != 0 {
		t.Error("publish was expected to fail, but succeeded")
	}
}

func TestIngest_EmptyRequiredFields(t *testing.T) {
	svc := newSvc(&mockStore{}, &mockPublisher{})

	cases := []struct {
		name string
		cmd  Command
	}{
		{"missing stream_id", Command{StreamID: "", Type: "t", Source: "s"}},
		{"missing type", Command{StreamID: "s", Type: "", Source: "s"}},
		{"missing source", Command{StreamID: "s", Type: "t", Source: ""}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &mockStore{}
			pub := &mockPublisher{}
			svc = newSvc(store, pub)

			_, err := svc.Ingest(authedCtx("tenant-a"), tc.cmd)

			if err == nil {
				t.Fatal("expected validation error, got nil")
			}
			if len(store.events) != 0 {
				t.Error("store must be empty when validation fails")
			}
			if pub.count() != 0 {
				t.Error("publisher must not be called when validation fails")
			}
		})
	}
}

func TestIngest_StoreFailurePreventsPublish(t *testing.T) {
	// If persisting fails, Kafka must never receive the event.
	// Publishing an event that isn't in the store is a consistency violation.
	store := &mockStore{failNext: true}
	pub := &mockPublisher{}
	svc := newSvc(store, pub)

	_, err := svc.Ingest(authedCtx("tenant-a"), Command{
		StreamID: "order:1", Type: "order.created", Source: "svc",
	})

	if err == nil {
		t.Fatal("store failure must cause Ingest to return an error")
	}
	if pub.count() != 0 {
		t.Error("publisher must not be called when store fails")
	}
}
