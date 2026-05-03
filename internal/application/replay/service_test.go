package replay

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
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
// Append assigns versions exactly as PostgreSQL does (MAX+1 per stream).
// inject() bypasses that logic so tests can seed arbitrary states,
// including gaps, to validate the service's defensive checks.
// ---------------------------------------------------------------------------

type mockStore struct {
	mu     sync.Mutex
	events []*event.Event
	err    error // non-nil → every read returns this error
}

// inject adds events directly without version assignment.
// Use this to simulate corrupt or out-of-order store states.
func (m *mockStore) inject(events ...*event.Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, events...)
}

func (m *mockStore) Append(_ context.Context, e *event.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var maxV int64
	for _, s := range m.events {
		if s.StreamID == e.StreamID && s.Version > maxV {
			maxV = s.Version
		}
	}
	e.Version = maxV + 1
	cp := *e
	m.events = append(m.events, &cp)
	return nil
}

func (m *mockStore) GetByStreamID(ctx context.Context, streamID string) ([]*event.Event, error) {
	return m.GetFromVersion(ctx, streamID, 0)
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
	if m.err != nil {
		return nil, m.err
	}
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
	end := offset + limit
	if end > len(matched) {
		end = len(matched)
	}
	return matched[offset:end], total, nil
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func makeEvent(streamID string, version int64) *event.Event {
	return &event.Event{
		ID:         uuid.New(),
		StreamID:   streamID,
		Type:       "test.event",
		Source:     "test-svc",
		Version:    version,
		OccurredAt: time.Now().UTC(),
	}
}

// authedCtx returns a context carrying a test Identity with the given tenantID.
func authedCtx(tenantID string) context.Context {
	return appauth.WithIdentity(context.Background(), appauth.Identity{
		SubjectID: "test-subject",
		TenantID:  tenantID,
	})
}

// ---------------------------------------------------------------------------
// validateContiguous — pure function, tested directly.
//
// These tests are the tightest specification of the gap-detection invariant.
// ---------------------------------------------------------------------------

func TestValidateContiguous_NilIsValid(t *testing.T) {
	if err := validateContiguous(nil); err != nil {
		t.Fatalf("nil slice must be valid, got: %v", err)
	}
}

func TestValidateContiguous_EmptyIsValid(t *testing.T) {
	if err := validateContiguous([]*event.Event{}); err != nil {
		t.Fatalf("empty slice must be valid, got: %v", err)
	}
}

func TestValidateContiguous_SingleEventIsValid(t *testing.T) {
	if err := validateContiguous([]*event.Event{makeEvent("s", 1)}); err != nil {
		t.Fatalf("single event must be valid, got: %v", err)
	}
}

func TestValidateContiguous_ContiguousSequence(t *testing.T) {
	events := []*event.Event{
		makeEvent("s", 1),
		makeEvent("s", 2),
		makeEvent("s", 3),
	}
	if err := validateContiguous(events); err != nil {
		t.Fatalf("contiguous [1,2,3] must be valid, got: %v", err)
	}
}

func TestValidateContiguous_GapInMiddle(t *testing.T) {
	events := []*event.Event{
		makeEvent("s", 1),
		makeEvent("s", 2),
		makeEvent("s", 4), // version 3 is missing
	}
	err := validateContiguous(events)
	if err == nil {
		t.Fatal("gap [1,2,4] must produce an error")
	}
	if !strings.Contains(err.Error(), "missing version 3") {
		t.Errorf("error must identify the missing version, got: %v", err)
	}
}

func TestValidateContiguous_GapAtStart(t *testing.T) {
	// from_version=1 requested, store returns [2,3] — version 1 was not found
	// but from the contiguity checker's perspective [2,3] is contiguous.
	// The gap at the start (1 was requested but 2 is first) is NOT a contiguity
	// error — it means version 1 does not exist, which is a valid empty range.
	events := []*event.Event{
		makeEvent("s", 2),
		makeEvent("s", 3),
	}
	if err := validateContiguous(events); err != nil {
		t.Fatalf("[2,3] is contiguous — no gap error expected, got: %v", err)
	}
}

func TestValidateContiguous_DuplicateVersionIsAGap(t *testing.T) {
	// [1, 1, 2]: second event has same version as first.
	// version[1]=1 != version[0]+1=2 → gap detected.
	events := []*event.Event{
		makeEvent("s", 1),
		makeEvent("s", 1),
		makeEvent("s", 2),
	}
	if err := validateContiguous(events); err == nil {
		t.Fatal("duplicate version must be detected as a gap")
	}
}

// ---------------------------------------------------------------------------
// replay.Service — behavior tests
// ---------------------------------------------------------------------------

func TestReplayService_EmptyStream(t *testing.T) {
	svc := NewService(&mockStore{}, discardLogger())

	result, err := svc.Replay(authedCtx("tenant-a"), Command{StreamID: "order:1", FromVersion: 0})

	if err != nil {
		t.Fatalf("empty stream must not error, got: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 events for empty stream, got %d", len(result))
	}
}

func TestReplayService_ReturnsAllEventsOrdered(t *testing.T) {
	store := &mockStore{}
	// Inject 5 events in order — simulates a healthy stream.
	for v := int64(1); v <= 5; v++ {
		store.inject(makeEvent("order:1", v))
	}
	svc := NewService(store, discardLogger())

	result, err := svc.Replay(authedCtx("tenant-a"), Command{StreamID: "order:1", FromVersion: 0})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 5 {
		t.Fatalf("expected 5 events, got %d", len(result))
	}
	for i, e := range result {
		if want := int64(i + 1); e.Version != want {
			t.Errorf("result[%d].Version = %d, want %d", i, e.Version, want)
		}
	}
}

func TestReplayService_FromVersionIsInclusive(t *testing.T) {
	store := &mockStore{}
	for v := int64(1); v <= 5; v++ {
		store.inject(makeEvent("order:1", v))
	}
	svc := NewService(store, discardLogger())

	result, err := svc.Replay(authedCtx("tenant-a"), Command{StreamID: "order:1", FromVersion: 3})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("from_version=3 on stream [1..5] must return [3,4,5], got %d events", len(result))
	}
	if result[0].Version != 3 {
		t.Errorf("first event version = %d, want 3", result[0].Version)
	}
	if result[2].Version != 5 {
		t.Errorf("last event version = %d, want 5", result[2].Version)
	}
}

func TestReplayService_FromVersionBeyondStreamIsEmpty(t *testing.T) {
	store := &mockStore{}
	store.inject(makeEvent("order:1", 1), makeEvent("order:1", 2))
	svc := NewService(store, discardLogger())

	// Requesting from version 99 on a stream that only has [1,2] returns empty — not an error.
	result, err := svc.Replay(authedCtx("tenant-a"), Command{StreamID: "order:1", FromVersion: 99})

	if err != nil {
		t.Fatalf("from_version beyond stream end must not error, got: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 events, got %d", len(result))
	}
}

func TestReplayService_GapCausesError(t *testing.T) {
	// The store has been corrupted: version 3 is absent.
	// The service must refuse to return a partial sequence and fail explicitly.
	store := &mockStore{}
	store.inject(
		makeEvent("order:1", 1),
		makeEvent("order:1", 2),
		makeEvent("order:1", 4), // version 3 missing
	)
	svc := NewService(store, discardLogger())

	_, err := svc.Replay(authedCtx("tenant-a"), Command{StreamID: "order:1", FromVersion: 0})

	if err == nil {
		t.Fatal("a version gap must cause Replay to return an error")
	}
	if !strings.Contains(err.Error(), "missing version 3") {
		t.Errorf("error must name the missing version; got: %v", err)
	}
}

func TestReplayService_DoesNotCrossStreamBoundaries(t *testing.T) {
	// Two streams share the same store. Replay for A must never include B's events.
	store := &mockStore{}
	store.inject(makeEvent("stream:A", 1))
	store.inject(makeEvent("stream:B", 1))
	store.inject(makeEvent("stream:A", 2))
	store.inject(makeEvent("stream:B", 2))
	svc := NewService(store, discardLogger())

	result, err := svc.Replay(authedCtx("tenant-a"), Command{StreamID: "stream:A", FromVersion: 0})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 events for stream:A, got %d", len(result))
	}
	for _, e := range result {
		if e.StreamID != "stream:A" {
			t.Errorf("result contains event from wrong stream: %s", e.StreamID)
		}
	}
}

func TestReplayService_RejectsEmptyStreamID(t *testing.T) {
	svc := NewService(&mockStore{}, discardLogger())

	_, err := svc.Replay(authedCtx("tenant-a"), Command{StreamID: "", FromVersion: 0})

	if err == nil {
		t.Fatal("empty stream_id must return an error")
	}
}

func TestReplayService_RejectsNegativeFromVersion(t *testing.T) {
	svc := NewService(&mockStore{}, discardLogger())

	_, err := svc.Replay(authedCtx("tenant-a"), Command{StreamID: "order:1", FromVersion: -1})

	if err == nil {
		t.Fatal("negative from_version must return an error")
	}
}

func TestReplayService_PropagatesStoreError(t *testing.T) {
	store := &mockStore{err: errors.New("connection pool exhausted")}
	svc := NewService(store, discardLogger())

	_, err := svc.Replay(authedCtx("tenant-a"), Command{StreamID: "order:1", FromVersion: 0})

	if err == nil {
		t.Fatal("store error must be propagated by the service")
	}
}

func TestReplayService_RejectsRequestWithNoIdentity(t *testing.T) {
	svc := NewService(&mockStore{}, discardLogger())

	_, err := svc.Replay(context.Background(), Command{StreamID: "order:1", FromVersion: 0})

	if err == nil {
		t.Fatal("Replay without Identity in context must return an error")
	}
}
