package consume

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/domain/event"
)

// ---------------------------------------------------------------------------
// mockSubscriber — synchronous delivery of a predefined event list.
// ---------------------------------------------------------------------------

type mockSubscriber struct {
	events []*event.Event
}

func (m *mockSubscriber) Run(ctx context.Context, handle func(context.Context, *event.Event) error) error {
	for _, e := range m.events {
		if err := handle(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// mockIndexer — stateful in-memory event.Indexer.
//
// Stores events keyed by UUID, mirroring Elasticsearch's idempotent upsert.
// ---------------------------------------------------------------------------

type mockIndexer struct {
	mu      sync.Mutex
	indexed map[uuid.UUID]*event.Event
	calls   int
	failFor uuid.UUID // Index returns error when called with this ID
}

func newMockIndexer() *mockIndexer {
	return &mockIndexer{indexed: make(map[uuid.UUID]*event.Event)}
}

func (m *mockIndexer) Index(_ context.Context, e *event.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if e.ID == m.failFor {
		return errors.New("elasticsearch shard unavailable")
	}
	cp := *e
	m.indexed[e.ID] = &cp
	return nil
}

func (m *mockIndexer) Close() error { return nil }

func (m *mockIndexer) get(id uuid.UUID) (*event.Event, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.indexed[id]
	return e, ok
}

func (m *mockIndexer) size() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.indexed)
}

// ---------------------------------------------------------------------------
// mockDLQPublisher — stateful in-memory DLQPublisher.
//
// Accumulates DLQMessages so tests can assert on what was routed to DLQ.
// failNext simulates DLQ unavailability without scripting per-call responses.
// ---------------------------------------------------------------------------

type mockDLQPublisher struct {
	mu       sync.Mutex
	messages []DLQMessage
	failNext bool
}

func (m *mockDLQPublisher) Publish(_ context.Context, msg DLQMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failNext {
		m.failNext = false
		return errors.New("kafka dlq broker unreachable")
	}
	m.messages = append(m.messages, msg)
	return nil
}

func (m *mockDLQPublisher) Close() error { return nil }

func (m *mockDLQPublisher) received() []DLQMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]DLQMessage, len(m.messages))
	copy(out, m.messages)
	return out
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

func newSvc(sub *mockSubscriber, idx *mockIndexer, dlq *mockDLQPublisher) *Service {
	return NewService(sub, idx, dlq, discardLogger())
}

// ---------------------------------------------------------------------------
// Happy-path tests — unchanged behaviour
// ---------------------------------------------------------------------------

func TestConsume_EventReachesIndexer(t *testing.T) {
	e := makeEvent("order:1", 1)
	sub := &mockSubscriber{events: []*event.Event{e}}
	idx := newMockIndexer()
	svc := newSvc(sub, idx, &mockDLQPublisher{})

	if err := svc.Run(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	indexed, ok := idx.get(e.ID)
	if !ok {
		t.Fatal("event must be present in the indexer after Run")
	}
	if indexed.StreamID != e.StreamID || indexed.Version != e.Version {
		t.Errorf("indexed event does not match original: got stream=%s v=%d, want stream=%s v=%d",
			indexed.StreamID, indexed.Version, e.StreamID, e.Version)
	}
}

func TestConsume_AllEventsFromSubscriberAreIndexed(t *testing.T) {
	events := []*event.Event{
		makeEvent("order:1", 1),
		makeEvent("order:1", 2),
		makeEvent("order:1", 3),
	}
	sub := &mockSubscriber{events: events}
	idx := newMockIndexer()
	svc := newSvc(sub, idx, &mockDLQPublisher{})

	if err := svc.Run(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if idx.size() != 3 {
		t.Errorf("expected 3 indexed events, got %d", idx.size())
	}
	for _, e := range events {
		if _, ok := idx.get(e.ID); !ok {
			t.Errorf("event %s (v%d) not found in indexer", e.ID, e.Version)
		}
	}
}

func TestConsume_IdempotentIndexing(t *testing.T) {
	e := makeEvent("order:1", 1)
	sub := &mockSubscriber{events: []*event.Event{e, e}}
	idx := newMockIndexer()
	svc := newSvc(sub, idx, &mockDLQPublisher{})

	if err := svc.Run(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if idx.calls != 2 {
		t.Errorf("indexer must be called once per delivery: expected 2, got %d", idx.calls)
	}
	if idx.size() != 1 {
		t.Errorf("idempotent indexer must hold 1 entry after 2 identical deliveries, got %d", idx.size())
	}
}

// ---------------------------------------------------------------------------
// DLQ behaviour tests
// ---------------------------------------------------------------------------

func TestConsume_IndexerErrorSentToDLQ(t *testing.T) {
	// When the indexer fails, the event must appear in the DLQ —
	// not be silently discarded, not trigger a retry.
	e := makeEvent("order:1", 1)
	idx := newMockIndexer()
	idx.failFor = e.ID
	dlq := &mockDLQPublisher{}
	svc := newSvc(&mockSubscriber{events: []*event.Event{e}}, idx, dlq)

	if err := svc.Run(context.Background()); err != nil {
		t.Fatalf("indexer failure must not cause Run to return an error, got: %v", err)
	}

	msgs := dlq.received()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 DLQ message, got %d", len(msgs))
	}
}

func TestConsume_DLQMessageContainsOriginalEvent(t *testing.T) {
	// The DLQ message must carry the full original event so it can be reprocessed.
	// Reason must be non-empty so on-call can diagnose without reading logs.
	e := makeEvent("order:1", 42)
	idx := newMockIndexer()
	idx.failFor = e.ID
	dlq := &mockDLQPublisher{}
	svc := newSvc(&mockSubscriber{events: []*event.Event{e}}, idx, dlq)

	svc.Run(context.Background()) //nolint:errcheck

	msgs := dlq.received()
	if len(msgs) == 0 {
		t.Fatal("no DLQ message received")
	}
	msg := msgs[0]

	if msg.Event == nil {
		t.Fatal("DLQ message must contain the original event")
	}
	if msg.Event.ID != e.ID {
		t.Errorf("DLQ event ID = %s, want %s", msg.Event.ID, e.ID)
	}
	if msg.Event.Version != e.Version {
		t.Errorf("DLQ event version = %d, want %d", msg.Event.Version, e.Version)
	}
	if msg.Reason == "" {
		t.Error("DLQ message Reason must not be empty")
	}
	if msg.FailedAt.IsZero() {
		t.Error("DLQ message FailedAt must be set")
	}
}

func TestConsume_DLQPublishFailureDoesNotCrash(t *testing.T) {
	// If both the indexer and the DLQ publisher fail, the service must
	// continue — it logs the double failure and commits the offset.
	// The event is recoverable from PostgreSQL via the replay engine.
	e := makeEvent("order:1", 1)
	idx := newMockIndexer()
	idx.failFor = e.ID
	dlq := &mockDLQPublisher{failNext: true}
	svc := newSvc(&mockSubscriber{events: []*event.Event{e}}, idx, dlq)

	err := svc.Run(context.Background())

	if err != nil {
		t.Fatalf("double failure (index + DLQ) must not crash Run, got: %v", err)
	}
}

func TestConsume_ConsumerContinuesAfterDLQ(t *testing.T) {
	// After routing one event to DLQ the consumer must continue processing
	// the remaining events — a single failure must not halt the pipeline.
	failing := makeEvent("order:1", 1)
	healthy := makeEvent("order:1", 2)
	idx := newMockIndexer()
	idx.failFor = failing.ID
	dlq := &mockDLQPublisher{}
	svc := newSvc(&mockSubscriber{events: []*event.Event{failing, healthy}}, idx, dlq)

	if err := svc.Run(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// failing event: in DLQ, not in index
	if _, ok := idx.get(failing.ID); ok {
		t.Error("failing event must not be in the index")
	}
	if len(dlq.received()) != 1 {
		t.Errorf("expected 1 DLQ message, got %d", len(dlq.received()))
	}

	// healthy event: indexed successfully
	if _, ok := idx.get(healthy.ID); !ok {
		t.Error("healthy event must be indexed after the failing one was routed to DLQ")
	}
}

func TestConsume_SuccessfulEventsNotSentToDLQ(t *testing.T) {
	// Events that index successfully must never appear in the DLQ.
	events := []*event.Event{
		makeEvent("order:1", 1),
		makeEvent("order:1", 2),
	}
	dlq := &mockDLQPublisher{}
	svc := newSvc(&mockSubscriber{events: events}, newMockIndexer(), dlq)

	svc.Run(context.Background()) //nolint:errcheck

	if len(dlq.received()) != 0 {
		t.Errorf("DLQ must be empty when all events index successfully, got %d messages", len(dlq.received()))
	}
}
