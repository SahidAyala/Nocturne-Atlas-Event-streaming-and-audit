package replay

import (
	"context"
	"fmt"
	"log/slog"

	appauth "github.com/SheykoWk/event-streaming-and-audit/internal/application/auth"
	"github.com/SheykoWk/event-streaming-and-audit/internal/domain/event"
	"github.com/SheykoWk/event-streaming-and-audit/internal/pkg/trace"
)

// Command carries the parameters for a replay request.
type Command struct {
	StreamID    string
	FromVersion int64 // inclusive; 0 = from the very first event
}

// Service replays events from PostgreSQL (source of truth).
// It does not touch Elasticsearch or Kafka.
type Service struct {
	store event.Store
	log   *slog.Logger
}

func NewService(store event.Store, log *slog.Logger) *Service {
	return &Service{store: store, log: log}
}

// Replay reads events for a stream starting at fromVersion, validates that the
// returned sequence is contiguous (no version gaps), and returns the ordered slice.
// Requires an Identity in ctx for tenant scoping — returns an error if absent.
// A detected gap is a data-integrity violation and causes the replay to fail —
// returning partial data would produce a corrupt audit trail.
func (s *Service) Replay(ctx context.Context, cmd Command) ([]*event.Event, error) {
	identity, ok := appauth.IdentityFromContext(ctx)
	if !ok || identity.TenantID == "" {
		return nil, fmt.Errorf("unauthenticated: identity with tenant_id is required")
	}

	if cmd.StreamID == "" {
		return nil, fmt.Errorf("stream_id is required")
	}
	if cmd.FromVersion < 0 {
		return nil, fmt.Errorf("from_version must be >= 0")
	}

	events, err := s.store.GetFromVersion(ctx, cmd.StreamID, cmd.FromVersion)
	if err != nil {
		s.log.Error("failed to read events from store",
			"correlation_id", trace.FromContext(ctx),
			"stream_id", cmd.StreamID,
			"from_version", cmd.FromVersion,
			"tenant_id", identity.TenantID,
			"error", err,
		)
		return nil, fmt.Errorf("read events from store: %w", err)
	}

	if err := validateContiguous(events); err != nil {
		s.log.Error("version gap detected during replay",
			"correlation_id", trace.FromContext(ctx),
			"stream_id", cmd.StreamID,
			"tenant_id", identity.TenantID,
			"from_version", cmd.FromVersion,
			"event_count", len(events),
			"error", err,
		)
		return nil, err
	}

	s.log.Info("replay completed",
		"correlation_id", trace.FromContext(ctx),
		"stream_id", cmd.StreamID,
		"tenant_id", identity.TenantID,
		"from_version", cmd.FromVersion,
		"event_count", len(events),
	)
	return events, nil
}

// validateContiguous asserts that every consecutive pair of events has
// version[i] == version[i-1]+1. An empty or single-event slice is always valid.
// This is O(n) over the returned slice with no additional I/O.
func validateContiguous(events []*event.Event) error {
	for i := 1; i < len(events); i++ {
		prev, curr := events[i-1].Version, events[i].Version
		if curr != prev+1 {
			return fmt.Errorf("version gap in stream %q: missing version %d (found %d after %d)",
				events[i].StreamID, prev+1, curr, prev)
		}
	}
	return nil
}
