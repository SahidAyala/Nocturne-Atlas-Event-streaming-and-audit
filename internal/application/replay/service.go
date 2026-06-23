// Package replay provides two services:
//
//   - Service: read-only replay — reads events from PostgreSQL by stream+version
//     and validates contiguity. Used internally for audit and debugging.
//
//   - ReplayEngine: active replay — re-ingests events matching a filter with
//     replay metadata attached. Used for recovery (ADR-015, ADR-016).
package replay

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	appauth "github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/application/auth"
	"github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/domain/event"
	"github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/pkg/trace"
)

// --- Read-only Service (stream-based replay for internal use) ---

// Command carries the parameters for a read-only stream replay request.
type Command struct {
	StreamID    string
	FromVersion int64 // inclusive; 0 = from the very first event
}

// Service replays events from PostgreSQL (source of truth) — read-only.
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

	s.log.Info("read-only replay completed",
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

// --- Active ReplayEngine (re-ingest with replay metadata) ---

const defaultReplaySafetyLimit = 1000

// ReplayRequest specifies which events to replay and how.
type ReplayRequest struct {
	Filter  event.ReplayFilter
	Options ReplayOptions
}

// ReplayOptions controls replay behaviour.
type ReplayOptions struct {
	// DryRun returns matched events without creating new events or publishing to Kafka.
	DryRun bool
	// ReplayReason is required for active replays. Stored permanently on each replayed event.
	ReplayReason string
	// SafetyLimit caps how many events a single replay call can create.
	// Defaults to 1000. The API returns ErrReplayLimitExceeded if the filter matches more.
	SafetyLimit int
}

// ReplayResult is the outcome of a ReplayRequest.
type ReplayResult struct {
	// ReplayID is the batch UUID assigned to this replay (empty for dry-runs).
	ReplayID string
	// DryRun reflects whether this was a dry-run.
	DryRun bool
	// MatchedCount is the number of original events that matched the filter.
	MatchedCount int
	// ReplayedCount is the number of new events created (0 for dry-runs).
	ReplayedCount int
	// Events is:
	//   - dry-run: the original matched events (for operator inspection)
	//   - active:  the newly created replay events
	Events []*event.Event
}

// ReplayEngine re-ingests events matching a filter with replay metadata attached.
// Each replayed event is a new row in the store with is_replay=true and a
// pointer back to the original. The original events are never modified.
//
// See ADR-015 for design rationale.
type ReplayEngine struct {
	store     event.Store
	publisher event.Publisher
	log       *slog.Logger
}

// NewReplayEngine returns a configured replay engine.
// publisher is required for active replays; dry-runs do not call it.
func NewReplayEngine(store event.Store, publisher event.Publisher, log *slog.Logger) *ReplayEngine {
	return &ReplayEngine{store: store, publisher: publisher, log: log}
}

// Execute runs a replay request. Requires an authenticated identity in ctx.
//
// For dry-run requests: queries matched events and returns them; no writes.
// For active requests: queries matched events, creates replay events, and publishes to Kafka.
func (e *ReplayEngine) Execute(ctx context.Context, req ReplayRequest) (*ReplayResult, error) {
	identity, ok := appauth.IdentityFromContext(ctx)
	if !ok || identity.TenantID == "" {
		return nil, fmt.Errorf("unauthenticated: identity with tenant_id is required")
	}

	if !req.Options.DryRun && req.Options.ReplayReason == "" {
		return nil, fmt.Errorf("replay_reason is required for active replays")
	}

	if req.Filter.TenantID == "" && len(req.Filter.EventIDs) == 0 {
		return nil, fmt.Errorf("replay filter must specify tenant_id or event_ids")
	}
	// Enforce tenant scope: the filter tenant must match the caller's identity.
	if req.Filter.TenantID != "" && req.Filter.TenantID != identity.TenantID {
		return nil, fmt.Errorf("replay filter tenant_id does not match authenticated tenant")
	}
	// When filtering by EventIDs without tenantId, scope to the caller's tenant.
	if req.Filter.TenantID == "" {
		req.Filter.TenantID = identity.TenantID
	}

	limit := req.Options.SafetyLimit
	if limit <= 0 {
		limit = defaultReplaySafetyLimit
	}

	matched, err := e.store.QueryForReplay(ctx, req.Filter, limit)
	if err != nil {
		e.log.Error("replay query failed",
			"correlation_id", trace.FromContext(ctx),
			"tenant_id", identity.TenantID,
			"dry_run", req.Options.DryRun,
			"error", err,
		)
		return nil, fmt.Errorf("query events for replay: %w", err)
	}

	if req.Options.DryRun {
		e.log.Info("replay dry-run completed",
			"correlation_id", trace.FromContext(ctx),
			"tenant_id", identity.TenantID,
			"matched_count", len(matched),
		)
		return &ReplayResult{
			DryRun:       true,
			MatchedCount: len(matched),
			Events:       matched,
		}, nil
	}

	// Active replay: create new events with replay metadata.
	replayID := uuid.NewString()
	now := time.Now().UTC()

	replayed := make([]*event.Event, 0, len(matched))
	for _, orig := range matched {
		newEvent := event.New(
			orig.StreamID,
			orig.Type,
			orig.Source,
			orig.CorrelationID,
			orig.Payload,
			cloneMetadata(orig.Metadata),
		)
		newEvent.TenantID = orig.TenantID
		newEvent.EventVersion = orig.EventVersion
		newEvent.CausationID = orig.CausationID
		newEvent.ActorID = orig.ActorID
		newEvent.TraceID = orig.TraceID
		newEvent.SourceVersion = orig.SourceVersion

		// Replay metadata.
		newEvent.IsReplay = true
		newEvent.ReplayID = replayID
		newEvent.ReplayedAt = &now
		newEvent.ReplayReason = req.Options.ReplayReason
		newEvent.ReplaySourceEventID = orig.ID.String()

		// Propagate replay metadata into the Kafka message's metadata map so
		// consumers without the replay struct fields can still inspect replay status.
		newEvent.Metadata["is_replay"] = "true"
		newEvent.Metadata["replay_id"] = replayID
		newEvent.Metadata["replay_source_event_id"] = orig.ID.String()
		newEvent.Metadata["replay_reason"] = req.Options.ReplayReason

		if err := e.store.Append(ctx, newEvent); err != nil {
			e.log.Error("replay: failed to append event",
				"correlation_id", trace.FromContext(ctx),
				"replay_id", replayID,
				"source_event_id", orig.ID,
				"error", err,
			)
			return nil, fmt.Errorf("replay: append event %s: %w", orig.ID, err)
		}
		replayed = append(replayed, newEvent)
	}

	// Publish all replayed events to Kafka best-effort.
	publishFailures := 0
	for _, ev := range replayed {
		if err := e.publisher.Publish(ctx, ev); err != nil {
			publishFailures++
			e.log.Warn("replay: kafka publish failed (event is durable in store)",
				"replay_id", replayID,
				"event_id", ev.ID,
				"error", err,
			)
		}
	}

	e.log.Info("active replay completed",
		"correlation_id", trace.FromContext(ctx),
		"tenant_id", identity.TenantID,
		"replay_id", replayID,
		"matched_count", len(matched),
		"replayed_count", len(replayed),
		"publish_failures", publishFailures,
		"replay_reason", req.Options.ReplayReason,
	)

	return &ReplayResult{
		ReplayID:      replayID,
		DryRun:        false,
		MatchedCount:  len(matched),
		ReplayedCount: len(replayed),
		Events:        replayed,
	}, nil
}

func cloneMetadata(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
