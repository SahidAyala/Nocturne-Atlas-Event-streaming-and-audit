package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	appauth "github.com/SheykoWk/event-streaming-and-audit/internal/application/auth"
	"github.com/SheykoWk/event-streaming-and-audit/internal/config"
	"github.com/SheykoWk/event-streaming-and-audit/internal/domain/event"
)

const ddl = `
CREATE TABLE IF NOT EXISTS events (
    id             UUID        PRIMARY KEY,
    tenant_id      TEXT        NOT NULL DEFAULT 'default',
    stream_id      TEXT        NOT NULL,
    type           TEXT        NOT NULL,
    source         TEXT        NOT NULL,
    version        BIGINT      NOT NULL,
    occurred_at    TIMESTAMPTZ NOT NULL,
    payload        JSONB       NOT NULL,
    metadata       JSONB       NOT NULL DEFAULT '{}',
    correlation_id TEXT        NOT NULL DEFAULT '',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (stream_id, version)
);
-- Idempotent column additions — safe to run on existing databases.
ALTER TABLE events ADD COLUMN IF NOT EXISTS tenant_id      TEXT NOT NULL DEFAULT 'default';
ALTER TABLE events ADD COLUMN IF NOT EXISTS correlation_id TEXT NOT NULL DEFAULT '';
-- event_version: schema/contract version of the event type (ADR-013).
ALTER TABLE events ADD COLUMN IF NOT EXISTS event_version  INT  NOT NULL DEFAULT 1;
-- causation_id: ID of the upstream event that caused this one (ADR-010).
ALTER TABLE events ADD COLUMN IF NOT EXISTS causation_id   TEXT NOT NULL DEFAULT '';
-- actor_id: who triggered the action (user UUID, "system", service name).
ALTER TABLE events ADD COLUMN IF NOT EXISTS actor_id       TEXT NOT NULL DEFAULT '';
-- trace_id: W3C distributed trace ID — 32 lowercase hex chars (ADR-014).
ALTER TABLE events ADD COLUMN IF NOT EXISTS trace_id       TEXT NOT NULL DEFAULT '';
-- source_version: semantic version of the originating service.
ALTER TABLE events ADD COLUMN IF NOT EXISTS source_version TEXT NOT NULL DEFAULT '';
-- Replay fields (ADR-015): populated only on events created by POST /replay.
ALTER TABLE events ADD COLUMN IF NOT EXISTS is_replay             BOOLEAN    NOT NULL DEFAULT FALSE;
ALTER TABLE events ADD COLUMN IF NOT EXISTS replay_id             TEXT       NOT NULL DEFAULT '';
ALTER TABLE events ADD COLUMN IF NOT EXISTS replayed_at           TIMESTAMPTZ;
ALTER TABLE events ADD COLUMN IF NOT EXISTS replay_reason         TEXT       NOT NULL DEFAULT '';
ALTER TABLE events ADD COLUMN IF NOT EXISTS replay_source_event_id TEXT      NOT NULL DEFAULT '';
-- Indexes
CREATE INDEX IF NOT EXISTS idx_events_stream_id             ON events (stream_id);
CREATE INDEX IF NOT EXISTS idx_events_type                  ON events (type);
CREATE INDEX IF NOT EXISTS idx_events_occurred_at           ON events (occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_tenant_id             ON events (tenant_id);
CREATE INDEX IF NOT EXISTS idx_events_correlation_id        ON events (correlation_id);
-- causation_id index supports causation-chain reconstruction (ADR-017).
CREATE INDEX IF NOT EXISTS idx_events_causation_id          ON events (causation_id) WHERE causation_id != '';
-- actor_id index supports per-actor audit queries.
CREATE INDEX IF NOT EXISTS idx_events_actor_id              ON events (tenant_id, actor_id) WHERE actor_id != '';
-- replay_id index supports "show all events from replay batch X".
CREATE INDEX IF NOT EXISTS idx_events_replay_id             ON events (replay_id) WHERE replay_id != '';
-- Partial index for replay events (is_replay = true is a minority of rows).
CREATE INDEX IF NOT EXISTS idx_events_is_replay             ON events (tenant_id, occurred_at DESC) WHERE is_replay = TRUE;
`

// EventStore is a PostgreSQL-backed append-only event store.
type EventStore struct {
	pool *pgxpool.Pool
}

func NewEventStore(ctx context.Context, cfg config.PostgresConfig) (*EventStore, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse postgres config: %w", err)
	}
	if cfg.PoolMax > 0 {
		poolCfg.MaxConns = int32(cfg.PoolMax)
	}
	if cfg.PoolMin > 0 {
		poolCfg.MinConns = int32(cfg.PoolMin)
	}
	if cfg.PoolMaxConnIdleTime > 0 {
		poolCfg.MaxConnIdleTime = cfg.PoolMaxConnIdleTime
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	if _, err := pool.Exec(ctx, ddl); err != nil {
		pool.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}

	return &EventStore{pool: pool}, nil
}

// Append inserts the event and writes the DB-assigned version back onto e.
// Version is calculated as MAX(version)+1 for the stream; the UNIQUE constraint
// on (stream_id, version) prevents concurrent duplicates.
//
// Idempotency: if an event with the same UUID already exists (e.g. the outbox
// retried a previously-successful forward), the existing row is returned without
// error and e.Version is populated from the existing record.
func (s *EventStore) Append(ctx context.Context, e *event.Event) error {
	meta, err := json.Marshal(e.Metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	err = s.pool.QueryRow(ctx, `
		INSERT INTO events (
			id, tenant_id, stream_id, type, source, version,
			event_version, occurred_at, payload, metadata,
			correlation_id, causation_id, actor_id, trace_id, source_version,
			is_replay, replay_id, replayed_at, replay_reason, replay_source_event_id
		)
		VALUES (
			$1, $2, $3, $4, $5,
			(SELECT COALESCE(MAX(version), 0) + 1 FROM events WHERE stream_id = $3),
			$6, $7, $8, $9, $10, $11, $12, $13, $14,
			$15, $16, $17, $18, $19
		)
		ON CONFLICT (id) DO UPDATE
			-- On duplicate event ID: update nothing, just return the existing version.
			-- Makes Append idempotent: a retried outbox forward never creates a second row.
			SET id = EXCLUDED.id
		RETURNING version`,
		e.ID, e.TenantID, e.StreamID, e.Type, e.Source,
		e.EventVersion, e.OccurredAt, e.Payload, meta,
		e.CorrelationID, e.CausationID, e.ActorID, e.TraceID, e.SourceVersion,
		e.IsReplay, e.ReplayID, e.ReplayedAt, e.ReplayReason, e.ReplaySourceEventID,
	).Scan(&e.Version)
	if err != nil {
		return fmt.Errorf("insert event: %w", err)
	}
	return nil
}

// selectCols is the canonical column list for all SELECT queries.
// Keep in sync with scanEvent's Scan() call — column count and order must match exactly.
const selectCols = ` id, tenant_id, stream_id, type, source, version, event_version,
	occurred_at, payload, metadata, correlation_id, causation_id,
	actor_id, trace_id, source_version,
	is_replay, replay_id, replayed_at, replay_reason, replay_source_event_id `

// GetByStreamID returns all events for a stream ordered by version ascending.
// If an Identity is present in ctx the results are filtered to that tenant.
func (s *EventStore) GetByStreamID(ctx context.Context, streamID string) ([]*event.Event, error) {
	identity, hasIdentity := appauth.IdentityFromContext(ctx)

	var rows pgx.Rows
	var err error

	if hasIdentity && identity.TenantID != "" {
		rows, err = s.pool.Query(ctx,
			`SELECT`+selectCols+`FROM events WHERE stream_id = $1 AND tenant_id = $2 ORDER BY version ASC`,
			streamID, identity.TenantID,
		)
	} else {
		rows, err = s.pool.Query(ctx,
			`SELECT`+selectCols+`FROM events WHERE stream_id = $1 ORDER BY version ASC`,
			streamID,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("query by stream: %w", err)
	}
	defer rows.Close()

	return scanRows(rows)
}

// GetFromVersion returns all events for a stream with version >= fromVersion,
// ordered by version ASC. fromVersion=0 returns all events from the beginning.
// If an Identity is present in ctx the results are filtered to that tenant.
func (s *EventStore) GetFromVersion(ctx context.Context, streamID string, fromVersion int64) ([]*event.Event, error) {
	identity, hasIdentity := appauth.IdentityFromContext(ctx)

	var rows pgx.Rows
	var err error

	if hasIdentity && identity.TenantID != "" {
		rows, err = s.pool.Query(ctx,
			`SELECT`+selectCols+`FROM events WHERE stream_id = $1 AND version >= $2 AND tenant_id = $3 ORDER BY version ASC`,
			streamID, fromVersion, identity.TenantID,
		)
	} else {
		rows, err = s.pool.Query(ctx,
			`SELECT`+selectCols+`FROM events WHERE stream_id = $1 AND version >= $2 ORDER BY version ASC`,
			streamID, fromVersion,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("query from version: %w", err)
	}
	defer rows.Close()

	return scanRows(rows)
}

// GetByID returns a single event by its UUID.
// Returns event.ErrNotFound when no row matches.
func (s *EventStore) GetByID(ctx context.Context, id uuid.UUID) (*event.Event, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT`+selectCols+`FROM events WHERE id = $1`, id)

	e, err := scanEvent(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, event.ErrNotFound
		}
		return nil, fmt.Errorf("get by id: %w", err)
	}
	return e, nil
}

// ListByCorrelationID returns a paginated, occurred_at DESC page of events for a tenant
// filtered by correlation_id. total reflects the full match count (window function).
func (s *EventStore) ListByCorrelationID(ctx context.Context, tenantID, correlationID string, limit, offset int) ([]*event.Event, int64, error) {
	return s.listWithCount(ctx,
		`WHERE tenant_id = $1 AND correlation_id = $2 ORDER BY occurred_at DESC`,
		`LIMIT $3 OFFSET $4`,
		tenantID, correlationID, limit, offset,
	)
}

// ListByCausationID returns events whose causation_id matches causationID for the given tenant,
// ordered by occurred_at ASC. Used for causation-tree traversal (ADR-017).
func (s *EventStore) ListByCausationID(ctx context.Context, tenantID, causationID string, limit, offset int) ([]*event.Event, int64, error) {
	return s.listWithCount(ctx,
		`WHERE tenant_id = $1 AND causation_id = $2 ORDER BY occurred_at ASC`,
		`LIMIT $3 OFFSET $4`,
		tenantID, causationID, limit, offset,
	)
}

// ListTimeline returns all events for a tenant within an optional time range, ordered
// by occurred_at DESC. Zero fromTime/toTime means no bound. Used for tenant timeline
// reconstruction (ADR-017).
func (s *EventStore) ListTimeline(ctx context.Context, tenantID string, fromTime, toTime time.Time, limit, offset int) ([]*event.Event, int64, error) {
	var clauses []string
	var args []any
	argIdx := 1

	clauses = append(clauses, fmt.Sprintf("tenant_id = $%d", argIdx))
	args = append(args, tenantID)
	argIdx++

	if !fromTime.IsZero() {
		clauses = append(clauses, fmt.Sprintf("occurred_at >= $%d", argIdx))
		args = append(args, fromTime)
		argIdx++
	}
	if !toTime.IsZero() {
		clauses = append(clauses, fmt.Sprintf("occurred_at <= $%d", argIdx))
		args = append(args, toTime)
		argIdx++
	}

	where := "WHERE " + strings.Join(clauses, " AND ") + " ORDER BY occurred_at DESC"
	page := fmt.Sprintf("LIMIT $%d OFFSET $%d", argIdx, argIdx+1)
	args = append(args, limit, offset)

	return s.listWithCount(ctx, where, page, args...)
}

// QueryForReplay returns events matching the filter for use by the replay engine.
// Returns ErrReplayLimitExceeded when the filter would match more than safetyLimit events.
// Results are ordered by occurred_at ASC so they replay in chronological order.
func (s *EventStore) QueryForReplay(ctx context.Context, f event.ReplayFilter, safetyLimit int) ([]*event.Event, error) {
	var clauses []string
	var args []any
	argIdx := 1

	if len(f.EventIDs) > 0 {
		// Specific IDs override all other filters.
		placeholders := make([]string, len(f.EventIDs))
		for i, id := range f.EventIDs {
			placeholders[i] = fmt.Sprintf("$%d", argIdx)
			args = append(args, id)
			argIdx++
		}
		clauses = append(clauses, fmt.Sprintf("id::text IN (%s)", strings.Join(placeholders, ",")))
		if f.TenantID != "" {
			clauses = append(clauses, fmt.Sprintf("tenant_id = $%d", argIdx))
			args = append(args, f.TenantID)
			argIdx++
		}
	} else {
		if f.TenantID == "" {
			return nil, fmt.Errorf("replay filter: tenant_id is required when event_ids is not specified")
		}
		clauses = append(clauses, fmt.Sprintf("tenant_id = $%d", argIdx))
		args = append(args, f.TenantID)
		argIdx++

		if f.StreamID != "" {
			clauses = append(clauses, fmt.Sprintf("stream_id = $%d", argIdx))
			args = append(args, f.StreamID)
			argIdx++
		}
		if f.CorrelationID != "" {
			clauses = append(clauses, fmt.Sprintf("correlation_id = $%d", argIdx))
			args = append(args, f.CorrelationID)
			argIdx++
		}
		if f.EventType != "" {
			clauses = append(clauses, fmt.Sprintf("type = $%d", argIdx))
			args = append(args, f.EventType)
			argIdx++
		}
		if f.ActorID != "" {
			clauses = append(clauses, fmt.Sprintf("actor_id = $%d", argIdx))
			args = append(args, f.ActorID)
			argIdx++
		}
		if !f.FromTime.IsZero() {
			clauses = append(clauses, fmt.Sprintf("occurred_at >= $%d", argIdx))
			args = append(args, f.FromTime)
			argIdx++
		}
		if !f.ToTime.IsZero() {
			clauses = append(clauses, fmt.Sprintf("occurred_at <= $%d", argIdx))
			args = append(args, f.ToTime)
			argIdx++
		}
	}

	where := "WHERE " + strings.Join(clauses, " AND ")
	// Fetch safetyLimit+1 to detect whether the limit would be exceeded.
	limitArg := fmt.Sprintf("$%d", argIdx)
	args = append(args, safetyLimit+1)

	q := `SELECT` + selectCols + `FROM events ` + where + ` ORDER BY occurred_at ASC LIMIT ` + limitArg
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query for replay: %w", err)
	}
	defer rows.Close()

	evts, err := scanRows(rows)
	if err != nil {
		return nil, err
	}
	if len(evts) > safetyLimit {
		return nil, fmt.Errorf("%w: matched at least %d events (limit %d)",
			event.ErrReplayLimitExceeded, len(evts), safetyLimit)
	}
	return evts, nil
}

func (s *EventStore) Close() {
	s.pool.Close()
}

// listWithCount is a shared helper that runs a SELECT with COUNT(*) OVER()
// for any paginated query. where and page are SQL fragments; args are their values.
// The caller is responsible for correct parameterisation order ($1, $2, ..., $N, $limit, $offset).
func (s *EventStore) listWithCount(ctx context.Context, where, page string, args ...any) ([]*event.Event, int64, error) {
	q := `SELECT ` + selectCols + `, COUNT(*) OVER() AS _total FROM events ` + where + ` ` + page
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list events: %w", err)
	}
	defer rows.Close()

	var evts []*event.Event
	var total int64
	for rows.Next() {
		e, rowTotal, scanErr := scanEventWithTotal(rows)
		if scanErr != nil {
			return nil, 0, scanErr
		}
		total = rowTotal
		evts = append(evts, e)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("rows error: %w", err)
	}
	return evts, total, nil
}

// scanner is satisfied by both pgx.Row and pgx.Rows.
type scanner interface {
	Scan(dest ...any) error
}

// scanEvent scans all 20 canonical columns from a row.
func scanEvent(s scanner) (*event.Event, error) {
	var e event.Event
	var meta []byte
	if err := s.Scan(
		&e.ID, &e.TenantID, &e.StreamID, &e.Type, &e.Source, &e.Version, &e.EventVersion,
		&e.OccurredAt, &e.Payload, &meta, &e.CorrelationID, &e.CausationID,
		&e.ActorID, &e.TraceID, &e.SourceVersion,
		&e.IsReplay, &e.ReplayID, &e.ReplayedAt, &e.ReplayReason, &e.ReplaySourceEventID,
	); err != nil {
		return nil, fmt.Errorf("scan event: %w", err)
	}
	if err := json.Unmarshal(meta, &e.Metadata); err != nil {
		return nil, fmt.Errorf("unmarshal metadata: %w", err)
	}
	return &e, nil
}

// scanEventWithTotal scans selectCols + one extra total column (COUNT(*) OVER()).
func scanEventWithTotal(s scanner) (*event.Event, int64, error) {
	var e event.Event
	var meta []byte
	var total int64
	if err := s.Scan(
		&e.ID, &e.TenantID, &e.StreamID, &e.Type, &e.Source, &e.Version, &e.EventVersion,
		&e.OccurredAt, &e.Payload, &meta, &e.CorrelationID, &e.CausationID,
		&e.ActorID, &e.TraceID, &e.SourceVersion,
		&e.IsReplay, &e.ReplayID, &e.ReplayedAt, &e.ReplayReason, &e.ReplaySourceEventID,
		&total,
	); err != nil {
		return nil, 0, fmt.Errorf("scan event with total: %w", err)
	}
	if err := json.Unmarshal(meta, &e.Metadata); err != nil {
		return nil, 0, fmt.Errorf("unmarshal metadata: %w", err)
	}
	return &e, total, nil
}

func scanRows(rows pgx.Rows) ([]*event.Event, error) {
	var evts []*event.Event
	for rows.Next() {
		e, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		evts = append(evts, e)
	}
	return evts, rows.Err()
}
