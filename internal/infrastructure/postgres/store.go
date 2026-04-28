package postgres

import (
	"context"
	"encoding/json"
	"fmt"

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
ALTER TABLE events ADD COLUMN IF NOT EXISTS tenant_id      TEXT NOT NULL DEFAULT 'default';
ALTER TABLE events ADD COLUMN IF NOT EXISTS correlation_id TEXT NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_events_stream_id       ON events (stream_id);
CREATE INDEX IF NOT EXISTS idx_events_type            ON events (type);
CREATE INDEX IF NOT EXISTS idx_events_occurred_at     ON events (occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_tenant_id       ON events (tenant_id);
CREATE INDEX IF NOT EXISTS idx_events_correlation_id  ON events (correlation_id);
`

// EventStore is a PostgreSQL-backed append-only event store.
type EventStore struct {
	pool *pgxpool.Pool
}

func NewEventStore(ctx context.Context, cfg config.PostgresConfig) (*EventStore, error) {
	pool, err := pgxpool.New(ctx, cfg.DSN)
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
func (s *EventStore) Append(ctx context.Context, e *event.Event) error {
	meta, err := json.Marshal(e.Metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	err = s.pool.QueryRow(ctx, `
		INSERT INTO events (id, tenant_id, stream_id, type, source, version, occurred_at, payload, metadata, correlation_id)
		VALUES (
			$1, $2, $3, $4, $5,
			(SELECT COALESCE(MAX(version), 0) + 1 FROM events WHERE stream_id = $3),
			$6, $7, $8, $9
		)
		RETURNING version`,
		e.ID, e.TenantID, e.StreamID, e.Type, e.Source, e.OccurredAt, e.Payload, meta, e.CorrelationID,
	).Scan(&e.Version)
	if err != nil {
		return fmt.Errorf("insert event: %w", err)
	}
	return nil
}

// GetByStreamID returns all events for a stream ordered by version ascending.
// If an Identity is present in ctx the results are filtered to that tenant.
func (s *EventStore) GetByStreamID(ctx context.Context, streamID string) ([]*event.Event, error) {
	identity, hasIdentity := appauth.IdentityFromContext(ctx)

	var rows pgx.Rows
	var err error

	if hasIdentity && identity.TenantID != "" {
		rows, err = s.pool.Query(ctx, `
			SELECT id, tenant_id, stream_id, type, source, version, occurred_at, payload, metadata, correlation_id
			FROM events
			WHERE stream_id = $1 AND tenant_id = $2
			ORDER BY version ASC`,
			streamID, identity.TenantID,
		)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT id, tenant_id, stream_id, type, source, version, occurred_at, payload, metadata, correlation_id
			FROM events
			WHERE stream_id = $1
			ORDER BY version ASC`,
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
// ordered by version ASC. This is the source-of-truth query for the replay engine.
// If an Identity is present in ctx the results are filtered to that tenant.
func (s *EventStore) GetFromVersion(ctx context.Context, streamID string, fromVersion int64) ([]*event.Event, error) {
	identity, hasIdentity := appauth.IdentityFromContext(ctx)

	var rows pgx.Rows
	var err error

	if hasIdentity && identity.TenantID != "" {
		rows, err = s.pool.Query(ctx, `
			SELECT id, tenant_id, stream_id, type, source, version, occurred_at, payload, metadata, correlation_id
			FROM events
			WHERE stream_id = $1 AND version >= $2 AND tenant_id = $3
			ORDER BY version ASC`,
			streamID, fromVersion, identity.TenantID,
		)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT id, tenant_id, stream_id, type, source, version, occurred_at, payload, metadata, correlation_id
			FROM events
			WHERE stream_id = $1 AND version >= $2
			ORDER BY version ASC`,
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
func (s *EventStore) GetByID(ctx context.Context, id uuid.UUID) (*event.Event, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, tenant_id, stream_id, type, source, version, occurred_at, payload, metadata, correlation_id
		FROM events WHERE id = $1`, id)

	e, err := scanEvent(row)
	if err != nil {
		return nil, fmt.Errorf("get by id: %w", err)
	}
	return e, nil
}

func (s *EventStore) Close() {
	s.pool.Close()
}

// scanner is satisfied by both pgx.Row and pgx.Rows.
type scanner interface {
	Scan(dest ...any) error
}

func scanEvent(s scanner) (*event.Event, error) {
	var e event.Event
	var meta []byte
	if err := s.Scan(
		&e.ID, &e.TenantID, &e.StreamID, &e.Type, &e.Source, &e.Version,
		&e.OccurredAt, &e.Payload, &meta, &e.CorrelationID,
	); err != nil {
		return nil, fmt.Errorf("scan event: %w", err)
	}
	if err := json.Unmarshal(meta, &e.Metadata); err != nil {
		return nil, fmt.Errorf("unmarshal metadata: %w", err)
	}
	return &e, nil
}

func scanRows(rows pgx.Rows) ([]*event.Event, error) {
	var events []*event.Event
	for rows.Next() {
		e, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}
