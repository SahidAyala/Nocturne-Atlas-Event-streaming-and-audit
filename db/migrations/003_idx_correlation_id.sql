-- 003_idx_correlation_id.sql
-- Index correlation_id to support point lookups when tracing a request
-- across the full lifecycle (HTTP → Postgres → Kafka → Elasticsearch).
-- btree is sufficient; correlation IDs are UUIDs or chi request IDs.
CREATE INDEX IF NOT EXISTS idx_events_correlation_id ON events (correlation_id);
