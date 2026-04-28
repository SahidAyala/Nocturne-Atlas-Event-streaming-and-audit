-- 002_add_correlation_id.sql
-- Persist correlation_id on the event row so the full lifecycle can be traced
-- from HTTP ingress through PostgreSQL, Kafka, and Elasticsearch without
-- relying on any transport layer (headers, context values, etc.).
-- Default '' keeps the column NOT NULL for existing rows written before this migration.
ALTER TABLE events ADD COLUMN IF NOT EXISTS correlation_id TEXT NOT NULL DEFAULT '';
