# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Planned
- Prometheus metrics endpoint (`/metrics`)
- S3/MinIO cold archival for long-term event retention
- Event snapshots for high-volume stream recovery
- Per-stream schema validation with a schema registry

---

## [0.1.0] — 2026-06-23

Initial public release of the Event Streaming & Audit platform.

### Added

**Event Store (PostgreSQL)**
- Append-only event log with per-stream monotonic versioning (`stream_id + version` unique constraint)
- Atomic writes — each `POST /events` is a single PostgreSQL transaction
- Embedded SQL migration runner — migrations live alongside the binary, applied on first run
- Full query surface: by stream, by ID, by correlation ID, by causation ID, tenant timeline

**Kafka Integration**
- Event publishing to configurable topics after successful PostgreSQL write
- Best-effort publish semantics — Kafka failures are logged but do not fail ingest requests
- Dead-letter queue (DLQ) routing for failed Elasticsearch indexing operations
- Domain-based topic routing via `KAFKA_TOPIC_ROUTES` (optional, defaults to single topic)
- KRaft mode (no ZooKeeper dependency) via `apache/kafka:3.7`

**Elasticsearch Read Model**
- Asynchronous event indexing from the Kafka consumer
- Idempotent indexing using event UUID as the document ID
- `GET /events/{streamID}` query API backed by Elasticsearch
- Full-text and structured query support via Elasticsearch DSL

**HTTP Ingest & Query API** (`cmd/ingest-api`)
- `POST /events` — ingest events, returns the persisted event with assigned version
- `GET /events/{streamID}` — query stream events from the read model
- `GET /events/{id}` — direct event lookup by UUID (strongly consistent, reads from PostgreSQL)
- `GET /events` — list events with optional `correlation_id`, `limit`, `offset` filters
- `GET /events/timeline` — tenant event timeline with time-range filter
- `GET /events/{id}/causes` — causation-tree traversal
- `POST /replay` — replay events from PostgreSQL with dry-run support
- `POST /tenants` — tenant provisioning (admin-key protected)
- `POST /auth/token` — JWT token validation and scope discovery
- Embedded Swagger UI at `/swagger/index.html`
- Structured JSON logging (stdlib `log/slog`)
- W3C `traceparent` propagation — trace IDs extracted from HTTP headers and stored per event

**gRPC Replay Service** (`cmd/replay-service`)
- `ReplayStream` RPC — streaming ordered events from PostgreSQL by stream ID
- Protobuf schema at `proto/events.proto`

**Authentication**
- Three modes: `none` (dev), `simple` (API key), `jwt` (HMAC-HS256 Bearer tokens)
- Tenant isolation enforced at the application layer — all queries scoped to `tenant_id`
- Constant-time API key comparison (prevents timing attacks)
- Middleware chain: auth → trace propagation → handler

**Developer Experience**
- `make dev` — infrastructure in Docker, API runs locally for fast iteration
- `make dev-full` — entire stack in Docker (production-like)
- `make test-e2e` — end-to-end smoke test validating the full pipeline
- `make migrate` — embedded migration runner, idempotent
- `make swag` — Swagger doc regeneration from annotations
- `make proto` — protobuf stub generation
- Kafka UI at `:8081`, Kibana at `:5601`, MinIO console at `:9001`

**Operational**
- RUNBOOK.md — incident reconstruction, correlation-based debugging, replay procedures, DLQ recovery
- Healthcheck endpoint `/health` — no authentication required

### Known Limitations

- No Prometheus metrics endpoint. Observability is currently limited to structured logs.
- No S3/MinIO archival. MinIO is provisioned but archival logic is not implemented.
- No event snapshots. High-volume streams require full replay from version 1.
- No schema registry or payload validation beyond JSON well-formedness.
- Elasticsearch is configured without TLS in the default docker-compose. Enable security for production.
- No rate limiting. Enforce request limits at the reverse proxy layer.
- `AUTH_MODE=none` is the default in `.env.example` for frictionless local development — always override in production.
- Integration tests (Postgres, Kafka, Elasticsearch) are not yet part of the CI pipeline; only unit tests run in CI.

### Tech Stack

| Concern | Technology |
|---|---|
| Language | Go 1.24 |
| Event store | PostgreSQL 16 |
| Event bus | Kafka 3.7 (KRaft mode) |
| Read model | Elasticsearch 8.13 |
| Cold storage | MinIO (S3-compatible) |
| HTTP router | chi v5 |
| gRPC | google.golang.org/grpc |
| Auth | Static API key / HMAC-HS256 JWT |
| API docs | Swaggo (OpenAPI 2.0) |

[Unreleased]: https://github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/releases/tag/v0.1.0
