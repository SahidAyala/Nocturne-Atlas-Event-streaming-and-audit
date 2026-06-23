# Nocturne Atlas — Event Streaming & Audit

A production-grade, append-only event streaming platform that gives you durable event ingestion, a Kafka-based fan-out, an Elasticsearch read model, and an auditable replay engine — all as a single self-contained Go service.

[![CI](https://github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/actions/workflows/ci.yml/badge.svg)](https://github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/actions/workflows/ci.yml)
[![Go 1.24](https://img.shields.io/badge/Go-1.24-00ADD8?logo=go)](https://golang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## Why This Exists

Most systems bolt on event streaming as an afterthought — Kafka or a message bus becomes the source of truth, events are lost when consumers are down, and auditing requires piecing together logs from multiple systems.

This platform flips that model:

- **PostgreSQL is always the source of truth.** Events are durable the moment `POST /events` returns `201`. Kafka and Elasticsearch are projections that can be rebuilt at any time.
- **Replay is first-class.** Any event or time window can be replayed from PostgreSQL into the pipeline without modifying the original data.
- **Every event is traceable.** Correlation IDs, causation IDs, W3C trace IDs, and actor IDs are stored on every event, enabling full causal-chain reconstruction after an incident.

---

## Key Features

- **Append-only event store** backed by PostgreSQL with per-stream monotonic versioning
- **Kafka fan-out** with best-effort publish semantics — Kafka failures never lose events
- **Dead-letter queue** for failed Elasticsearch indexing — no silent data loss
- **Elasticsearch read model** for efficient stream queries
- **Replay engine** with dry-run support — preview before re-ingesting
- **Causation and correlation tracking** — traverse causal chains across services
- **Multi-tenant** — tenant isolation enforced at every database query
- **Three auth modes** — `none` (dev), API key, or HMAC-HS256 JWT
- **Embedded Swagger UI** — interactive API docs with no separate container
- **gRPC replay service** — streaming ordered events directly from PostgreSQL

---

## Architecture Overview

```
HTTP POST /events
      │
      ▼
 ingest-api ──► PostgreSQL (source of truth, append-only)
      │
      └──► Kafka events.v1 (best-effort, async)
                │
                ▼
       consumer-service
                │
          ┌─────┴──────────┐
          ▼                ▼
  Elasticsearch       events.v1.dlq
  (read model)        (failed ops)

HTTP GET /events/:streamID ──► Elasticsearch (eventual consistency)
HTTP GET /events/:id       ──► PostgreSQL    (strong consistency)
gRPC ReplayStream          ──► PostgreSQL    (strong consistency)
```

Three binaries, one Docker image (build-arg selects the binary):

| Binary | Port | Responsibility |
|---|---|---|
| `cmd/ingest-api` | `8080` (HTTP), `50051` (gRPC) | Ingest, query, replay HTTP API + gRPC replay |
| `cmd/consumer-service` | — | Kafka → Elasticsearch indexer + DLQ router |
| `cmd/replay-service` | `50051` (gRPC) | Standalone gRPC replay server (optional) |

See [docs/architecture.md](docs/architecture.md) for the full architectural breakdown including layer boundaries, data flow, storage model, and deployment topology.

---

## Quick Start

Get from `git clone` to a working system in under 5 minutes.

**Prerequisites:** Go 1.24+, Docker, Docker Compose

```bash
# 1. Clone
git clone https://github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit.git
cd Nocturne-Atlas-Event-streaming-and-audit

# 2. Configure (defaults work out of the box)
cp .env.example .env

# 3. Start everything
make dev-full

# 4. Apply database migrations (first time only)
make migrate

# 5. Verify the full pipeline
make test-e2e
```

The stack is ready when you see the `ingest-api` health check pass.

---

## Running Locally

### Mode 1 — Dev (recommended for day-to-day coding)

Infrastructure in Docker, application services run locally with `go run` for fast iteration.

```bash
# Terminal 1 — start infrastructure + ingest-api
make dev

# Terminal 2 — start Kafka consumer (needed for GET /events to return results)
make dev-consumer

# Apply migrations on first run
make migrate
```

### Mode 2 — Full stack (integration testing and demos)

Everything in Docker, production-like:

```bash
make dev-full          # foreground (shows all logs)
# or
make up && make logs   # detached
```

### Service ports

| Service | Port | Notes |
|---|---|---|
| `ingest-api` | `8080` | HTTP API + Swagger UI |
| PostgreSQL | `5434` | Exposed as `5434` to avoid conflicts with a local Postgres on `5432` |
| Kafka | `9094` | External listener for host-side tooling |
| Elasticsearch | `9200` | REST API |
| Kafka UI | `8081` | Browse topics and messages |
| Kibana | `5601` | Explore the events index |
| MinIO | `9000` / `9001` | S3 API / Console |

### Verify it works

```bash
# Ingest an event
curl -X POST http://localhost:8080/events \
  -H "X-API-Key: dev-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "stream_id": "order:1",
    "type":      "order.created",
    "source":    "orders-svc",
    "payload":   {"amount": 99.99, "currency": "USD"}
  }'

# Wait ~3s for Kafka → consumer → Elasticsearch, then query
sleep 3
curl http://localhost:8080/events/order:1 \
  -H "X-API-Key: dev-api-key"
```

Or run the automated smoke test:

```bash
make test-e2e
```

### Swagger UI

Interactive API docs are available at `http://localhost:8080/swagger/index.html`.

To authenticate: click **Authorize** → enter `dev-api-key` → click **Authorize**.

---

## Configuration

All configuration is via environment variables. Copy `.env.example` to `.env` and adjust.

```bash
cp .env.example .env
```

| Variable | Required | Default | Description |
|---|---|---|---|
| `HTTP_ADDR` | No | `:8080` | HTTP listen address |
| `GRPC_ADDR` | No | `:50051` | gRPC listen address |
| `POSTGRES_DSN` | No | `postgres://events:events@localhost:5434/events?sslmode=disable` | PostgreSQL connection string |
| `POSTGRES_POOL_MAX` | No | `20` | Maximum DB connections per instance |
| `POSTGRES_POOL_MIN` | No | `2` | Minimum DB connections kept alive |
| `POSTGRES_POOL_IDLE_SECS` | No | `300` | Seconds before idle connections are closed |
| `KAFKA_BROKERS` | No | `localhost:9094` | Comma-separated Kafka broker addresses |
| `KAFKA_TOPIC` | No | `events.v1` | Main events topic |
| `KAFKA_DLQ_TOPIC` | No | _(auto)_ | DLQ topic — defaults to `KAFKA_TOPIC + ".dlq"` |
| `KAFKA_GROUP_ID` | No | `consumer-service` | Kafka consumer group ID |
| `KAFKA_TOPIC_PARTITIONS` | No | `6` | Partition count for newly created topics |
| `KAFKA_TOPIC_REPLICATION` | No | `1` | Replication factor (use `3` in production) |
| `KAFKA_TOPIC_ROUTES` | No | _(empty)_ | Domain routing: `order:events.order.v1,user:events.user.v1` |
| `ELASTICSEARCH_ADDRS` | No | `http://localhost:9200` | Comma-separated Elasticsearch node addresses |
| `ELASTICSEARCH_INDEX` | No | `events` | Elasticsearch index name |
| `ELASTICSEARCH_USERNAME` | No | _(empty)_ | Elasticsearch username (if security enabled) |
| `ELASTICSEARCH_PASSWORD` | No | _(empty)_ | Elasticsearch password (if security enabled) |
| `AUTH_MODE` | No | `none` | `none` \| `simple` \| `jwt` |
| `AUTH_API_KEY` | No | `dev-api-key` | API key for `simple` mode |
| `AUTH_JWT_SECRET` | **Yes (jwt mode)** | _(empty)_ | HMAC-HS256 secret — minimum 32 random bytes |
| `ADMIN_KEY` | No | `admin-secret` | Protects `POST /tenants` — change in production |

### Auth modes

```bash
# No auth (dev/demo — never use in production)
AUTH_MODE=none

# API key (single-tenant or trusted-network)
AUTH_MODE=simple
AUTH_API_KEY=your-api-key

# JWT (multi-tenant production)
AUTH_MODE=jwt
AUTH_JWT_SECRET=change-me-use-at-least-32-random-characters
```

---

## Architecture

See [docs/architecture.md](docs/architecture.md) for the full document. Key points:

**Request flow:**

1. `POST /events` hits `ingest-api`
2. Auth middleware validates credentials and places an `Identity` (with `tenant_id`) in context
3. `ingest.Service` validates fields, creates the domain `Event`, calls `store.Append()` (PostgreSQL transaction)
4. Version (`MAX(version)+1` per stream) is assigned atomically by PostgreSQL
5. Event is published to Kafka (best-effort — failure is logged, not propagated)
6. `201 Created` returned with the full persisted event including assigned version

**Fan-out flow:**

1. `consumer-service` reads from `events.v1`
2. Each message is indexed into Elasticsearch with `event.ID` as the document ID (idempotent upsert)
3. On indexing failure: message is routed to `events.v1.dlq`, offset is committed — the consumer never stalls

**Data consistency:**

| Endpoint | Read from | Consistency |
|---|---|---|
| `POST /events` response | PostgreSQL | Strong |
| `GET /events/{id}` | PostgreSQL | Strong |
| `GET /events?correlation_id=X` | PostgreSQL | Strong |
| `GET /events/timeline` | PostgreSQL | Strong |
| `GET /events/{streamID}` | Elasticsearch | Eventual |

---

## Development

```bash
# Unit tests
make test

# Unit tests with race detector
go test -race ./...

# Lint
make lint

# Build all binaries to bin/
make build

# End-to-end smoke test (requires running stack)
make test-e2e

# Regenerate Swagger docs from annotations
make swag

# Regenerate gRPC stubs from proto/events.proto
make proto

# Apply pending database migrations
make migrate
```

### Adding a database migration

```bash
# Create the next numbered file
touch db/migrations/004_my_change.sql
# Edit it, then apply:
make migrate
```

Migration files run in lexicographic order. Each migration runs in its own transaction — if it fails, nothing is committed. Already-applied migrations are skipped.

### Project layout

```
cmd/                    entry points (one directory per binary)
  ingest-api/
  consumer-service/
  replay-service/
  migrate/
db/migrations/          numbered SQL migration files (embedded at build time)
docs/                   generated Swagger docs + architecture doc
gen/proto/              generated protobuf stubs
internal/
  domain/event/         domain model + port interfaces (zero external deps)
  application/          use cases (depend only on domain interfaces)
  infrastructure/       adapters (Postgres, Kafka, Elasticsearch, HTTP, gRPC)
  config/               environment variable loading
  pkg/trace/            W3C traceparent helpers
proto/                  protobuf schema definitions
scripts/                dev helper scripts
```

---

## Roadmap

| Status | Item |
|---|---|
| Done | Event ingestion, Kafka fan-out, Elasticsearch read model |
| Done | Query API (by stream, by ID, by correlation, by causation, timeline) |
| Done | Replay engine with dry-run support |
| Done | Auth (none / API key / JWT), multi-tenant isolation |
| Done | DLQ routing for failed indexing |
| Done | gRPC replay service |
| Done | Embedded Swagger UI |
| Done | Database migrations (embedded, idempotent) |
| Planned | Prometheus metrics endpoint |
| Planned | S3/MinIO cold archival |
| Planned | Event snapshots (stream compaction) |
| Planned | Integration tests in CI (testcontainers) |
| Planned | Schema validation (JSON Schema per event type) |

---

## API Reference

### POST /events — Ingest

```bash
curl -X POST http://localhost:8080/events \
  -H "X-API-Key: dev-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "stream_id":    "order:1",
    "type":         "order.created",
    "source":       "orders-svc",
    "payload":      {"amount": 99.99},
    "correlation_id": "req-abc-123",
    "actor_id":     "user-uuid"
  }'
```

Response `201 Created`:

```json
{
  "id":          "01906c2e-4a3b-7000-8000-abc123def456",
  "tenant_id":   "default",
  "stream_id":   "order:1",
  "type":        "order.created",
  "source":      "orders-svc",
  "version":     1,
  "occurred_at": "2026-06-23T10:00:00Z",
  "payload":     {"amount": 99.99}
}
```

### GET /events/{streamID} — Query stream (Elasticsearch)

```bash
curl "http://localhost:8080/events/order:1?limit=20&offset=0" \
  -H "X-API-Key: dev-api-key"
```

### GET /events/{id} — Direct lookup (PostgreSQL)

```bash
curl "http://localhost:8080/events/01906c2e-4a3b-7000-8000-abc123def456" \
  -H "X-API-Key: dev-api-key"
```

### POST /replay — Replay events

```bash
# Dry run (preview without writing)
curl -X POST http://localhost:8080/replay \
  -H "X-API-Key: dev-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "filter":  { "stream_id": "order:1" },
    "options": { "dry_run": true, "safety_limit": 100 }
  }'

# Active replay
curl -X POST http://localhost:8080/replay \
  -H "X-API-Key: dev-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "filter":  { "stream_id": "order:1" },
    "options": { "replay_reason": "Consumer was down, re-triggering workflow runs." }
  }'
```

For the full API reference with all endpoints and request/response schemas, see the Swagger UI at `http://localhost:8080/swagger/index.html`.

---

## Operational Runbook

See [RUNBOOK.md](RUNBOOK.md) for step-by-step procedures covering:

- Incident reconstruction using correlation and causation IDs
- Replay procedures (dry run → active replay → verification)
- DLQ recovery workflows
- Cross-service log correlation using trace IDs

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup instructions, the layer boundary rules, commit style, and PR process.

## Security

See [SECURITY.md](SECURITY.md) for the security model, supported auth modes, and how to report a vulnerability.

## License

[MIT](LICENSE) — Copyright (c) 2026 Sahid Ayala
