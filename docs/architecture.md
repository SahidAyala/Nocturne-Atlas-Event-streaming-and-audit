# Architecture — Event Streaming & Audit Platform

## 1. System Overview

The Event Streaming & Audit platform is a durable, append-only event log with a Kafka-based fan-out and an Elasticsearch read model. It is designed to be the authoritative record of every domain fact in a multi-tenant SaaS system.

**Core guarantee:** an event written to `POST /events` is durable in PostgreSQL before the HTTP response is sent. Kafka and Elasticsearch are downstream projections that may lag, fail, or be rebuilt — they never affect durability.

---

## 2. High-Level Architecture

```
                        ┌─────────────────────────────────────────┐
                        │              External Clients            │
                        └────────────────┬────────────────────────┘
                                         │ HTTP / gRPC
                        ┌────────────────▼────────────────────────┐
                        │              ingest-api (:8080)          │
                        │  chi router · auth middleware · Swagger  │
                        └──────┬──────────────────────────────────┘
                               │
              ┌────────────────▼────────────────────┐
              │          Application Layer           │
              │  ingest · query · replay · auth      │
              └──┬──────────────────┬───────────────┘
                 │                  │
   ┌─────────────▼──────┐  ┌───────▼──────────────┐
   │   PostgreSQL 16     │  │   Elasticsearch 8    │
   │   (source of truth) │  │   (read model)       │
   │   append-only log   │  │   eventually consist │
   └────────┬────────────┘  └──────────────────────┘
            │ async publish
   ┌─────────▼────────────┐
   │   Kafka 3.7 (KRaft)  │
   │   events.v1 topic    │
   └─────────┬────────────┘
             │ consume
   ┌──────────▼─────────────────────────────────────┐
   │            consumer-service                     │
   │  index → Elasticsearch · DLQ on failure        │
   └─────────────────────────────────────────────────┘

   ┌─────────────────────────────────────────────────┐
   │           replay-service (:50051 gRPC)          │
   │   streams events from PostgreSQL (strongly      │
   │   consistent, bypasses Kafka / Elasticsearch)   │
   └─────────────────────────────────────────────────┘
```

---

## 3. Layered Architecture (Hexagonal / Ports & Adapters)

The codebase enforces strict layer boundaries. Dependencies always point inward.

```
cmd/                      entry points — wire up dependencies
  ingest-api/             HTTP server + application bootstrap
  consumer-service/       Kafka consumer bootstrap
  replay-service/         gRPC server bootstrap
  migrate/                migration runner

internal/
  domain/event/           pure domain model — zero external imports
    event.go              Event entity (immutable)
    repository.go         Store, Publisher, Indexer, Searcher interfaces

  application/            use cases — depend only on domain interfaces
    ingest/               Service.Ingest (store → publish)
    query/                Service.GetByID, ListAll, ListByCorrelation…
    replay/               Service.Replay, Service.DryRun
    consume/              Service.Handle (Kafka → index)
    auth/                 Identity, IdentityFromContext

  infrastructure/         adapters — implement domain interfaces
    postgres/store.go     event.Store (PostgreSQL)
    kafka/producer.go     event.Publisher (Kafka)
    kafka/consumer.go     Kafka consumer loop
    kafka/dlq_producer.go event.Publisher (DLQ)
    kafka/topic_manager.go auto-provision topics
    kafka/topic_resolver.go domain-based routing
    elasticsearch/        event.Indexer + event.Searcher
    httpserver/           chi router, handlers, middleware
    grpcserver/           gRPC ReplayStream handler
    auth/                 simple, jwt, none authenticators

  config/                 environment variable loading
  pkg/trace/              W3C traceparent helpers
```

### Layer rules (enforced by code review)

| Layer | May import | Must NOT import |
|---|---|---|
| `domain` | stdlib only | anything in `internal/` |
| `application` | `domain`, stdlib | `infrastructure`, `cmd` |
| `infrastructure` | `domain`, `application`, `config`, stdlib, external libs | `cmd` |
| `cmd` | all of the above | nothing outside this repo |

---

## 4. Data Flow

### Write path (event ingestion)

```
1. Client sends POST /events with JSON body
2. Auth middleware validates credentials (API key or JWT)
3. HTTP handler extracts trace/correlation IDs from headers
4. ingest.Service.Ingest() is called:
   a. Validates required fields (stream_id, type, source)
   b. Ensures non-empty correlation_id (generates UUID if absent)
   c. Creates domain Event (UUID, timestamp, metadata)
   d. Scopes to tenant_id from Identity in context
   e. Calls store.Append() → PostgreSQL transaction
      - Assigns version = MAX(version)+1 per stream (atomic)
      - Writes to events table
   f. Calls publisher.Publish() → Kafka (best-effort)
      - Failure is logged at WARN level, does NOT fail the request
5. Handler returns 201 Created with the persisted event (including version)
```

### Read path (query)

```
GET /events/{streamID}          → Elasticsearch (eventually consistent)
GET /events/{id}                → PostgreSQL (strongly consistent, O(1) PK)
GET /events?correlation_id=X   → PostgreSQL (strongly consistent)
GET /events/timeline            → PostgreSQL (strongly consistent)
GET /events/{id}/causes         → PostgreSQL (strongly consistent)
```

Reads that require strong consistency (by ID, by correlation ID) always go to PostgreSQL. List-by-stream reads go to Elasticsearch for scalability.

### Fan-out path (async indexing)

```
1. consumer-service subscribes to events.v1 topic (consumer group: consumer-service)
2. For each Kafka message:
   a. Deserializes the Event from JSON
   b. Calls indexer.Index() → Elasticsearch upsert by event.ID
   c. On success: commits Kafka offset
   d. On failure: routes to events.v1.dlq topic, commits offset
      (DLQ prevents consumer from stalling indefinitely)
3. Events in events.v1.dlq can be replayed via POST /replay
```

### Replay path

```
1. Client sends POST /replay with filter + options
2. replay.Service validates filter and safety_limit
3. store.QueryForReplay() fetches matching events from PostgreSQL
4. Each event is re-published to Kafka as a new event (new UUID, is_replay=true)
   - replay_id groups all events from one replay batch
   - replay_source_event_id points to the original event
5. Re-published events flow through the normal fan-out path
```

---

## 5. Storage Model

### PostgreSQL schema (events table)

```sql
CREATE TABLE events (
  id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id        TEXT        NOT NULL,
  stream_id        TEXT        NOT NULL,
  type             TEXT        NOT NULL,
  source           TEXT        NOT NULL,
  version          BIGINT      NOT NULL,
  event_version    INT         NOT NULL DEFAULT 1,
  occurred_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  payload          JSONB       NOT NULL DEFAULT 'null',
  metadata         JSONB       NOT NULL DEFAULT '{}',
  correlation_id   TEXT        NOT NULL DEFAULT '',
  causation_id     TEXT        NOT NULL DEFAULT '',
  actor_id         TEXT        NOT NULL DEFAULT '',
  trace_id         TEXT        NOT NULL DEFAULT '',
  source_version   TEXT        NOT NULL DEFAULT '',
  is_replay        BOOLEAN     NOT NULL DEFAULT FALSE,
  replay_id        TEXT        NOT NULL DEFAULT '',
  replayed_at      TIMESTAMPTZ,
  replay_reason    TEXT        NOT NULL DEFAULT '',
  replay_source_event_id TEXT  NOT NULL DEFAULT '',

  UNIQUE (stream_id, version)
);
```

Key properties:
- `UNIQUE (stream_id, version)` is the concurrency guard — duplicate version assignments fail at the DB layer, not the application layer
- `payload` is JSONB — indexed, queryable, but treated as opaque bytes by the application
- `tenant_id` is indexed — all multi-tenant queries use it as the first predicate
- `occurred_at` is set server-side to avoid clock skew across calling services

### Kafka topics

| Topic | Purpose | Partitions |
|---|---|---|
| `events.v1` | Main event stream | 6 (configurable) |
| `events.v1.dlq` | Failed indexing operations | 6 (configurable) |

Kafka is configured in KRaft mode (no ZooKeeper). The `CLUSTER_ID` is fixed in docker-compose to make the setup reproducible.

### Elasticsearch index

Events are indexed with `event.ID` as the document ID, making all indexing operations idempotent. The index name is configurable via `ELASTICSEARCH_INDEX` (default: `events`).

Index is created automatically on first indexing operation with a default mapping. Add an explicit index template for production deployments to control shard count and replica settings.

---

## 6. Authentication Model

```
Auth mode      Header                  Scope
none           —                       All requests allowed (dev only)
simple         X-API-Key: <key>        Single tenant ("default")
jwt            Authorization: Bearer   Per-tenant, extracted from JWT claims
```

The `Identity` struct (`tenant_id`, `subject_id`, `roles`) is placed in request context by the auth middleware. Every use-case method reads Identity from context to enforce tenant isolation.

The `ADMIN_KEY` is a separate credential that protects `POST /tenants`. It is never used for event operations.

---

## 7. Trace & Correlation Model

Every inbound HTTP request gets a `correlation_id` and optionally propagates a W3C `traceparent`:

| Field | Source | Stored as |
|---|---|---|
| `correlation_id` | `X-Correlation-ID` header or generated UUID | `events.correlation_id` |
| `trace_id` | `traceparent` header (W3C format) | `events.trace_id` |
| `causation_id` | Event body field | `events.causation_id` |
| `actor_id` | Event body field | `events.actor_id` |

These fields enable three classes of post-incident queries:
- **By correlation**: `GET /events?correlation_id=X` — all events from one request
- **By causation**: `GET /events/{id}/causes` — causal chain traversal
- **By timeline**: `GET /events/timeline?from_time=...&to_time=...` — temporal reconstruction

---

## 8. Deployment Model

### Single-node (current)

All three binaries (`ingest-api`, `consumer-service`, `replay-service`) can run on a single host alongside their dependencies. This is the mode docker-compose uses.

### Production baseline

For a production deployment, the recommended topology is:

```
                   ┌──────────────────────────────┐
                   │   Load Balancer / Reverse Proxy │
                   │   (nginx, Caddy, ALB, etc.)    │
                   │   - TLS termination            │
                   │   - Rate limiting              │
                   └─────────────┬────────────────────┘
                                 │
              ┌──────────────────▼─────────────────┐
              │   N × ingest-api pods/instances    │
              │   (stateless, horizontally scalable)│
              └──────────────────┬─────────────────┘
                                 │
       ┌─────────────────────────▼──────────────────────┐
       │              PostgreSQL (primary + replica)      │
       │              - Primary: all writes              │
       │              - Replica: optional read offload   │
       └──────────────────────────────────────────────────┘
                                 │
       ┌─────────────────────────▼──────────────────────┐
       │              Kafka cluster (3 brokers)           │
       │              - 3x replication factor            │
       │              - KRaft mode                       │
       └──────────────────────────────────────────────────┘
                                 │
       ┌─────────────────────────▼──────────────────────┐
       │        M × consumer-service instances           │
       │        (scale with Kafka partition count)       │
       └──────────────────────────────────────────────────┘
```

### Scaling guidance

| Component | Scaling axis |
|---|---|
| `ingest-api` | Horizontal. Stateless. Scale with request volume. |
| `consumer-service` | Horizontal, up to partition count. Add instances = add parallelism. |
| `replay-service` | Single instance is sufficient for most workloads. Replay is rare and rate-limited by `safety_limit`. |
| PostgreSQL | Vertical (primary). Add a read replica for heavy query workloads. |
| Kafka | Increase partition count before increasing consumer count. Partitions cannot be decreased after creation. |
| Elasticsearch | Standard ES horizontal scaling. Add nodes and increase replica count as data grows. |

---

## 9. Reliability Properties

| Property | Implementation |
|---|---|
| Append-only store | No `UPDATE` or `DELETE` in the event store — enforced by the application layer |
| Per-stream ordering | `UNIQUE (stream_id, version)` + `MAX(version)+1` assignment in a transaction |
| At-least-once delivery | Kafka consumer commits offsets only after successful handling |
| Idempotent indexing | Elasticsearch upsert using event UUID as document ID |
| DLQ protection | Failed indexing routes to DLQ; consumer never stalls on a poison message |
| Replay integrity | Replay reads from PostgreSQL — always consistent, even if Kafka or Elasticsearch are stale |
| Tenant isolation | Every DB query includes `tenant_id` predicate; enforced at the application layer |

---

## 10. Known Gaps (Pre-Production)

| Gap | Impact | Mitigation |
|---|---|---|
| No Prometheus metrics | Cannot detect consumer lag, ingest rate spikes, or indexing failures via metrics | Parse structured logs for now |
| No S3 archival | PostgreSQL grows indefinitely | Implement archival job with MinIO (provisioned, logic missing) |
| No event snapshots | Full replay of large streams requires reading every event from version 1 | Acceptable for current data volume |
| No schema validation | Malformed payloads are stored and indexed as-is | Add JSON schema validation in the ingest path |
| No integration tests in CI | Infrastructure adapter bugs only caught in local e2e runs | Add testcontainers-based integration tests |
| Elasticsearch without TLS | Data is unencrypted in transit | Enable `xpack.security` for production |
