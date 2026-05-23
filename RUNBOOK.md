# Event Streaming & Audit — Operational Runbook

This document covers four operational scenarios:

1. **Incident reconstruction** — rebuilding what happened from logs and API calls
2. **Correlation-based debugging** — following a single request through all systems
3. **Replay runbook** — safely re-ingesting lost or corrupted events
4. **Dead-letter queue recovery** — recovering stalled Kafka consumers

---

## 1. Distributed Tracing Model (ADR-014)

Every HTTP request entering the Event Streaming API gets a **trace ID** (32 lowercase hex chars, W3C-compatible):

- Carried in the `traceparent` header: `00-{traceId}-{spanId}-{flags}`
- Stored on each event as `trace_id` (first-class column, not inside metadata)
- Echoed back to callers in the `traceparent` and `X-Trace-ID` response headers
- Forwarded to Kafka as the `traceparent` message header
- Extracted by the Workflow Engine Kafka consumer and logged as `trace_id`

A **correlation ID** (`X-Correlation-ID` header) is a request-scoped identifier that groups all events from a single operation. Unlike `trace_id`, correlation IDs are caller-assigned and survive across service boundaries.

**Minimum log fields** every service must emit per event:

| Field | Source |
|---|---|
| `trace_id` | W3C traceparent → `trace.TraceIDFromContext(ctx)` |
| `correlation_id` | `X-Correlation-ID` header → `trace.FromContext(ctx)` |
| `event_id` | `event.ID.String()` |
| `tenant_id` | JWT claim / identity context |
| `stream_id` | `event.StreamID` |
| `event_type` | `event.Type` |

---

## 2. Incident Reconstruction Workflow (ADR-017)

### Step 1 — Find a starting point

You need either a `correlation_id` (best case) or a tenant ID + rough time window.

**If you have a correlation_id:**
```bash
# Returns all events sharing this correlation ID (PostgreSQL, strongly consistent)
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8080/events?correlation_id=<corr-id>&limit=100" | jq .
```

**If you only know the tenant and approximate time:**
```bash
# Returns all tenant events in the window (newest first)
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8080/events/timeline?from_time=2026-04-21T10:00:00Z&to_time=2026-04-21T11:00:00Z&limit=50" | jq .
```

Pick the event that looks like the root cause from the timeline. Note its `id`, `correlation_id`, `trace_id`.

### Step 2 — Reconstruct the causation chain

For any event of interest, find all events it caused:

```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8080/events/<event-id>/causes?limit=50" | jq .
```

Recurse: for each caused event, call `/events/{id}/causes` again to traverse the full causation tree.

### Step 3 — Cross-service log correlation

With `trace_id` in hand, search across all three systems:

```bash
# Event Streaming structured logs
grep '"trace_id":"<trace-id>"' /var/log/event-streaming.jsonl

# Workflow Engine structured logs
grep '"trace_id":"<trace-id>"' /var/log/workflow-engine.jsonl

# NestJS platform logs (if using pino/winston with trace_id field)
grep '"traceId":"<trace-id>"' /var/log/saas-platform.jsonl
```

### Step 4 — Get event details

```bash
# Direct lookup by UUID (O(1) PK lookup, always consistent)
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8080/events/<event-id>" | jq .
```

### Step 5 — Check for replays

Look at the `is_replay`, `replay_id`, `replay_source_event_id` fields on any event. If `is_replay=true`, the event was re-ingested from a previous replay batch. The `replay_source_event_id` points to the original.

---

## 3. Replay Runbook (ADR-015)

### When to replay

- Events were lost due to a Kafka consumer crash
- Downstream system (Workflow Engine) was unavailable during event ingestion
- A bug caused events to be processed incorrectly; the consumer has been fixed
- You need to re-index events into Elasticsearch after a reindex failure

### Pre-flight checklist

1. Verify the downstream consumer is **healthy** and **idempotent** before replaying — replayed events flow through the same Kafka pipeline
2. Confirm the affected `correlation_id`, `stream_id`, or `event_type`
3. Start with a **dry run** to verify filter accuracy

### Step 1 — Dry run (preview without writing)

```bash
curl -s -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  http://localhost:8080/replay \
  -d '{
    "filter": {
      "stream_id": "order:1234",
      "from_time": "2026-04-21T10:00:00Z",
      "to_time": "2026-04-21T11:00:00Z"
    },
    "options": {
      "dry_run": true,
      "safety_limit": 100
    }
  }' | jq '{matched: .matched_count, events: [.events[] | {id, type, occurred_at}]}'
```

Verify `matched_count` is what you expect. Review the events list.

### Step 2 — Active replay

```bash
curl -s -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  http://localhost:8080/replay \
  -d '{
    "filter": {
      "stream_id": "order:1234",
      "from_time": "2026-04-21T10:00:00Z",
      "to_time": "2026-04-21T11:00:00Z"
    },
    "options": {
      "replay_reason": "Consumer was down 2026-04-21T10:00-11:00 due to DB connection failure. Replaying to re-trigger workflow runs.",
      "safety_limit": 100
    }
  }' | jq '{replay_id: .replay_id, replayed: .replayed_count}'
```

Note the `replay_id` — use it to find all replayed events later:

```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8080/events?correlation_id=<replay_id>" | jq .
```

### Filter options

| Field | Example | Notes |
|---|---|---|
| `stream_id` | `"order:1234"` | Narrow to one stream |
| `correlation_id` | `"corr-abc-123"` | All events from one request |
| `event_type` | `"order.created"` | One event type across all streams |
| `actor_id` | `"user-uuid"` | All events by one actor |
| `from_time` / `to_time` | `"2026-04-21T10:00:00Z"` | ISO-8601 time window |
| `event_ids` | `["uuid1", "uuid2"]` | Exact event list |

### Safety limits

- Default: 1000 events per call. Returns HTTP 422 if exceeded.
- Narrow the filter or increase `safety_limit` to override.
- Large replays should be batched: replay by 1-hour windows.

### What replay does NOT do

- Does not modify original events (append-only store)
- Does not deduplicate: replayed events are new UUIDs — consumers must be idempotent
- Does not guarantee ordering relative to live events
- Does not replay to a point in time — all replayed events appear with `occurred_at = now()`

---

## 4. Dead-Letter Queue Recovery (ADR-016)

### Identifying DLQ messages

The Event Streaming Kafka consumer logs at `ERROR` level when it drops a message:

```
"kafka consumer: handler rejected event — consumer stopping"
```

The Workflow Engine consumer logs at `ERROR` level when a message is dead-lettered:

```
"kafka consumer: message dead-lettered after max retries — offset committed"
```

Search these patterns in your log aggregator. Every dead-lettered message includes `event_id`, `tenant_id`, `trace_id`, and `correlation_id`.

### Recovery: Event Streaming consumer crash

The Event Streaming consumer commits offsets only after successful handling. If it crashes mid-processing, it restarts from the last committed offset. No manual intervention needed unless the consumer is stuck in a crash loop.

**If the index is corrupt or missing:**

```sql
-- Find events that should have been indexed but weren't
SELECT id, tenant_id, stream_id, type, occurred_at
FROM events
WHERE occurred_at > NOW() - INTERVAL '2 hours'
  AND tenant_id = 'affected-tenant'
ORDER BY occurred_at DESC
LIMIT 100;
```

Then replay those events via the API (see §3) to re-publish them to Kafka and re-trigger the indexer.

### Recovery: Workflow Engine dead-lettered event

The Workflow Engine commits the offset after `maxHandlerRetries` (3) failures, logging:
```
"kafka consumer: message dead-lettered after max retries — offset committed"
```

The event is durable in Event Streaming's PostgreSQL. Recovery options:

**Option A — Replay the original event (recommended):**
```bash
# Get the event_id from the dead-letter log, then replay it
curl -s -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  http://localhost:8080/replay \
  -d '{
    "filter": {
      "event_ids": ["<event-id-from-dlq-log>"]
    },
    "options": {
      "replay_reason": "Dead-lettered by Workflow Engine consumer on 2026-04-21 — handler was crashing due to DB connection exhaustion."
    }
  }'
```

**Option B — Manual re-trigger via Workflow Engine API:**
```bash
# If you know the workflow_id and tenant project_id
curl -s -X POST \
  -H "Authorization: Bearer $WF_TOKEN" \
  http://localhost:8090/workflows/<workflow-id>/runs
```

### Recovery: NestJS outbox stuck

If NestJS events are not flowing to Event Streaming:

```sql
-- Check outbox table for stuck records
SELECT id, event_type, aggregate_id, created_at, retry_count
FROM outbox_events
WHERE processed_at IS NULL
ORDER BY created_at ASC
LIMIT 20;

-- Reset retry count to allow reprocessing
UPDATE outbox_events
SET retry_count = 0, last_error = NULL
WHERE processed_at IS NULL
  AND created_at > NOW() - INTERVAL '24 hours';
```

---

## 5. Debugging Checklist

For any incident, work through this list in order:

- [ ] 1. Get `correlation_id` from the user report or HTTP logs
- [ ] 2. `GET /events?correlation_id=<id>` — find all events for this request
- [ ] 3. Check `is_replay` on each event — distinguish replays from originals
- [ ] 4. For any suspicious event, `GET /events/{id}/causes` — find downstream effects
- [ ] 5. Get `trace_id` from any event — cross-reference with Workflow Engine logs
- [ ] 6. If events are missing, `GET /events/timeline?from_time=...&to_time=...` — find what did happen
- [ ] 7. If the root cause is a consumer crash, replay the affected time window (§3)
- [ ] 8. After replay, verify `replayed_count` matches `matched_count` and check Workflow Engine logs

**Key invariants to verify:**

| Invariant | Check |
|---|---|
| All events for a correlationId have the same `correlation_id` | `GET /events?correlation_id=X` returns complete causal set |
| Causation chain is intact | `GET /events/{id}/causes` returns expected children |
| No version gaps in a stream | `GET /events?stream_id=X` has contiguous versions |
| Replay events reference original | `replay_source_event_id` matches the original event's `id` |
