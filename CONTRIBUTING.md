# Contributing to Nocturne Atlas — Event Streaming & Audit

Thank you for taking the time to contribute. This document covers everything you need to go from zero to a merged pull request.

---

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Before You Start](#before-you-start)
- [Development Setup](#development-setup)
- [Making Changes](#making-changes)
- [Commit Style](#commit-style)
- [Pull Request Process](#pull-request-process)
- [Architecture Principles](#architecture-principles)
- [Testing Requirements](#testing-requirements)

---

## Code of Conduct

Be respectful and constructive. Harassment of any kind is not tolerated. Violations may result in being blocked from the project.

---

## Before You Start

For anything beyond a typo fix, open an issue first. Describe:

- **What** you want to change or add
- **Why** it belongs in this project
- **How** you plan to implement it (rough outline)

This avoids duplicate work and makes sure the design fits the existing architecture before any code is written.

---

## Development Setup

### Prerequisites

- Go 1.24+
- Docker & Docker Compose
- `make`, `curl`, `nc`, `pg_isready` (standard on macOS/Linux)

### First-time setup

```bash
git clone https://github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit.git
cd Nocturne-Atlas-Event-streaming-and-audit

cp .env.example .env   # defaults work out of the box

make dev               # starts infra in Docker, runs ingest-api locally
make migrate           # apply database migrations
```

To also run the Kafka consumer:

```bash
# in a second terminal
make dev-consumer
```

Verify everything works:

```bash
make test-e2e
```

---

## Making Changes

### Hexagonal architecture — the non-negotiable rule

The codebase follows a strict Ports & Adapters (hexagonal) architecture:

```
domain/         <- pure business logic, zero dependencies
application/    <- use cases, depends only on domain interfaces
infrastructure/ <- adapters (Postgres, Kafka, Elasticsearch, HTTP, gRPC)
```

**Never** import infrastructure packages from the domain or application layer. **Never** query the database directly from HTTP handlers. If your change violates a layer boundary, it will not be merged.

### Workflow

1. Fork the repository and create a branch:
   ```bash
   git checkout -b feat/my-feature
   ```
2. Write your code. Follow the patterns already in the repo — do not introduce new abstractions or patterns without discussing them first.
3. Write or update tests. See [Testing Requirements](#testing-requirements).
4. Run the full test suite:
   ```bash
   make test
   make lint
   ```
5. Run the end-to-end smoke test against a live stack:
   ```bash
   make dev-full   # in one terminal
   make test-e2e   # in another
   ```

---

## Commit Style

Use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/):

```
<type>(<scope>): <short description>

[optional body]
```

Types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`, `perf`

Scopes: `ingest`, `query`, `replay`, `consume`, `kafka`, `postgres`, `elasticsearch`, `auth`, `grpc`, `http`, `config`, `migrations`, `ci`, `docker`

Examples:

```
feat(replay): add dry_run option to preview matched events
fix(kafka): handle nil message key on DLQ routing
test(ingest): cover store failure preventing kafka publish
docs(runbook): add step-by-step DLQ recovery procedure
```

Keep the subject line under 72 characters. Use the body for the **why**, not the what.

---

## Pull Request Process

1. Target the `main` branch.
2. Fill in the PR template. Include:
   - What changed and why
   - How to test it manually
   - Any known limitations
3. All CI checks must pass (build, tests, lint).
4. At least one maintainer approval is required before merging.
5. Squash merge is preferred for feature branches; merge commits are used for releases.

---

## Architecture Principles

These are standing constraints. Do not work around them:

| Principle | What it means |
|---|---|
| PostgreSQL is the source of truth | Events are durable the moment `POST /events` returns `201`. Kafka and Elasticsearch are downstream projections. |
| Append-only | Events are never updated or deleted. No `UPDATE events`, no `DELETE FROM events`. |
| Best-effort Kafka | A Kafka publish failure **must not** fail an ingest request. Log it, continue. |
| Idempotent indexing | Elasticsearch indexing uses `event.ID` as the document ID. Re-indexing is always safe. |
| Tenant isolation | Every query must be scoped to `tenant_id`. Never return events across tenant boundaries. |
| No shared state outside DB | Do not introduce in-process caches or global state that is not the PostgreSQL store. |

---

## Testing Requirements

### What must be tested

- Every new use-case method in `internal/application/` needs unit tests.
- Use mock implementations of the domain ports (`event.Store`, `event.Publisher`, `event.Indexer`, `event.Searcher`) — follow the pattern in `internal/application/ingest/service_test.go`.
- Middleware and HTTP handlers warrant tests against the real `http.Handler` using `httptest`.
- Infrastructure adapters (Postgres, Kafka, Elasticsearch) do **not** require unit tests — integration tests are preferred for those, but they are outside the current scope.

### What the tests must prove

For the ingest service specifically, these are the invariants that must be covered:

- Event is persisted before Kafka is called
- Kafka failure does not roll back persistence
- Store failure prevents Kafka publish
- Version is monotonically increasing per stream
- Cross-stream versions are independent
- Missing identity returns an error
- Missing required fields (`stream_id`, `type`, `source`) return validation errors

Replicate this level of rigor for any new service.

### Running tests

```bash
make test        # all unit tests
make lint        # golangci-lint
make test-e2e    # end-to-end (requires running stack)
```
