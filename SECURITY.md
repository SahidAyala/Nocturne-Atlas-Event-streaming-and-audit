# Security Policy

## Supported Versions

This project is currently in active development (pre-1.0). Security fixes are applied to the `main` branch only.

| Version | Supported |
|---------|-----------|
| `main`  | Yes       |
| older   | No        |

---

## Reporting a Vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Report security issues by email to: **sahid.ayala@vtwo.co**

Include in your report:

- A description of the vulnerability
- Steps to reproduce (minimal proof-of-concept if possible)
- Potential impact (what an attacker could achieve)
- Any suggested mitigations you are aware of

You will receive an acknowledgment within **72 hours**. We aim to provide an initial assessment within 7 days and a fix or mitigation plan within 30 days, depending on severity and complexity.

---

## Security Model

### Authentication

The service supports three authentication modes, controlled by the `AUTH_MODE` environment variable:

| Mode | Use case | Header |
|------|----------|--------|
| `none` | Local development only — **never use in production** | — |
| `simple` | Single-tenant or trusted-network deployments | `X-API-Key: <key>` |
| `jwt` | Multi-tenant production deployments | `Authorization: Bearer <token>` |

In `jwt` mode, tokens are HMAC-HS256 signed. The secret must be at least 32 random bytes. Tokens are scoped to a `tenant_id` claim, and the application enforces tenant isolation on every database query.

### Admin endpoint

`POST /tenants` is protected by a separate `ADMIN_KEY` environment variable. This key is used only for tenant provisioning. It must be kept secret and rotated independently of `AUTH_API_KEY` and `AUTH_JWT_SECRET`.

### Network exposure

By default, all services listen on `0.0.0.0`. In production, place the API behind a reverse proxy (nginx, Caddy, or a cloud load balancer) that terminates TLS and restricts access as appropriate. Do not expose the PostgreSQL port, Kafka brokers, or Elasticsearch to the public internet.

### Known limitations

- No rate limiting is implemented. Callers can send unbounded numbers of events. Add rate limiting in your reverse proxy.
- JWT tokens do not expire by default in dev mode. Set a short `exp` claim in production issuance.
- Elasticsearch is started without TLS (`xpack.security.enabled: "false"`) in the default docker-compose. Enable security for production deployments.
- The `AUTH_MODE=none` default in `.env.example` is intentional for frictionless local development. The `.env.example` comments clearly document this. Never deploy with `AUTH_MODE=none`.

---

## Dependency Security

Dependencies are managed with Go modules. Run `go mod tidy` and review `go.sum` after any dependency update.

To audit known vulnerabilities in dependencies:

```bash
go install golang.org/x/vuln/cmd/govulncheck@latest
govulncheck ./...
```

We recommend running `govulncheck` as part of your CI pipeline.
