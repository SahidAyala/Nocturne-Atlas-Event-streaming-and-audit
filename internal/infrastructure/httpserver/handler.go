package httpserver

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	httpSwagger "github.com/swaggo/http-swagger"

	appauth "github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/application/auth"
	"github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/application/ingest"
	"github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/application/query"
	appreplay "github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/application/replay"
	"github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/domain/event"
	infraauth "github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/infrastructure/auth"
	authmw "github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/infrastructure/httpserver/middleware"
	"github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/pkg/trace"
)

type handler struct {
	ingestSvc     *ingest.Service
	querySvc      *query.Service
	replayEngine  *appreplay.ReplayEngine
	authenticator infraauth.Authenticator
	issuer        appauth.Issuer // nil when JWT secret is not configured
	adminKey      string
	log           *slog.Logger
}

// NewRouter wires up all routes and middleware.
// issuer may be nil — POST /auth/issue returns 501 in that case.
// adminKey protects POST /tenants via the X-Admin-Key header.
// replayEngine may be nil — POST /replay returns 501 in that case.
func NewRouter(ingestSvc *ingest.Service, querySvc *query.Service, replayEngine *appreplay.ReplayEngine, authenticator infraauth.Authenticator, issuer appauth.Issuer, adminKey string, log *slog.Logger) http.Handler {
	h := &handler{ingestSvc: ingestSvc, querySvc: querySvc, replayEngine: replayEngine, authenticator: authenticator, issuer: issuer, adminKey: adminKey, log: log}

	r := chi.NewRouter()
	r.Use(chimw.RequestID)
	r.Use(chimw.RealIP)
	r.Use(chimw.Recoverer)
	// W3C TraceContext propagation — extracts traceparent, falls back to X-Trace-ID,
	// or generates a fresh trace ID. Must run before auth so traceId is available
	// in auth failure logs (ADR-014).
	r.Use(authmw.TraceContext)

	r.Get("/health", h.health)
	r.Get("/swagger/*", httpSwagger.WrapHandler)

	// /auth/token  — public, validates credentials and returns identity.
	// /auth/issue  — requires valid credentials, upgrades to a JWT.
	// /tenants     — requires X-Admin-Key, provisions a new tenant + issues credentials.
	r.Post("/auth/token", h.authToken)
	r.With(authmw.Auth(authenticator)).Post("/auth/issue", h.authIssue)
	r.Post("/tenants", h.createTenant)
	r.Get("/streams/{streamID}/events", h.getByStream)
	r.With(authmw.Auth(authenticator)).Post("/replay", h.replayEvents)

	r.Route("/events", func(r chi.Router) {
		r.Use(authmw.Auth(authenticator))
		r.Post("/", h.ingest)
		r.Get("/", h.listEvents)
		// /events/timeline must be registered before /{id} so the static segment wins.
		r.Get("/timeline", h.getTimeline)
		r.Get("/{id}", h.getByID)
		r.Get("/{id}/causes", h.getEventCauses)
	})

	return r
}

// health godoc
// @Summary      Health check
// @Description  Returns "ok" if the service is running. No authentication required.
// @Tags         health
// @Produce      json
// @Success      200  {object}  HealthResponse
// @Router       /health [get]
func (h *handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, HealthResponse{Status: "ok"})
}

// authToken godoc
// @Summary      Validate credentials and retrieve identity
// @Description  Validates the credentials supplied in the request headers and returns the
// @Description  caller's Identity (subject, tenant_id, roles). No request body required.
// @Description
// @Description  **Simple mode** (AUTH_MODE=simple): send `X-API-Key: <key>` header.
// @Description  **JWT mode** (AUTH_MODE=jwt): send `Authorization: Bearer <token>` header.
// @Description
// @Description  Use this endpoint to verify that credentials are accepted before making
// @Description  other API calls, and to discover the tenant_id scoped to your key/token.
// @Tags         auth
// @Produce      json
// @Security     ApiKeyAuth
// @Security     BearerAuth
// @Success      200  {object}  TokenResponse
// @Failure      401  {object}  ErrorResponse  "Missing or invalid credentials"
// @Router       /auth/token [post]
func (h *handler) authToken(w http.ResponseWriter, r *http.Request) {
	identity, err := h.authenticator.Authenticate(r.Context(), r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	writeJSON(w, http.StatusOK, TokenResponse{
		SubjectID: identity.SubjectID,
		TenantID:  identity.TenantID,
		Roles:     identity.Roles,
	})
}

// authIssue godoc
// @Summary      Issue a JWT access token
// @Description  Authenticates the caller using their current credentials (API key or existing JWT)
// @Description  and mints a new signed JWT that can be used as a Bearer token.
// @Description
// @Description  The issued token inherits the caller's **subject_id**, **tenant_id**, and **roles**,
// @Description  so it carries exactly the same authorization scope as the originating credential.
// @Description
// @Description  **Requires** `AUTH_JWT_SECRET` to be configured on the server — returns 501 otherwise.
// @Description
// @Description  Typical use-cases:
// @Description  - Upgrade from static API key to short-lived JWT
// @Description  - Refresh an expiring JWT before it expires
// @Description  - Issue scoped tokens for downstream services
// @Tags         auth
// @Accept       json
// @Produce      json
// @Security     ApiKeyAuth
// @Security     BearerAuth
// @Param        body  body      IssueTokenRequest  false  "Optional — override token lifetime"
// @Success      200   {object}  IssuedTokenResponse
// @Failure      401   {object}  ErrorResponse  "Missing or invalid credentials"
// @Failure      501   {object}  ErrorResponse  "Token issuance not configured (AUTH_JWT_SECRET missing)"
// @Failure      500   {object}  ErrorResponse  "Failed to sign token"
// @Router       /auth/issue [post]
func (h *handler) authIssue(w http.ResponseWriter, r *http.Request) {
	if h.issuer == nil {
		writeError(w, http.StatusNotImplemented,
			"token issuance is not configured — set AUTH_JWT_SECRET on the server")
		return
	}

	identity, ok := appauth.IdentityFromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	var req IssueTokenRequest
	// Body is optional — ignore decode errors and use defaults.
	json.NewDecoder(r.Body).Decode(&req) //nolint:errcheck

	expiresIn := time.Duration(req.ExpiresIn) * time.Second
	if expiresIn <= 0 {
		expiresIn = time.Hour
	}
	const maxExpiry = 24 * time.Hour
	if expiresIn > maxExpiry {
		expiresIn = maxExpiry
	}

	issued, err := h.issuer.Issue(appauth.IssueCommand{
		Identity:  identity,
		ExpiresIn: expiresIn,
	})
	if err != nil {
		h.log.Error("failed to issue token",
			"correlation_id", chimw.GetReqID(r.Context()),
			"subject", identity.SubjectID,
			"error", err,
		)
		writeError(w, http.StatusInternalServerError, "failed to issue token")
		return
	}

	writeJSON(w, http.StatusOK, IssuedTokenResponse{
		Token:     issued.Token,
		ExpiresAt: issued.ExpiresAt.Format(time.RFC3339),
		SubjectID: identity.SubjectID,
		TenantID:  identity.TenantID,
	})
}

// createTenant godoc
// @Summary      Provision a new tenant and issue initial credentials
// @Description  Creates a logical tenant identity and returns the credentials needed to start
// @Description  making API calls. This is the **bootstrap endpoint** — use it once per tenant
// @Description  to obtain the first token or API key.
// @Description
// @Description  Protected by `X-Admin-Key` header (configured via `ADMIN_KEY` env var).
// @Description  Keep this key secret and separate from regular API credentials.
// @Description
// @Description  **JWT mode** (`AUTH_MODE=jwt`): issues a signed JWT embedding tenant_id, subject_id,
// @Description  and roles. Use the returned `token` as `Authorization: Bearer <token>`.
// @Description
// @Description  **Simple mode** (`AUTH_MODE=simple`): single-tenant only — returns the static
// @Description  `AUTH_API_KEY` hint since all callers share the same credential.
// @Tags         tenants
// @Accept       json
// @Produce      json
// @Param        X-Admin-Key  header    string               true  "Admin secret key (ADMIN_KEY env var)"
// @Param        body         body      CreateTenantRequest  true  "Tenant identity and credential options"
// @Success      201          {object}  TenantCredentialsResponse
// @Failure      400          {object}  ErrorResponse  "Missing required fields"
// @Failure      401          {object}  ErrorResponse  "Invalid or missing X-Admin-Key"
// @Failure      500          {object}  ErrorResponse  "Failed to issue token"
// @Router       /tenants [post]
func (h *handler) createTenant(w http.ResponseWriter, r *http.Request) {
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Admin-Key")), []byte(h.adminKey)) != 1 {
		writeError(w, http.StatusUnauthorized, "invalid or missing X-Admin-Key")
		return
	}

	var req CreateTenantRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.TenantID == "" || req.SubjectID == "" {
		writeError(w, http.StatusBadRequest, "tenant_id and subject_id are required")
		return
	}
	if len(req.Roles) == 0 {
		req.Roles = []string{"writer", "reader"}
	}

	// JWT mode: issue a signed token for the new tenant.
	if h.issuer != nil {
		expiresIn := time.Duration(req.ExpiresIn) * time.Second
		if expiresIn <= 0 {
			expiresIn = 24 * time.Hour
		}
		const maxExpiry = 30 * 24 * time.Hour
		if expiresIn > maxExpiry {
			expiresIn = maxExpiry
		}

		issued, err := h.issuer.Issue(appauth.IssueCommand{
			Identity: appauth.Identity{
				SubjectID: req.SubjectID,
				TenantID:  req.TenantID,
				Roles:     req.Roles,
			},
			ExpiresIn: expiresIn,
		})
		if err != nil {
			h.log.Error("failed to issue token for tenant",
				"correlation_id", chimw.GetReqID(r.Context()),
				"tenant_id", req.TenantID,
				"error", err,
			)
			writeError(w, http.StatusInternalServerError, "failed to issue token")
			return
		}

		writeJSON(w, http.StatusCreated, TenantCredentialsResponse{
			TenantID:  req.TenantID,
			SubjectID: req.SubjectID,
			Mode:      "jwt",
			Token:     issued.Token,
			ExpiresAt: issued.ExpiresAt.Format(time.RFC3339),
		})
		return
	}

	// Simple mode: single-tenant, no dynamic credentials.
	writeJSON(w, http.StatusCreated, TenantCredentialsResponse{
		TenantID:  "default",
		SubjectID: req.SubjectID,
		Mode:      "simple",
		Note:      "simple mode is single-tenant — use X-API-Key: <AUTH_API_KEY> for all requests",
	})
}

// listEvents godoc
// @Summary      List all events
// @Description  Returns a paginated list of events across all streams, sorted by
// @Description  occurred_at DESC (newest first). Useful as an audit feed or event log.
// @Description
// @Description  **Without `correlation_id`**: results come from the Elasticsearch read model
// @Description  (eventually consistent — recently ingested events may not appear immediately).
// @Description
// @Description  **With `correlation_id`**: results are fetched from PostgreSQL (source of truth,
// @Description  strongly consistent) using the `idx_events_correlation_id` index.
// @Description  Response headers reflect the active data source:
// @Description  - X-Read-Model: postgres | elasticsearch
// @Description  - X-Data-Consistency: strong | eventual
// @Tags         events
// @Produce      json
// @Security     ApiKeyAuth
// @Security     BearerAuth
// @Param        limit          query     int     false  "Page size — default 20, max 100"
// @Param        offset         query     int     false  "Number of events to skip — default 0"
// @Param        correlation_id query     string  false  "Filter by correlation ID (strong consistency via PostgreSQL)"
// @Success      200  {object}  ListEventsResponse
// @Failure      401  {object}  ErrorResponse  "Missing or invalid credentials"
// @Failure      500  {object}  ErrorResponse  "Read model query failed"
// @Router       /events [get]
func (h *handler) listEvents(w http.ResponseWriter, r *http.Request) {
	limit := parseIntParam(r, "limit", 20)
	offset := parseIntParam(r, "offset", 0)
	filterCorrelationID := r.URL.Query().Get("correlation_id")

	correlationID := r.Header.Get("X-Correlation-ID")
	if correlationID == "" {
		correlationID = chimw.GetReqID(r.Context())
	}
	ctx := trace.WithCorrelationID(r.Context(), correlationID)
	w.Header().Set("X-Correlation-ID", correlationID)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	result, err := h.querySvc.ListAll(ctx, query.ListQuery{
		Limit:         limit,
		Offset:        offset,
		CorrelationID: filterCorrelationID,
	})
	if err != nil {
		h.log.Error("list events failed",
			"correlation_id", correlationID,
			"filter_correlation_id", filterCorrelationID,
			"error", err,
		)
		writeError(w, http.StatusInternalServerError, "query failed")
		return
	}

	consistency := "eventual"
	if result.ReadModel == "postgres" {
		consistency = "strong"
	}

	events := make([]EventResponse, len(result.Events))
	for i, e := range result.Events {
		events[i] = domainToEventResponse(e)
	}

	w.Header().Set("X-Read-Model", result.ReadModel)
	w.Header().Set("X-Data-Consistency", consistency)
	writeJSON(w, http.StatusOK, ListEventsResponse{
		Events:    events,
		Total:     result.Total,
		Limit:     result.Limit,
		Offset:    result.Offset,
		ReadModel: result.ReadModel,
	})
}

// ingest godoc
// @Summary      Ingest an event
// @Description  Appends a new event to the specified stream. The event is persisted in PostgreSQL
// @Description  synchronously, then published to Kafka best-effort. A Kafka failure does NOT fail
// @Description  this request — the event is already durable in the store. Version is assigned by
// @Description  the store (MAX+1 per stream) and returned in the response.
// @Tags         events
// @Accept       json
// @Produce      json
// @Security     ApiKeyAuth
// @Security     BearerAuth
// @Param        body  body      IngestRequest  true  "Event payload"
// @Success      201   {object}  EventResponse
// @Failure      400   {object}  ErrorResponse  "Invalid JSON body"
// @Failure      401   {object}  ErrorResponse  "Missing or invalid credentials"
// @Failure      422   {object}  ErrorResponse  "Validation error (missing required fields)"
// @Router       /events [post]
func (h *handler) ingest(w http.ResponseWriter, r *http.Request) {
	// Resolve correlation ID with explicit priority:
	//   1. X-Correlation-ID request header (caller-supplied, preserves upstream trace)
	//   2. chi request ID from context  (generated by chimw.RequestID middleware)
	//   3. freshly generated UUID       (last resort — guarantees non-empty)
	correlationID := r.Header.Get("X-Correlation-ID")
	if correlationID == "" {
		correlationID = chimw.GetReqID(r.Context())
	}
	if correlationID == "" {
		correlationID = uuid.NewString()
	}
	// Inject into context so all downstream logs can read it via trace.FromContext.
	ctx := trace.WithCorrelationID(r.Context(), correlationID)
	// Echo the resolved ID back so callers can correlate this request in their logs.
	w.Header().Set("X-Correlation-ID", correlationID)

	// Buffer the body so we can include a preview in decode-failure logs.
	// 4 KiB covers any realistic ingest payload; larger bodies are truncated.
	const maxBodyRead = 4096
	body, _ := io.ReadAll(io.LimitReader(r.Body, maxBodyRead))

	var req IngestRequest
	if err := json.NewDecoder(bytes.NewReader(body)).Decode(&req); err != nil {
		h.log.Warn("invalid request body",
			"correlation_id", correlationID,
			"body_truncated", len(body) == maxBodyRead,
			"payload_preview", bodyPreview(body, 512),
			"error", err,
		)
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	// Resolve causation ID from header or request body (header takes precedence to
	// allow proxies to set it without modifying the body).
	causationID := r.Header.Get("X-Causation-ID")
	if causationID == "" {
		causationID = req.CausationID
	}
	// TraceID: prefer the W3C traceId extracted from traceparent by the trace middleware,
	// then fall back to X-Trace-ID header, then to the request body field.
	traceID := trace.TraceIDFromContext(ctx)
	if traceID == "" {
		traceID = r.Header.Get("X-Trace-ID")
	}
	if traceID == "" {
		traceID = req.TraceID
	}

	e, err := h.ingestSvc.Ingest(ctx, ingest.Command{
		StreamID:      req.StreamID,
		Type:          req.Type,
		Source:        req.Source,
		Payload:       req.Payload, // already json.RawMessage — no re-encoding
		Metadata:      req.Metadata,
		CorrelationID: correlationID,
		EventVersion:  req.EventVersion,
		CausationID:   causationID,
		ActorID:       req.ActorID,
		TraceID:       traceID,
		SourceVersion: req.SourceVersion,
	})
	if err != nil {
		h.log.Error("ingest failed",
			"correlation_id", correlationID,
			"stream_id", req.StreamID,
			"type", req.Type,
			"error", err,
		)
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, e)
}

// getByStream godoc
// @Summary      Query events by stream
// @Description  Returns a paginated, version-ordered list of events for a stream from the
// @Description  Elasticsearch read model. Results are **eventually consistent** with the
// @Description  PostgreSQL event store — recently ingested events may not be visible immediately.
// @Description  Response headers communicate the data source and consistency level:
// @Description  - X-Read-Model: elasticsearch
// @Description  - X-Data-Consistency: eventual
// @Tags         events
// @Produce      json
// @Security     ApiKeyAuth
// @Security     BearerAuth
// @Param        streamID  path      string  true   "Stream identifier (e.g. order:1, user:42)"
// @Param        limit     query     int     false  "Page size — default 20, max 100"
// @Param        offset    query     int     false  "Number of events to skip — default 0"
// @Success      200       {object}  QueryResultResponse
// @Failure      400       {object}  ErrorResponse  "stream_id missing"
// @Failure      401       {object}  ErrorResponse  "Missing or invalid credentials"
// @Failure      500       {object}  ErrorResponse  "Read model query failed"
// @Router       /events/{streamID} [get]
func (h *handler) getByStream(w http.ResponseWriter, r *http.Request) {
	streamID := chi.URLParam(r, "streamID")
	if streamID == "" {
		writeError(w, http.StatusBadRequest, "stream_id is required")
		return
	}

	limit := parseIntParam(r, "limit", 20)
	offset := parseIntParam(r, "offset", 0)

	correlationID := r.Header.Get("X-Correlation-ID")
	if correlationID == "" {
		correlationID = chimw.GetReqID(r.Context())
	}
	ctx := trace.WithCorrelationID(r.Context(), correlationID)
	w.Header().Set("X-Correlation-ID", correlationID)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	result, err := h.querySvc.QueryByStream(ctx, query.StreamQuery{
		StreamID: streamID,
		Limit:    limit,
		Offset:   offset,
	})
	if err != nil {
		h.log.Error("query by stream failed",
			"correlation_id", correlationID,
			"stream_id", streamID,
			"error", err,
		)
		writeError(w, http.StatusInternalServerError, "query failed")
		return
	}

	events := make([]EventResponse, len(result.Events))
	for i, e := range result.Events {
		events[i] = domainToEventResponse(e)
	}

	w.Header().Set("X-Read-Model", "elasticsearch")
	w.Header().Set("X-Data-Consistency", "eventual")
	writeJSON(w, http.StatusOK, QueryResultResponse{
		StreamID:  result.StreamID,
		Events:    events,
		Total:     result.Total,
		Limit:     result.Limit,
		Offset:    result.Offset,
		ReadModel: result.ReadModel,
	})
}

// getByID godoc
// @Summary      Get event by ID
// @Description  Returns a single event by its UUID. This is a direct O(1) lookup against
// @Description  PostgreSQL (source of truth) — always consistent, no replication lag.
// @Description  Use this when you know the event ID and need the authoritative record.
// @Description  For stream history or aggregate reconstruction, use GET /streams/{streamID}/events.
// @Tags         events
// @Produce      json
// @Security     ApiKeyAuth
// @Security     BearerAuth
// @Param        id  path      string  true  "Event UUID (e.g. 01906c2e-4a3b-7000-8000-abc123def456)"
// @Success      200  {object}  EventResponse
// @Failure      400  {object}  ErrorResponse  "id is not a valid UUID"
// @Failure      401  {object}  ErrorResponse  "Missing or invalid credentials"
// @Failure      404  {object}  ErrorResponse  "Event not found"
// @Failure      500  {object}  ErrorResponse  "Store lookup failed"
// @Router       /events/by-id/{id} [get]
func (h *handler) getByID(w http.ResponseWriter, r *http.Request) {
	rawID := chi.URLParam(r, "id")

	id, err := uuid.Parse(rawID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "id must be a valid UUID")
		return
	}

	correlationID := r.Header.Get("X-Correlation-ID")
	if correlationID == "" {
		correlationID = chimw.GetReqID(r.Context())
	}
	ctx := trace.WithCorrelationID(r.Context(), correlationID)
	w.Header().Set("X-Correlation-ID", correlationID)

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	e, err := h.querySvc.GetByID(ctx, id)
	if err != nil {
		if errors.Is(err, event.ErrNotFound) {
			writeError(w, http.StatusNotFound, "event not found")
			return
		}
		h.log.Error("get by id failed",
			"correlation_id", correlationID,
			"event_id", rawID,
			"error", err,
		)
		writeError(w, http.StatusInternalServerError, "lookup failed")
		return
	}

	w.Header().Set("X-Read-Model", "postgres")
	w.Header().Set("X-Data-Consistency", "strong")
	writeJSON(w, http.StatusOK, domainToEventResponse(e))
}

// replayEvents godoc
// @Summary      Replay events
// @Description  Replays events that match the given filter. In dry-run mode the matched
// @Description  original events are returned without creating any new events. In active mode
// @Description  each matched event is re-ingested as a new event with replay metadata attached
// @Description  (is_replay=true, replay_id, replay_source_event_id). Original events are never
// @Description  modified. See ADR-015 for full design rationale.
// @Description
// @Description  **Safety constraints**:
// @Description  - `replay_reason` is required for active replays.
// @Description  - Default safety limit is 1000 events per call; set `safety_limit` to override.
// @Description  - Returns 422 if the filter matches more events than the safety limit.
// @Tags         replay
// @Accept       json
// @Produce      json
// @Security     ApiKeyAuth
// @Security     BearerAuth
// @Param        body  body      ReplayRequest  true  "Replay filter and options"
// @Success      200   {object}  ReplayResponse
// @Failure      400   {object}  ErrorResponse  "Invalid request body or timestamp format"
// @Failure      401   {object}  ErrorResponse  "Missing or invalid credentials"
// @Failure      422   {object}  ErrorResponse  "Validation error or safety limit exceeded"
// @Failure      500   {object}  ErrorResponse  "Replay execution failed"
// @Router       /replay [post]
func (h *handler) replayEvents(w http.ResponseWriter, r *http.Request) {
	if h.replayEngine == nil {
		writeError(w, http.StatusNotImplemented, "replay is not configured")
		return
	}

	correlationID := r.Header.Get("X-Correlation-ID")
	if correlationID == "" {
		correlationID = chimw.GetReqID(r.Context())
	}
	ctx := trace.WithCorrelationID(r.Context(), correlationID)
	w.Header().Set("X-Correlation-ID", correlationID)

	var req ReplayRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	var fromTime, toTime time.Time
	if req.Filter.FromTime != "" {
		t, err := time.Parse(time.RFC3339, req.Filter.FromTime)
		if err != nil {
			writeError(w, http.StatusBadRequest, "from_time must be an ISO-8601 timestamp (e.g. 2026-04-21T10:00:00Z)")
			return
		}
		fromTime = t
	}
	if req.Filter.ToTime != "" {
		t, err := time.Parse(time.RFC3339, req.Filter.ToTime)
		if err != nil {
			writeError(w, http.StatusBadRequest, "to_time must be an ISO-8601 timestamp (e.g. 2026-04-21T10:00:00Z)")
			return
		}
		toTime = t
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	result, err := h.replayEngine.Execute(ctx, appreplay.ReplayRequest{
		Filter: event.ReplayFilter{
			TenantID:      req.Filter.TenantID,
			StreamID:      req.Filter.StreamID,
			CorrelationID: req.Filter.CorrelationID,
			EventType:     req.Filter.EventType,
			ActorID:       req.Filter.ActorID,
			FromTime:      fromTime,
			ToTime:        toTime,
			EventIDs:      req.Filter.EventIDs,
		},
		Options: appreplay.ReplayOptions{
			DryRun:       req.Options.DryRun,
			ReplayReason: req.Options.ReplayReason,
			SafetyLimit:  req.Options.SafetyLimit,
		},
	})
	if err != nil {
		if errors.Is(err, event.ErrReplayLimitExceeded) {
			writeError(w, http.StatusUnprocessableEntity, err.Error())
			return
		}
		h.log.Error("replay failed",
			"correlation_id", correlationID,
			"dry_run", req.Options.DryRun,
			"error", err,
		)
		if errors.Is(err, context.DeadlineExceeded) {
			writeError(w, http.StatusGatewayTimeout, "replay timed out")
			return
		}
		writeError(w, http.StatusInternalServerError, "replay failed")
		return
	}

	events := make([]EventResponse, len(result.Events))
	for i, e := range result.Events {
		events[i] = domainToEventResponse(e)
	}

	writeJSON(w, http.StatusOK, ReplayResponse{
		ReplayID:      result.ReplayID,
		DryRun:        result.DryRun,
		MatchedCount:  result.MatchedCount,
		ReplayedCount: result.ReplayedCount,
		Events:        events,
	})
}

// getEventCauses godoc
// @Summary      Get events caused by a given event
// @Description  Returns all events whose causation_id equals the given event ID. Allows
// @Description  traversal of the causation tree — one hop at a time. Results are scoped to
// @Description  the caller's tenant and ordered by occurred_at ASC. Served from PostgreSQL
// @Description  (source of truth, strongly consistent). See ADR-017.
// @Tags         events
// @Produce      json
// @Security     ApiKeyAuth
// @Security     BearerAuth
// @Param        id      path      string  true   "Source event UUID"
// @Param        limit   query     int     false  "Page size — default 20, max 100"
// @Param        offset  query     int     false  "Number of events to skip — default 0"
// @Success      200     {object}  CausationResponse
// @Failure      401     {object}  ErrorResponse  "Missing or invalid credentials"
// @Failure      500     {object}  ErrorResponse  "Causation query failed"
// @Router       /events/{id}/causes [get]
func (h *handler) getEventCauses(w http.ResponseWriter, r *http.Request) {
	rawID := chi.URLParam(r, "id")

	limit := parseIntParam(r, "limit", 20)
	offset := parseIntParam(r, "offset", 0)

	correlationID := r.Header.Get("X-Correlation-ID")
	if correlationID == "" {
		correlationID = chimw.GetReqID(r.Context())
	}
	ctx := trace.WithCorrelationID(r.Context(), correlationID)
	w.Header().Set("X-Correlation-ID", correlationID)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	result, err := h.querySvc.ListByCausationID(ctx, query.CausationQuery{
		EventID: rawID,
		Limit:   limit,
		Offset:  offset,
	})
	if err != nil {
		h.log.Error("causation query failed",
			"correlation_id", correlationID,
			"event_id", rawID,
			"error", err,
		)
		writeError(w, http.StatusInternalServerError, "causation query failed")
		return
	}

	events := make([]EventResponse, len(result.Events))
	for i, e := range result.Events {
		events[i] = domainToEventResponse(e)
	}

	w.Header().Set("X-Read-Model", "postgres")
	w.Header().Set("X-Data-Consistency", "strong")
	writeJSON(w, http.StatusOK, CausationResponse{
		SourceEventID: result.SourceEventID,
		Events:        events,
		Total:         result.Total,
		Limit:         result.Limit,
		Offset:        result.Offset,
	})
}

// getTimeline godoc
// @Summary      Tenant event timeline
// @Description  Returns a paginated, occurred_at DESC list of all events for the caller's
// @Description  tenant within an optional time window. Use this as the entry point for
// @Description  incident reconstruction when a correlationId is not yet known (ADR-017).
// @Description  Served from PostgreSQL (source of truth, strongly consistent).
// @Tags         events
// @Produce      json
// @Security     ApiKeyAuth
// @Security     BearerAuth
// @Param        from_time  query     string  false  "Lower bound (inclusive), ISO-8601, e.g. 2026-04-21T10:00:00Z"
// @Param        to_time    query     string  false  "Upper bound (inclusive), ISO-8601, e.g. 2026-04-21T11:00:00Z"
// @Param        limit      query     int     false  "Page size — default 20, max 100"
// @Param        offset     query     int     false  "Number of events to skip — default 0"
// @Success      200        {object}  TimelineResponse
// @Failure      400        {object}  ErrorResponse  "Invalid timestamp format"
// @Failure      401        {object}  ErrorResponse  "Missing or invalid credentials"
// @Failure      500        {object}  ErrorResponse  "Timeline query failed"
// @Router       /events/timeline [get]
func (h *handler) getTimeline(w http.ResponseWriter, r *http.Request) {
	limit := parseIntParam(r, "limit", 20)
	offset := parseIntParam(r, "offset", 0)

	var fromTime, toTime time.Time
	if s := r.URL.Query().Get("from_time"); s != "" {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			writeError(w, http.StatusBadRequest, "from_time must be an ISO-8601 timestamp (e.g. 2026-04-21T10:00:00Z)")
			return
		}
		fromTime = t
	}
	if s := r.URL.Query().Get("to_time"); s != "" {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			writeError(w, http.StatusBadRequest, "to_time must be an ISO-8601 timestamp (e.g. 2026-04-21T10:00:00Z)")
			return
		}
		toTime = t
	}

	correlationID := r.Header.Get("X-Correlation-ID")
	if correlationID == "" {
		correlationID = chimw.GetReqID(r.Context())
	}
	ctx := trace.WithCorrelationID(r.Context(), correlationID)
	w.Header().Set("X-Correlation-ID", correlationID)
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	result, err := h.querySvc.GetTimeline(ctx, query.TimelineQuery{
		FromTime: fromTime,
		ToTime:   toTime,
		Limit:    limit,
		Offset:   offset,
	})
	if err != nil {
		h.log.Error("timeline query failed",
			"correlation_id", correlationID,
			"from_time", fromTime,
			"to_time", toTime,
			"error", err,
		)
		writeError(w, http.StatusInternalServerError, "timeline query failed")
		return
	}

	events := make([]EventResponse, len(result.Events))
	for i, e := range result.Events {
		events[i] = domainToEventResponse(e)
	}

	fromStr := ""
	if !fromTime.IsZero() {
		fromStr = fromTime.Format(time.RFC3339)
	}
	toStr := ""
	if !toTime.IsZero() {
		toStr = toTime.Format(time.RFC3339)
	}

	w.Header().Set("X-Read-Model", "postgres")
	w.Header().Set("X-Data-Consistency", "strong")
	writeJSON(w, http.StatusOK, TimelineResponse{
		TenantID: result.TenantID,
		Events:   events,
		Total:    result.Total,
		Limit:    result.Limit,
		Offset:   result.Offset,
		FromTime: fromStr,
		ToTime:   toStr,
	})
}

// parseIntParam reads an integer query parameter with a fallback default.
// Returns the default if the param is absent, non-numeric, or negative.
func parseIntParam(r *http.Request, key string, fallback int) int {
	v := r.URL.Query().Get(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return fallback
	}
	return n
}

// domainToEventResponse converts a domain Event to its HTTP response representation.
// Keeping this mapping in the HTTP layer ensures the domain never leaks into the wire format.
func domainToEventResponse(e *event.Event) EventResponse {
	return EventResponse{
		ID:                  e.ID.String(),
		TenantID:            e.TenantID,
		StreamID:            e.StreamID,
		Type:                e.Type,
		Source:              e.Source,
		Version:             e.Version,
		EventVersion:        e.EventVersion,
		OccurredAt:          e.OccurredAt,
		CorrelationID:       e.CorrelationID,
		CausationID:         e.CausationID,
		ActorID:             e.ActorID,
		TraceID:             e.TraceID,
		SourceVersion:       e.SourceVersion,
		Payload:             e.Payload,
		Metadata:            e.Metadata,
		IsReplay:            e.IsReplay,
		ReplayID:            e.ReplayID,
		ReplayedAt:          e.ReplayedAt,
		ReplayReason:        e.ReplayReason,
		ReplaySourceEventID: e.ReplaySourceEventID,
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, ErrorResponse{Error: msg})
}

// bodyPreview returns a safe, truncated string of raw bytes for log diagnostics.
func bodyPreview(b []byte, max int) string {
	if len(b) <= max {
		return string(b)
	}
	return string(b[:max]) + "…"
}
