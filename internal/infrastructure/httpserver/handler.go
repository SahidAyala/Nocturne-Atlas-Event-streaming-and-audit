package httpserver

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	httpSwagger "github.com/swaggo/http-swagger"

	appauth "github.com/SheykoWk/event-streaming-and-audit/internal/application/auth"
	"github.com/SheykoWk/event-streaming-and-audit/internal/application/ingest"
	"github.com/SheykoWk/event-streaming-and-audit/internal/application/query"
	"github.com/SheykoWk/event-streaming-and-audit/internal/domain/event"
	infraauth "github.com/SheykoWk/event-streaming-and-audit/internal/infrastructure/auth"
	authmw "github.com/SheykoWk/event-streaming-and-audit/internal/infrastructure/httpserver/middleware"
)

type handler struct {
	ingestSvc     *ingest.Service
	querySvc      *query.Service
	authenticator infraauth.Authenticator
	issuer        appauth.Issuer // nil when JWT secret is not configured
	adminKey      string
	log           *slog.Logger
}

// NewRouter wires up all routes and middleware.
// issuer may be nil — POST /auth/issue returns 501 in that case.
// adminKey protects POST /tenants via the X-Admin-Key header.
func NewRouter(ingestSvc *ingest.Service, querySvc *query.Service, authenticator infraauth.Authenticator, issuer appauth.Issuer, adminKey string, log *slog.Logger) http.Handler {
	h := &handler{ingestSvc: ingestSvc, querySvc: querySvc, authenticator: authenticator, issuer: issuer, adminKey: adminKey, log: log}

	r := chi.NewRouter()
	r.Use(chimw.RequestID)
	r.Use(chimw.RealIP)
	r.Use(chimw.Recoverer)

	r.Get("/health", h.health)
	r.Get("/swagger/*", httpSwagger.WrapHandler)

	// /auth/token  — public, validates credentials and returns identity.
	// /auth/issue  — requires valid credentials, upgrades to a JWT.
	// /tenants     — requires X-Admin-Key, provisions a new tenant + issues credentials.
	r.Post("/auth/token", h.authToken)
	r.With(authmw.Auth(authenticator)).Post("/auth/issue", h.authIssue)
	r.Post("/tenants", h.createTenant)

	r.Route("/events", func(r chi.Router) {
		r.Use(authmw.Auth(authenticator))
		r.Post("/", h.ingest)
		r.Get("/", h.listEvents)
		r.Get("/{streamID}", h.getByStream)
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
		h.log.Error("failed to issue token", "subject", identity.SubjectID, "error", err)
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
	if r.Header.Get("X-Admin-Key") != h.adminKey {
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
			h.log.Error("failed to issue token for tenant", "tenant_id", req.TenantID, "error", err)
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
// @Description  Results come from the Elasticsearch read model and are **eventually consistent**
// @Description  with the PostgreSQL event store — recently ingested events may not appear immediately.
// @Tags         events
// @Produce      json
// @Security     ApiKeyAuth
// @Security     BearerAuth
// @Param        limit   query     int  false  "Page size — default 20, max 100"
// @Param        offset  query     int  false  "Number of events to skip — default 0"
// @Success      200     {object}  ListEventsResponse
// @Failure      401     {object}  ErrorResponse  "Missing or invalid credentials"
// @Failure      500     {object}  ErrorResponse  "Read model query failed"
// @Router       /events [get]
func (h *handler) listEvents(w http.ResponseWriter, r *http.Request) {
	limit := parseIntParam(r, "limit", 20)
	offset := parseIntParam(r, "offset", 0)

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	result, err := h.querySvc.ListAll(ctx, query.ListQuery{
		Limit:  limit,
		Offset: offset,
	})
	if err != nil {
		h.log.Error("list events failed", "error", err)
		writeError(w, http.StatusInternalServerError, "query failed")
		return
	}

	events := make([]EventResponse, len(result.Events))
	for i, e := range result.Events {
		events[i] = domainToEventResponse(e)
	}

	w.Header().Set("X-Read-Model", "elasticsearch")
	w.Header().Set("X-Data-Consistency", "eventual")
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
	var req IngestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	e, err := h.ingestSvc.Ingest(r.Context(), ingest.Command{
		StreamID: req.StreamID,
		Type:     req.Type,
		Source:   req.Source,
		Payload:  req.Payload, // already json.RawMessage — no re-encoding
		Metadata: req.Metadata,
	})
	if err != nil {
		h.log.Error("ingest failed", "error", err)
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

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	result, err := h.querySvc.QueryByStream(ctx, query.Query{
		StreamID: streamID,
		Limit:    limit,
		Offset:   offset,
	})
	if err != nil {
		h.log.Error("query by stream failed", "stream_id", streamID, "error", err)
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
		ID:         e.ID.String(),
		TenantID:   e.TenantID,
		StreamID:   e.StreamID,
		Type:       e.Type,
		Source:     e.Source,
		Version:    e.Version,
		OccurredAt: e.OccurredAt,
		Payload:    e.Payload,
		Metadata:   e.Metadata,
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
