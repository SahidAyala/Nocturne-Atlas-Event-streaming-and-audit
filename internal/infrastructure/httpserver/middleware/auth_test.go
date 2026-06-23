package middleware

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	appauth "github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/application/auth"
)

// stubAuthenticator lets tests control the Authenticate outcome.
type stubAuthenticator struct {
	identity appauth.Identity
	err      error
}

func (s *stubAuthenticator) Authenticate(_ context.Context, _ *http.Request) (appauth.Identity, error) {
	return s.identity, s.err
}

func TestAuthMiddleware_ValidCredentialsInjectIdentity(t *testing.T) {
	want := appauth.Identity{SubjectID: "user-1", TenantID: "acme", Roles: []string{"writer"}}
	stub := &stubAuthenticator{identity: want}

	var gotIdentity appauth.Identity
	var gotOK bool

	handler := Auth(stub)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		gotIdentity, gotOK = appauth.IdentityFromContext(r.Context())
	}))

	w := httptest.NewRecorder()
	r, _ := http.NewRequest(http.MethodGet, "/", nil)
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if !gotOK {
		t.Fatal("Identity must be present in context after successful auth")
	}
	if gotIdentity.SubjectID != want.SubjectID {
		t.Errorf("SubjectID = %q, want %q", gotIdentity.SubjectID, want.SubjectID)
	}
	if gotIdentity.TenantID != want.TenantID {
		t.Errorf("TenantID = %q, want %q", gotIdentity.TenantID, want.TenantID)
	}
}

func TestAuthMiddleware_InvalidCredentialsReturns401(t *testing.T) {
	stub := &stubAuthenticator{err: errors.New("bad credentials")}
	reached := false

	handler := Auth(stub)(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		reached = true
	}))

	w := httptest.NewRecorder()
	r, _ := http.NewRequest(http.MethodGet, "/", nil)
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}
	if reached {
		t.Error("handler must not be called when authentication fails")
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
}
