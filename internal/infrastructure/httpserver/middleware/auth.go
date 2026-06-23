package middleware

import (
	"net/http"

	appauth "github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/application/auth"
	infraauth "github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/infrastructure/auth"
)

// Auth returns an HTTP middleware that authenticates every request.
// On success it injects the Identity into the request context.
// On failure it responds with 401 Unauthorized and halts the chain.
func Auth(authenticator infraauth.Authenticator) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			identity, err := authenticator.Authenticate(r.Context(), r)
			if err != nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte(`{"error":"unauthorized"}`)) //nolint:errcheck
				return
			}
			next.ServeHTTP(w, r.WithContext(appauth.WithIdentity(r.Context(), identity)))
		})
	}
}
