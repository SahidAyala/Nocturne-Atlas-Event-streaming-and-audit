package auth

import (
	"context"
	"net/http"

	appauth "github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/application/auth"
)

// Authenticator is the port for request authentication.
// Implementations inspect an HTTP request and return the caller's Identity.
// An error is returned when credentials are absent or invalid.
type Authenticator interface {
	Authenticate(ctx context.Context, r *http.Request) (appauth.Identity, error)
}
