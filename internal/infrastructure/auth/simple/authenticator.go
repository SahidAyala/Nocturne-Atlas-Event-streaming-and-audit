package simple

import (
	"context"
	"errors"
	"net/http"

	appauth "github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/application/auth"
	infraauth "github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/infrastructure/auth"
)

// Authenticator validates requests using a static API key in the X-API-Key header.
// Suitable for local development and single-tenant deployments.
// TenantID is fixed to "default"; SubjectID is set to "api-key".
type Authenticator struct {
	apiKey string
}

// Compile-time check that Authenticator satisfies the port.
var _ infraauth.Authenticator = (*Authenticator)(nil)

func New(apiKey string) *Authenticator {
	return &Authenticator{apiKey: apiKey}
}

func (a *Authenticator) Authenticate(_ context.Context, r *http.Request) (appauth.Identity, error) {
	key := r.Header.Get("X-API-Key")
	if key == "" {
		return appauth.Identity{}, errors.New("missing X-API-Key header")
	}
	if key != a.apiKey {
		return appauth.Identity{}, errors.New("invalid API key")
	}
	return appauth.Identity{
		SubjectID: "api-key",
		TenantID:  "default",
		Roles:     []string{"writer", "reader"},
	}, nil
}
