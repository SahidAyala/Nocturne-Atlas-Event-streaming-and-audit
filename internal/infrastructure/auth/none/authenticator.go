// Package none provides a passthrough Authenticator for development.
// Every request is accepted and receives a fixed default Identity.
//
// Use AUTH_MODE=none (the default) when running locally — no headers required.
// Switch to simple or jwt when you need real access control.
package none

import (
	"context"
	"net/http"

	appauth "github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/application/auth"
)

// Authenticator always succeeds and injects a default Identity.
// Satisfies infraauth.Authenticator without any credentials check.
type Authenticator struct{}

func New() *Authenticator { return &Authenticator{} }

func (Authenticator) Authenticate(_ context.Context, _ *http.Request) (appauth.Identity, error) {
	return appauth.Identity{
		SubjectID: "anonymous",
		TenantID:  "default",
		Roles:     []string{"writer", "reader"},
	}, nil
}
