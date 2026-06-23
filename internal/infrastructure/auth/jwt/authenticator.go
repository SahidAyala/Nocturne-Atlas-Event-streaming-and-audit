package jwt

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	gjwt "github.com/golang-jwt/jwt/v5"

	appauth "github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/application/auth"
	infraauth "github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/infrastructure/auth"
)

// Authenticator validates Bearer tokens signed with an HMAC secret.
// Required claims: sub (subject), tenant_id.
// Optional claims: roles ([]string).
// Suitable for multi-tenant production deployments.
type Authenticator struct {
	secret []byte
}

// Compile-time check that Authenticator satisfies the port.
var _ infraauth.Authenticator = (*Authenticator)(nil)

// claims is the JWT payload schema.
type claims struct {
	gjwt.RegisteredClaims
	TenantID string   `json:"tenant_id"`
	Roles    []string `json:"roles"`
}

func New(secret string) *Authenticator {
	return &Authenticator{secret: []byte(secret)}
}

func (a *Authenticator) Authenticate(_ context.Context, r *http.Request) (appauth.Identity, error) {
	header := r.Header.Get("Authorization")
	if header == "" {
		return appauth.Identity{}, errors.New("missing Authorization header")
	}
	parts := strings.SplitN(header, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return appauth.Identity{}, errors.New("authorization header must be: Bearer <token>")
	}

	var c claims
	token, err := gjwt.ParseWithClaims(parts[1], &c, func(t *gjwt.Token) (any, error) {
		if _, ok := t.Method.(*gjwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return a.secret, nil
	})
	if err != nil || !token.Valid {
		return appauth.Identity{}, fmt.Errorf("invalid token: %w", err)
	}

	sub, err := c.GetSubject()
	if err != nil || sub == "" {
		return appauth.Identity{}, errors.New("token missing sub claim")
	}
	if c.TenantID == "" {
		return appauth.Identity{}, errors.New("token missing tenant_id claim")
	}

	return appauth.Identity{
		SubjectID: sub,
		TenantID:  c.TenantID,
		Roles:     c.Roles,
	}, nil
}
