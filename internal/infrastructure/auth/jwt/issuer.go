package jwt

import (
	"fmt"
	"time"

	gjwt "github.com/golang-jwt/jwt/v5"

	appauth "github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/application/auth"
)

const defaultExpiry = time.Hour

// Issue mints a new HMAC-signed JWT for the identity contained in cmd.
// The token inherits subject, tenant_id, and roles from the caller's Identity,
// ensuring the issued token carries exactly the same authorization scope.
//
// Authenticator implements both infraauth.Authenticator and appauth.Issuer,
// so a single jwt.New(secret) instance serves both ports.
func (a *Authenticator) Issue(cmd appauth.IssueCommand) (appauth.IssuedToken, error) {
	expiry := cmd.ExpiresIn
	if expiry <= 0 {
		expiry = defaultExpiry
	}
	expiresAt := time.Now().UTC().Add(expiry)

	c := claims{
		RegisteredClaims: gjwt.RegisteredClaims{
			Subject:   cmd.Identity.SubjectID,
			ExpiresAt: gjwt.NewNumericDate(expiresAt),
			IssuedAt:  gjwt.NewNumericDate(time.Now().UTC()),
		},
		TenantID: cmd.Identity.TenantID,
		Roles:    cmd.Identity.Roles,
	}

	token, err := gjwt.NewWithClaims(gjwt.SigningMethodHS256, c).SignedString(a.secret)
	if err != nil {
		return appauth.IssuedToken{}, fmt.Errorf("sign token: %w", err)
	}

	return appauth.IssuedToken{
		Token:     token,
		ExpiresAt: expiresAt,
	}, nil
}
