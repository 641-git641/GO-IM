// Package jwt provides JWT token creation and validation.
package jwt

import (
	"errors"
	"fmt"
	"time"

	jwtlib "github.com/golang-jwt/jwt/v5"
)

// Sentinel errors for token validation.
var (
	ErrTokenExpired   = errors.New("token expired")
	ErrTokenInvalid   = errors.New("token invalid")
	ErrTokenSignature = errors.New("token signature mismatch")
)

// Claims carries the authenticated user's identity.
type Claims struct {
	UID      string `json:"uid"`
	Username string `json:"username"`
	Role     string `json:"role"` // "user" or "admin"
	jwtlib.RegisteredClaims
}

// Manager handles JWT operations.
type Manager struct {
	secret     []byte
	expiration time.Duration
}

// New creates a JWT Manager.
func New(secret string, expiration time.Duration) *Manager {
	return &Manager{
		secret:     []byte(secret),
		expiration: expiration,
	}
}

// Generate creates a signed JWT token for a user.
func (m *Manager) Generate(uid, username, role string) (string, error) {
	claims := Claims{
		UID:      uid,
		Username: username,
		Role:     role,
		RegisteredClaims: jwtlib.RegisteredClaims{
			ExpiresAt: jwtlib.NewNumericDate(time.Now().Add(m.expiration)),
			IssuedAt:  jwtlib.NewNumericDate(time.Now()),
			Issuer:    "im-server",
		},
	}
	token := jwtlib.NewWithClaims(jwtlib.SigningMethodHS256, claims)
	return token.SignedString(m.secret)
}

// Validate parses and validates a token string, returning the claims.
// Returns specific sentinel errors for callers to differentiate:
//   - ErrTokenExpired when the token has expired
//   - ErrTokenSignature when the signing method or secret is wrong
//   - ErrTokenInvalid for all other validation failures
func (m *Manager) Validate(tokenStr string) (*Claims, error) {
	token, err := jwtlib.ParseWithClaims(tokenStr, &Claims{},
		func(t *jwtlib.Token) (interface{}, error) {
			if _, ok := t.Method.(*jwtlib.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("%w: unexpected signing method: %v", ErrTokenSignature, t.Header["alg"])
			}
			return m.secret, nil
		})
	if err != nil {
		if errors.Is(err, jwtlib.ErrTokenExpired) {
			return nil, ErrTokenExpired
		}
		// Distinguish signature errors from other failures
		if errors.Is(err, jwtlib.ErrSignatureInvalid) {
			return nil, ErrTokenSignature
		}
		return nil, fmt.Errorf("%w: %w", ErrTokenInvalid, err)
	}
	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, ErrTokenInvalid
	}
	return claims, nil
}
