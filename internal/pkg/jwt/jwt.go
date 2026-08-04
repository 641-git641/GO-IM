// Package jwt 提供 JWT 令牌的创建与校验。
package jwt

import (
	"errors"
	"fmt"
	"time"

	jwtlib "github.com/golang-jwt/jwt/v5"
)

// 用于令牌校验的哨兵错误。
var (
	ErrTokenExpired   = errors.New("token expired")
	ErrTokenInvalid   = errors.New("token invalid")
	ErrTokenSignature = errors.New("token signature mismatch")
)

// Claims 携带已认证用户的身份信息。
type Claims struct {
	UID      string `json:"uid"`
	Username string `json:"username"`
	Role     string `json:"role"` // "user" 或 "admin"
	jwtlib.RegisteredClaims
}

// Manager 负责 JWT 操作。
type Manager struct {
	secret     []byte
	expiration time.Duration
}

// New 创建一个 JWT Manager。
func New(secret string, expiration time.Duration) *Manager {
	return &Manager{
		secret:     []byte(secret),
		expiration: expiration,
	}
}

// Generate 为用户创建带签名的 JWT 令牌。
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

// Validate 解析并校验令牌字符串，返回其中的声明。
// 返回特定的哨兵错误，便于调用方区分：
//   - ErrTokenExpired 当令牌已过期时
//   - ErrTokenSignature 当签名方法或密钥错误时
//   - ErrTokenInvalid 其他所有校验失败时
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
		// 将签名错误与其他失败区分开来
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
