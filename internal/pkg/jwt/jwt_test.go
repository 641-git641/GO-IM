package jwt

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestSignAndValidate(t *testing.T) {
	m := New("test-secret", 1*time.Hour)

	token, err := m.Generate("user-1", "Alice", "user")
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if token == "" {
		t.Fatal("token is empty")
	}

	claims, err := m.Validate(token)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if claims.UID != "user-1" {
		t.Errorf("expected UID 'user-1', got '%s'", claims.UID)
	}
	if claims.Username != "Alice" {
		t.Errorf("expected Username 'Alice', got '%s'", claims.Username)
	}
	if claims.Issuer != "im-server" {
		t.Errorf("expected Issuer 'im-server', got '%s'", claims.Issuer)
	}
}

func TestExpiredToken(t *testing.T) {
	// Use a negative expiration so the token is already expired
	m := New("test-secret", -1*time.Second)

	token, err := m.Generate("user-1", "Alice", "user")
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	_, err = m.Validate(token)
	if err == nil {
		t.Fatal("expected error for expired token, got nil")
	}
	if !errors.Is(err, ErrTokenExpired) {
		t.Errorf("expected ErrTokenExpired, got: %v", err)
	}
}

func TestTamperedToken(t *testing.T) {
	m := New("test-secret", 1*time.Hour)

	token, err := m.Generate("user-1", "Alice", "user")
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	// Tamper with the payload by appending garbage
	tampered := token + "extra-garbage"

	_, err = m.Validate(tampered)
	if err == nil {
		t.Fatal("expected error for tampered token, got nil")
	}
}

func TestWrongSecret(t *testing.T) {
	m1 := New("secret-a", 1*time.Hour)
	m2 := New("secret-b", 1*time.Hour)

	token, err := m1.Generate("user-1", "Alice", "user")
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	_, err = m2.Validate(token)
	if err == nil {
		t.Fatal("expected error for wrong secret, got nil")
	}
	if !errors.Is(err, ErrTokenSignature) {
		t.Errorf("expected ErrTokenSignature, got: %v", err)
	}
}

func TestEmptyToken(t *testing.T) {
	m := New("test-secret", 1*time.Hour)

	_, err := m.Validate("")
	if err == nil {
		t.Fatal("expected error for empty token, got nil")
	}
}

func TestMalformedToken(t *testing.T) {
	m := New("test-secret", 1*time.Hour)

	tests := []struct {
		name  string
		token string
	}{
		{"not a jwt", "this-is-not-a-jwt"},
		{"three parts but invalid", "a.b.c"},
		{"base64 but garbage", strings.Repeat("x", 100)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := m.Validate(tt.token)
			if err == nil {
				t.Errorf("expected error for '%s', got nil", tt.name)
			}
			t.Logf("error for '%s': %v", tt.name, err)
		})
	}
}

func TestMultipleUsers(t *testing.T) {
	m := New("test-secret", 1*time.Hour)

	users := []struct{ uid, username string }{
		{"uid-1", "Alice"},
		{"uid-2", "Bob"},
		{"uid-3", "Charlie"},
	}

	for _, u := range users {
		token, err := m.Generate(u.uid, u.username, "user")
		if err != nil {
			t.Fatalf("Generate(%s): %v", u.uid, err)
		}
		claims, err := m.Validate(token)
		if err != nil {
			t.Fatalf("Validate(%s): %v", u.uid, err)
		}
		if claims.UID != u.uid {
			t.Errorf("expected UID %s, got %s", u.uid, claims.UID)
		}
	}
}

func TestClaimsExpiryWindow(t *testing.T) {
	// JWT timestamps have second precision, so use whole seconds.
	m := New("test-secret", 2*time.Second)

	token, err := m.Generate("user-1", "Alice", "user")
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	// Immediately validate should work
	claims, err := m.Validate(token)
	if err != nil {
		t.Fatalf("Validate before expiry: %v", err)
	}
	if claims.UID != "user-1" {
		t.Errorf("expected UID 'user-1', got '%s'", claims.UID)
	}

	// Wait for expiry
	time.Sleep(3 * time.Second)

	_, err = m.Validate(token)
	if err == nil {
		t.Fatal("expected error after expiry, got nil")
	}
	if !errors.Is(err, ErrTokenExpired) {
		t.Errorf("expected ErrTokenExpired, got: %v", err)
	}
}
