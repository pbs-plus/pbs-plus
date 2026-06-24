package crypto

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"time"
)

const (
	defaultTokenSecretSize = 32
	defaultTokenDuration   = 24 * time.Hour

	tokenV2Prefix = "v2:"
)

type TokenClaims struct {
	IssuedAt  int64
	ExpiresAt int64
}

type TokenManager struct {
	secret []byte
	config TokenConfig
}

type TokenConfig struct {
	TokenExpiration time.Duration
	SecretKey       string
}

func NewTokenManager(config TokenConfig) (*TokenManager, error) {
	if config.SecretKey == "" {
		secret := make([]byte, defaultTokenSecretSize)
		if _, err := io.ReadFull(rand.Reader, secret); err != nil {
			return nil, fmt.Errorf("crypto: generate token secret: %w", err)
		}
		config.SecretKey = base64.StdEncoding.EncodeToString(secret)
	}
	if config.TokenExpiration == 0 {
		config.TokenExpiration = defaultTokenDuration
	}
	return &TokenManager{
		secret: []byte(config.SecretKey),
		config: config,
	}, nil
}

func (m *TokenManager) GenerateToken(expiration time.Duration) (string, error) {
	nonce := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("crypto: generate token nonce: %w", err)
	}

	issuedAt := time.Now().Unix()
	var expiresAt int64
	if expiration > 0 {
		expiresAt = time.Now().Add(expiration).Unix()
	} else {
		expiresAt = 0
	}

	mac := hmac.New(sha256.New, m.secret)
	fmt.Fprintf(mac, "%d:%d:%x", issuedAt, expiresAt, nonce)
	sig := mac.Sum(nil)

	token := tokenV2Prefix + fmt.Sprintf("%d:%s:%s", expiresAt, base64.RawURLEncoding.EncodeToString(nonce), base64.RawURLEncoding.EncodeToString(sig))
	return token, nil
}

func (m *TokenManager) ValidateToken(tokenString string) error {
	if !hasPrefix(tokenString, tokenV2Prefix) {
		return errors.New("crypto: unsupported token version")
	}
	rest := tokenString[len(tokenV2Prefix):]

	expiresAtStr, rest, ok := splitFirstColon(rest)
	if !ok {
		return errors.New("crypto: invalid token format")
	}
	expiresAt, err := parseInt64(expiresAtStr)
	if err != nil {
		return fmt.Errorf("crypto: parse token expiry: %w", err)
	}

	nonceB64, sigB64, ok := splitFirstColon(rest)
	if !ok {
		return errors.New("crypto: invalid token format")
	}

	nonce, err := base64.RawURLEncoding.DecodeString(nonceB64)
	if err != nil {
		return fmt.Errorf("crypto: decode nonce: %w", err)
	}
	sig, err := base64.RawURLEncoding.DecodeString(sigB64)
	if err != nil {
		return fmt.Errorf("crypto: decode signature: %w", err)
	}

	mac := hmac.New(sha256.New, m.secret)
	fmt.Fprintf(mac, "%d:%d:%x", time.Now().Unix(), expiresAt, nonce)
	expectedSig := mac.Sum(nil)

	if !hmac.Equal(sig, expectedSig) {
		return errors.New("crypto: token signature invalid")
	}

	if expiresAt > 0 && time.Now().Unix() > expiresAt {
		return errors.New("crypto: token expired")
	}

	return nil
}

func (m *TokenManager) GetTokenRemainingDuration(tokenString string) time.Duration {
	if !hasPrefix(tokenString, tokenV2Prefix) {
		return 0
	}
	rest := tokenString[len(tokenV2Prefix):]

	expiresAtStr, _, ok := splitFirstColon(rest)
	if !ok {
		return 0
	}
	expiresAt, err := parseInt64(expiresAtStr)
	if err != nil {
		return 0
	}
	if expiresAt == 0 {
		return -1
	}
	remaining := time.Until(time.Unix(expiresAt, 0))
	if remaining < 0 {
		return 0
	}
	return remaining
}

func parseInt64(s string) (int64, error) {
	var n int64
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("invalid digit %c", c)
		}
		n = n*10 + int64(c-'0')
	}
	return n, nil
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func splitFirstColon(s string) (before, after string, ok bool) {
	for i := 0; i < len(s); i++ {
		if s[i] == ':' {
			return s[:i], s[i+1:], true
		}
	}
	return s, "", false
}
