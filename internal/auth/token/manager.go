package token

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt"
	authErrors "github.com/sonroyaalmerol/pbs-plus/internal/auth/errors"
)

// Claims represents the JWT claims
type Claims struct {
	jwt.StandardClaims
}

// Manager handles token generation and validation
type Manager struct {
	secret []byte
	config Config
}

// Config represents token manager configuration
type Config struct {
	// TokenExpiration is the duration for which a token is valid
	TokenExpiration time.Duration
	// SecretKey is the key used to sign tokens
	SecretKey string
}

// NewManager creates a new token manager
func NewManager(config Config) (*Manager, error) {
	if config.SecretKey == "" {
		// Generate a random secret if none provided
		secret := make([]byte, 32)
		if _, err := rand.Read(secret); err != nil {
			return nil, authErrors.WrapError("generate_secret", err)
		}
		config.SecretKey = base64.StdEncoding.EncodeToString(secret)
	}

	if config.TokenExpiration == 0 {
		config.TokenExpiration = 24 * time.Hour
	}

	m := &Manager{
		secret: []byte(config.SecretKey),
		config: config,
	}

	return m, nil
}

// GenerateToken creates a new JWT token for an agent
func (m *Manager) GenerateToken() (string, error) {
	claims := Claims{
		StandardClaims: jwt.StandardClaims{
			ExpiresAt: time.Now().Add(m.config.TokenExpiration).Unix(),
			IssuedAt:  time.Now().Unix(),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(m.secret)
	if err != nil {
		return "", authErrors.WrapError("generate_token", err)
	}

	return tokenString, nil
}

// ValidateToken checks if a token is valid
func (m *Manager) ValidateToken(tokenString string) error {
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, authErrors.WrapError("validate_token", fmt.Errorf("unexpected signing method: %v", token.Header["alg"]))
		}
		return m.secret, nil
	})
	if err != nil {
		return authErrors.WrapError("validate_token", err)
	}

	if _, ok := token.Claims.(*Claims); ok && token.Valid {
		return nil
	}

	return authErrors.ErrInvalidToken
}
