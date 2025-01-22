package token

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/golang-jwt/jwt"
	authErrors "github.com/sonroyaalmerol/pbs-plus/internal/auth/errors"
)

// Claims represents the JWT claims
type Claims struct {
	AgentID string `json:"agent_id"`
	jwt.StandardClaims
}

// Manager handles token generation and validation
type Manager struct {
	secret []byte
	tokens sync.Map
	config Config
}

// Config represents token manager configuration
type Config struct {
	// TokenExpiration is the duration for which a token is valid
	TokenExpiration time.Duration
	// SecretKey is the key used to sign tokens
	SecretKey string
	// MaxTokens is the maximum number of valid tokens allowed
	MaxTokens int
	// CleanupInterval is how often to clean up expired tokens
	CleanupInterval time.Duration
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

	if config.MaxTokens == 0 {
		config.MaxTokens = 1000
	}

	if config.CleanupInterval == 0 {
		config.CleanupInterval = 1 * time.Hour
	}

	m := &Manager{
		secret: []byte(config.SecretKey),
		config: config,
	}

	// Start cleanup routine
	go m.cleanupRoutine()

	return m, nil
}

// GenerateToken creates a new JWT token for an agent
func (m *Manager) GenerateToken(agentID string) (string, error) {
	// Check if we've reached the maximum number of tokens
	if m.countTokens() >= m.config.MaxTokens {
		return "", authErrors.WrapError("generate_token", errors.New("maximum token limit reached"))
	}

	claims := Claims{
		AgentID: agentID,
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

	m.tokens.Store(tokenString, time.Now().Add(m.config.TokenExpiration))
	return tokenString, nil
}

// ValidateToken checks if a token is valid
func (m *Manager) ValidateToken(tokenString string) (string, error) {
	// First check if token exists in our store
	if _, ok := m.tokens.Load(tokenString); !ok {
		return "", authErrors.ErrInvalidToken
	}

	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, authErrors.WrapError("validate_token", fmt.Errorf("unexpected signing method: %v", token.Header["alg"]))
		}
		return m.secret, nil
	})

	if err != nil {
		return "", authErrors.WrapError("validate_token", err)
	}

	if claims, ok := token.Claims.(*Claims); ok && token.Valid {
		return claims.AgentID, nil
	}

	return "", authErrors.ErrInvalidToken
}

// RevokeToken removes a token from the valid tokens list
func (m *Manager) RevokeToken(tokenString string) {
	m.tokens.Delete(tokenString)
}

// cleanupRoutine periodically removes expired tokens
func (m *Manager) cleanupRoutine() {
	ticker := time.NewTicker(m.config.CleanupInterval)
	for range ticker.C {
		now := time.Now()
		m.tokens.Range(func(key, value interface{}) bool {
			if expiry, ok := value.(time.Time); ok && now.After(expiry) {
				m.tokens.Delete(key)
			}
			return true
		})
	}
}

// countTokens returns the current number of valid tokens
func (m *Manager) countTokens() int {
	count := 0
	m.tokens.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}

// GetValidTokens returns a list of all valid tokens and their expiry times
func (m *Manager) GetValidTokens() map[string]time.Time {
	tokens := make(map[string]time.Time)
	m.tokens.Range(func(key, value interface{}) bool {
		if k, ok := key.(string); ok {
			if v, ok := value.(time.Time); ok {
				tokens[k] = v
			}
		}
		return true
	})
	return tokens
}
