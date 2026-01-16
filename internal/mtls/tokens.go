package mtls

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt"
)

type Claims struct {
	jwt.StandardClaims
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
		secret := make([]byte, 32)
		if _, err := rand.Read(secret); err != nil {
			return nil, WrapError("generate_secret", err)
		}
		config.SecretKey = base64.StdEncoding.EncodeToString(secret)
	}

	if config.TokenExpiration == 0 {
		config.TokenExpiration = 24 * time.Hour
	}

	m := &TokenManager{
		secret: []byte(config.SecretKey),
		config: config,
	}

	return m, nil
}

func (m *TokenManager) GenerateToken(expiration time.Duration) (string, error) {
	var expiresAt int64

	if expiration > 0 {
		expiresAt = time.Now().Add(expiration).Unix()
	} else if expiration < 0 {
		expiresAt = time.Now().Add(m.config.TokenExpiration).Unix()
	} else {
		expiresAt = 0
	}

	claims := Claims{
		StandardClaims: jwt.StandardClaims{
			IssuedAt: time.Now().Unix(),
		},
	}

	if expiresAt > 0 {
		claims.StandardClaims.ExpiresAt = expiresAt
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(m.secret)
	if err != nil {
		return "", WrapError("generate_token", err)
	}

	return tokenString, nil
}

func (m *TokenManager) GetTokenRemainingDuration(tokenString string) time.Duration {
	claims := &Claims{}

	_, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (any, error) {
		return m.secret, nil
	})

	if err != nil {
		return 0
	}

	if claims.StandardClaims.ExpiresAt == 0 {
		return -1
	}

	expirationTime := time.Unix(claims.StandardClaims.ExpiresAt, 0)
	remaining := time.Until(expirationTime)

	if remaining < 0 {
		return 0
	}

	return remaining
}

func (m *TokenManager) ValidateToken(tokenString string) error {
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, WrapError("validate_token", fmt.Errorf("unexpected signing method: %v", token.Header["alg"]))
		}
		return m.secret, nil
	})
	if err != nil {
		return WrapError("validate_token", err)
	}

	if _, ok := token.Claims.(*Claims); ok && token.Valid {
		return nil
	}

	return ErrInvalidToken
}
