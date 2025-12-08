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
	SecretKey string
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

func (m *TokenManager) GenerateToken() (string, error) {
	claims := Claims{
		StandardClaims: jwt.StandardClaims{
			ExpiresAt: time.Now().Add(m.config.TokenExpiration).Unix(),
			IssuedAt:  time.Now().Unix(),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(m.secret)
	if err != nil {
		return "", WrapError("generate_token", err)
	}

	return tokenString, nil
}

func (m *TokenManager) ValidateToken(tokenString string) error {
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (interface{}, error) {
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
