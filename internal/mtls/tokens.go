package mtls

import (
	"time"

	"github.com/pbs-plus/pbs-plus/internal/crypto"
)

type TokenManager struct {
	manager *crypto.TokenManager
	config  TokenConfig
}

type TokenConfig = crypto.TokenConfig

func NewTokenManager(config TokenConfig) (*TokenManager, error) {
	m, err := crypto.NewTokenManager(config)
	if err != nil {
		return nil, err
	}
	return &TokenManager{manager: m, config: config}, nil
}

func (m *TokenManager) GenerateToken(expiration time.Duration) (string, error) {
	return m.manager.GenerateToken(expiration)
}

func (m *TokenManager) GetTokenRemainingDuration(tokenString string) time.Duration {
	return m.manager.GetTokenRemainingDuration(tokenString)
}

func (m *TokenManager) ValidateToken(tokenString string) error {
	return m.manager.ValidateToken(tokenString)
}
