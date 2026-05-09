//go:build linux

package application

import (
	"time"

	"github.com/pbs-plus/pbs-plus/internal/store/database"
)

// AgentHostService encapsulates agent host management logic.
type AgentHostService struct {
	db *database.Database
}

func NewAgentHostService(db *database.Database) *AgentHostService {
	return &AgentHostService{db: db}
}

func (s *AgentHostService) Get(hostname string) (database.AgentHost, error) {
	return s.db.GetAgentHost(hostname)
}

func (s *AgentHostService) Create(tx *database.Transaction, host database.AgentHost) error {
	return s.db.CreateAgentHost(tx, host)
}

func (s *AgentHostService) Update(tx *database.Transaction, host database.AgentHost) error {
	return s.db.UpdateAgentHost(tx, host)
}

func (s *AgentHostService) Delete(hostname string) error {
	return s.db.DeleteAgentHost(nil, hostname)
}

// TokenService encapsulates token management logic.
type TokenService struct {
	db *database.Database
}

func NewTokenService(db *database.Database) *TokenService {
	return &TokenService{db: db}
}

func (s *TokenService) List() ([]database.AgentToken, error) {
	return s.db.GetAllTokens(false)
}

func (s *TokenService) Get(id string) (database.AgentToken, error) {
	return s.db.GetToken(id)
}

func (s *TokenService) Create(duration time.Duration, comment string) error {
	return s.db.CreateToken(duration, comment)
}

func (s *TokenService) Revoke(token database.AgentToken) error {
	return s.db.RevokeToken(token)
}
