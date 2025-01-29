//go:build linux

package database

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	configLib "github.com/sonroyaalmerol/pbs-plus/internal/config"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

func (database *Database) RegisterTokenPlugin() {
	plugin := &configLib.SectionPlugin[types.AgentToken]{
		TypeName:   "token",
		FolderPath: database.paths["tokens"],
		Validate: func(config types.AgentToken) error {
			if err := database.TokenManager.ValidateToken(config.Token); err != nil {
				return fmt.Errorf("invalid token: %w", err)
			}
			return nil
		},
	}

	database.tokensConfig = configLib.NewSectionConfig(plugin)
}

func (database *Database) CreateToken(comment string) error {
	token, err := database.TokenManager.GenerateToken()
	if err != nil {
		return fmt.Errorf("CreateToken: error generating token: %w", err)
	}

	configData := &configLib.ConfigData[types.AgentToken]{
		Sections: map[string]*configLib.Section[types.AgentToken]{
			token: {
				Type: "token",
				ID:   token,
				Properties: types.AgentToken{
					Token:     token,
					Comment:   comment,
					CreatedAt: time.Now().Unix(),
					Revoked:   false,
				},
			},
		},
		Order: []string{token},
	}

	if err := database.tokensConfig.Write(configData); err != nil {
		return fmt.Errorf("CreateToken: error writing config: %w", err)
	}

	return nil
}

func (database *Database) GetToken(token string) (*types.AgentToken, error) {
	configPath := filepath.Join(database.paths["tokens"], utils.EncodePath(token)+".cfg")
	configData, err := database.tokensConfig.Parse(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("GetToken: error reading config: %w", err)
	}

	section, exists := configData.Sections[token]
	if !exists {
		return nil, nil
	}

	tokenProp := &section.Properties
	revoked := tokenProp.Revoked

	// Double-check token validity
	if err := database.TokenManager.ValidateToken(token); err != nil {
		revoked = true
	}

	tokenProp.Revoked = revoked

	return tokenProp, nil
}

func (database *Database) GetAllTokens() ([]types.AgentToken, error) {
	files, err := os.ReadDir(database.paths["tokens"])
	if err != nil {
		return nil, fmt.Errorf("GetAllTokens: error reading directory: %w", err)
	}

	var tokens []types.AgentToken
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		token, err := database.GetToken(utils.DecodePath(strings.TrimSuffix(file.Name(), ".cfg")))
		if err != nil || token == nil {
			syslog.L.Errorf("GetAllTokens: error getting token: %v", err)
			continue
		}

		tokens = append(tokens, *token)
	}

	return tokens, nil
}

func (database *Database) RevokeToken(token *types.AgentToken) error {
	if token.Revoked {
		return nil
	}

	token.Revoked = true

	configData := &configLib.ConfigData[types.AgentToken]{
		Sections: map[string]*configLib.Section[types.AgentToken]{
			token.Token: {
				Type:       "token",
				ID:         token.Token,
				Properties: *token,
			},
		},
		Order: []string{token.Token},
	}

	if err := database.tokensConfig.Write(configData); err != nil {
		return fmt.Errorf("RevokeToken: error writing config: %w", err)
	}

	return nil
}
