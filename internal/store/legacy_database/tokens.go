//go:build linux

package database

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	configLib "github.com/pbs-plus/pbs-plus/internal/legacy_config"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

func (database *Database) RegisterTokenPlugin() {
	plugin := &configLib.SectionPlugin[types.AgentToken]{
		TypeName:   "token",
		FolderPath: database.paths["tokens"],
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
					CreatedAt: int(time.Now().Unix()),
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

func (database *Database) GetToken(token string) (types.AgentToken, error) {
	configPath := filepath.Join(database.paths["tokens"], utils.EncodePath(token)+".cfg")
	configData, err := database.tokensConfig.Parse(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return types.AgentToken{}, err
		}
		return types.AgentToken{}, fmt.Errorf("GetToken: error reading config: %w", err)
	}

	section, exists := configData.Sections[token]
	if !exists {
		return types.AgentToken{}, fmt.Errorf("GetToken: section %s does not exist", token)
	}

	tokenProp := section.Properties
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

		decoded, err := utils.DecodePath(strings.TrimSuffix(file.Name(), ".cfg"))
		if err != nil {
			syslog.L.Error(err).WithField("id", file.Name())
			continue
		}

		token, err := database.GetToken(decoded)
		if err != nil {
			syslog.L.Error(err).WithField("id", file.Name())
			continue
		}

		tokens = append(tokens, token)
	}

	return tokens, nil
}

func (database *Database) RevokeToken(token types.AgentToken) error {
	if token.Revoked {
		return nil
	}

	token.Revoked = true

	configData := &configLib.ConfigData[types.AgentToken]{
		Sections: map[string]*configLib.Section[types.AgentToken]{
			token.Token: {
				Type:       "token",
				ID:         token.Token,
				Properties: token,
			},
		},
		Order: []string{token.Token},
	}

	if err := database.tokensConfig.Write(configData); err != nil {
		return fmt.Errorf("RevokeToken: error writing config: %w", err)
	}

	return nil
}
