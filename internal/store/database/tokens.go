package database

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	configLib "github.com/sonroyaalmerol/pbs-plus/internal/config"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

func (database *Database) RegisterTokenPlugin() {
	plugin := &configLib.SectionPlugin{
		FolderPath: database.paths["tokens"],
		TypeName:   "token",
		Properties: map[string]*configLib.Schema{
			"token": {
				Type:        configLib.TypeString,
				Description: "JWT Token",
				Required:    true,
			},
			"comment": {
				Type:        configLib.TypeString,
				Description: "Comment",
				Required:    false,
			},
			"created_at": {
				Type:        configLib.TypeString,
				Description: "Date/time created",
				Required:    true,
			},
			"revoked": {
				Type:        configLib.TypeBool,
				Description: "Token revoked",
				Required:    false,
			},
		},
	}

	database.config.RegisterPlugin(plugin)
}

func (database *Database) CreateToken(comment string) error {
	database.mu.Lock()
	defer database.mu.Unlock()

	token, err := database.TokenManager.GenerateToken()
	if err != nil {
		return fmt.Errorf("CreateToken: error generating token: %w", err)
	}

	configData := &configLib.ConfigData{
		Sections: map[string]*configLib.Section{
			token: {
				Type: "token",
				ID:   token,
				Properties: map[string]string{
					"token":      token,
					"comment":    comment,
					"created_at": strconv.FormatInt(time.Now().Unix(), 10),
					"revoked":    "false",
				},
			},
		},
		Order: []string{token},
	}

	if err := database.config.Write(configData); err != nil {
		return fmt.Errorf("CreateToken: error writing config: %w", err)
	}

	return nil
}

func (database *Database) GetToken(token string) (*types.AgentToken, error) {
	database.mu.RLock()
	defer database.mu.RUnlock()

	plugin := database.config.GetPlugin("token")
	configPath := filepath.Join(plugin.FolderPath, utils.EncodePath(token)+".cfg")
	configData, err := database.config.Parse(configPath)
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

	createdAt, err := strconv.ParseInt(section.Properties["created_at"], 10, 64)
	if err != nil {
		createdAt = 0
	}

	revoked, err := strconv.ParseBool(section.Properties["revoked"])
	if err != nil {
		revoked = false
	}

	if err := database.TokenManager.ValidateToken(token); err != nil {
		revoked = true
	}

	return &types.AgentToken{
		Token:     section.Properties["token"],
		Comment:   section.Properties["comment"],
		CreatedAt: createdAt,
		Revoked:   revoked,
	}, nil
}

func (database *Database) GetAllTokens() ([]types.AgentToken, error) {
	plugin := database.config.GetPlugin("token")
	files, err := os.ReadDir(plugin.FolderPath)
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

		if token.Revoked {
			continue
		}

		tokens = append(tokens, *token)
	}

	return tokens, nil
}

func (database *Database) RevokeToken(token *types.AgentToken) error {
	database.mu.Lock()
	defer database.mu.Unlock()

	if token.Revoked {
		return nil
	}

	configData := &configLib.ConfigData{
		Sections: map[string]*configLib.Section{
			token.Token: {
				Type: "token",
				ID:   token.Token,
				Properties: map[string]string{
					"token":      token.Token,
					"comment":    token.Comment,
					"created_at": strconv.FormatInt(token.CreatedAt, 10),
					"revoked":    "true",
				},
			},
		},
		Order: []string{token.Token},
	}

	if err := database.config.Write(configData); err != nil {
		return fmt.Errorf("RevokeToken: error writing config: %w", err)
	}

	return nil
}
