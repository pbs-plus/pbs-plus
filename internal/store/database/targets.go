//go:build linux

package database

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	configLib "github.com/sonroyaalmerol/pbs-plus/internal/config"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

func (database *Database) RegisterTargetPlugin() {
	plugin := &configLib.SectionPlugin{
		FolderPath: database.paths["targets"],
		TypeName:   "target",
		Properties: map[string]*configLib.Schema{
			"path": {
				Type:        configLib.TypeString,
				Description: "Target path",
				Required:    true,
			},
			"auth": {
				Type:        configLib.TypeString,
				Description: "Auth used by target (only applicable to agents)",
				Required:    false,
			},
			"token_used": {
				Type:        configLib.TypeString,
				Description: "Token used (only applicable to agents)",
				Required:    false,
			},
			"drive_type": {
				Type:        configLib.TypeString,
				Description: "Drive type (only applicable to agents)",
				Required:    false,
			},
		},
	}

	database.config.RegisterPlugin(plugin)
}

func (database *Database) CreateTarget(target types.Target) error {
	if target.Path == "" {
		return fmt.Errorf("UpdateTarget: target path empty -> %s", target.Path)
	}

	if !utils.ValidateTargetPath(target.Path) {
		return fmt.Errorf("UpdateTarget: invalid target path -> %s", target.Path)
	}

	configData := &configLib.ConfigData{
		Sections: map[string]*configLib.Section{
			target.Name: {
				Type: "target",
				ID:   target.Name,
				Properties: map[string]string{
					"path":       target.Path,
					"auth":       target.Auth,
					"token_used": target.TokenUsed,
					"drive_type": target.DriveType,
				},
			},
		},
		Order: []string{target.Name},
	}

	if err := database.config.Write(configData); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return database.UpdateTarget(target)
		}
		return fmt.Errorf("CreateTarget: error writing config: %w", err)
	}

	return nil
}

func (database *Database) GetTarget(name string) (*types.Target, error) {
	plugin := database.config.GetPlugin("target")
	targetPath := filepath.Join(plugin.FolderPath, utils.EncodePath(name)+".cfg")
	configData, err := database.config.Parse(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("GetTarget: error reading config: %w", err)
	}

	section, exists := configData.Sections[name]
	if !exists {
		return nil, nil
	}

	target := &types.Target{
		Name:      name,
		Path:      section.Properties["path"],
		Auth:      section.Properties["auth"],
		TokenUsed: section.Properties["token_used"],
		DriveType: section.Properties["drive_type"],
	}

	if strings.HasPrefix(target.Path, "agent://") {
		target.IsAgent = true
	} else {
		target.ConnectionStatus = utils.IsValid(target.Path)
		target.IsAgent = false
	}

	return target, nil
}

func (database *Database) UpdateTarget(target types.Target) error {
	if target.Path == "" {
		return fmt.Errorf("UpdateTarget: target path empty -> %s", target.Path)
	}

	if !utils.ValidateTargetPath(target.Path) {
		return fmt.Errorf("UpdateTarget: invalid target path -> %s", target.Path)
	}

	configData := &configLib.ConfigData{
		Sections: map[string]*configLib.Section{
			target.Name: {
				Type: "target",
				ID:   target.Name,
				Properties: map[string]string{
					"path":       target.Path,
					"auth":       target.Auth,
					"token_used": target.TokenUsed,
					"drive_type": target.DriveType,
				},
			},
		},
		Order: []string{target.Name},
	}

	if err := database.config.Write(configData); err != nil {
		return fmt.Errorf("UpdateTarget: error writing config: %w", err)
	}

	return nil
}

func (database *Database) DeleteTarget(name string) error {
	plugin := database.config.GetPlugin("target")
	targetPath := filepath.Join(plugin.FolderPath, utils.EncodePath(name)+".cfg")
	if err := os.Remove(targetPath); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("DeleteTarget: error deleting target file: %w", err)
		}
	}

	return nil
}

func (database *Database) GetAllTargets() ([]types.Target, error) {
	plugin := database.config.GetPlugin("target")
	files, err := os.ReadDir(plugin.FolderPath)
	if err != nil {
		return nil, fmt.Errorf("GetAllTargets: error reading targets directory: %w", err)
	}

	var targets []types.Target
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		target, err := database.GetTarget(utils.DecodePath(strings.TrimSuffix(file.Name(), ".cfg")))
		if err != nil {
			syslog.L.Errorf("GetAllTargets: error getting target: %v", err)
			continue
		}
		targets = append(targets, *target)
	}

	return targets, nil
}

func (database *Database) GetAllTargetsByIP(clientIP string) ([]types.Target, error) {
	// Get all targets first
	plugin := database.config.GetPlugin("target")
	files, err := os.ReadDir(plugin.FolderPath)
	if err != nil {
		return nil, fmt.Errorf("GetAllTargetsByIP: error reading targets directory: %w", err)
	}

	var targets []types.Target
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		target, err := database.GetTarget(utils.DecodePath(strings.TrimSuffix(file.Name(), ".cfg")))
		if err != nil {
			syslog.L.Errorf("GetAllTargets: error getting target: %v", err)
			continue
		}

		// Check if it's an agent target and matches the clientIP
		if target.IsAgent {
			// Split path into parts: ["agent:", "", "<clientIP>", "<driveLetter>"]
			parts := strings.Split(target.Path, "/")
			if len(parts) >= 3 && parts[2] == clientIP {
				targets = append(targets, *target)
			}
		}
	}

	return targets, nil
}
