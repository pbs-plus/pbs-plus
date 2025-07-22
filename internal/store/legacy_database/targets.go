//go:build linux

package database

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	configLib "github.com/pbs-plus/pbs-plus/internal/legacy_config"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

func (database *Database) RegisterTargetPlugin() {
	plugin := &configLib.SectionPlugin[types.Target]{
		TypeName:   "target",
		FolderPath: database.paths["targets"],
		Validate: func(config types.Target) error {
			if config.Path == "" {
				return fmt.Errorf("target path empty")
			}
			if !utils.ValidateTargetPath(config.Path) {
				return fmt.Errorf("invalid target path: %s", config.Path)
			}
			return nil
		},
	}

	database.targetsConfig = configLib.NewSectionConfig(plugin)
}

func (database *Database) CreateTarget(target types.Target) error {
	configData := &configLib.ConfigData[types.Target]{
		Sections: map[string]*configLib.Section[types.Target]{
			target.Name: {
				Type:       "target",
				ID:         target.Name,
				Properties: target,
			},
		},
		Order: []string{target.Name},
	}

	if err := database.targetsConfig.Write(configData); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return database.UpdateTarget(target)
		}
		return fmt.Errorf("CreateTarget: error writing config: %w", err)
	}

	return nil
}

func (database *Database) GetTarget(name string) (types.Target, error) {
	targetPath := filepath.Join(database.paths["targets"], utils.EncodePath(name)+".cfg")
	configData, err := database.targetsConfig.Parse(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			return types.Target{}, err
		}
		return types.Target{}, fmt.Errorf("GetTarget: error reading config: %w", err)
	}

	section, exists := configData.Sections[name]
	if !exists {
		return types.Target{}, fmt.Errorf("GetTarget: section %s does not exist", name)
	}

	target := section.Properties
	target.Name = name

	if strings.HasPrefix(target.Path, "agent://") {
		target.IsAgent = true
	} else {
		target.ConnectionStatus = utils.IsValid(target.Path)
		target.IsAgent = false
	}

	return target, nil
}

func (database *Database) UpdateTarget(target types.Target) error {
	configData := &configLib.ConfigData[types.Target]{
		Sections: map[string]*configLib.Section[types.Target]{
			target.Name: {
				Type:       "target",
				ID:         target.Name,
				Properties: target,
			},
		},
		Order: []string{target.Name},
	}

	if err := database.targetsConfig.Write(configData); err != nil {
		return fmt.Errorf("UpdateTarget: error writing config: %w", err)
	}

	return nil
}

func (database *Database) DeleteTarget(name string) error {
	targetPath := filepath.Join(database.paths["targets"], utils.EncodePath(name)+".cfg")
	if err := os.Remove(targetPath); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("DeleteTarget: error deleting target file: %w", err)
		}
	}

	return nil
}

func (database *Database) GetAllTargets() ([]types.Target, error) {
	files, err := os.ReadDir(database.paths["targets"])
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
			syslog.L.Error(err).WithField("id", file.Name()).Write()
			continue
		}
		targets = append(targets, target)
	}

	return targets, nil
}

func (database *Database) GetAllTargetsByIP(clientIP string) ([]types.Target, error) {
	files, err := os.ReadDir(database.paths["targets"])
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
			syslog.L.Error(err).WithField("id", file.Name()).Write()
			continue
		}

		// Check if it's an agent target and matches the clientIP
		if target.IsAgent {
			// Split path into parts: ["agent:", "", "<clientIP>", "<driveLetter>"]
			parts := strings.Split(target.Path, "/")
			if len(parts) >= 3 && parts[2] == clientIP {
				targets = append(targets, target)
			}
		}
	}

	return targets, nil
}
