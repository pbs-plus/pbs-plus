//go:build linux

package database

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	configLib "github.com/sonroyaalmerol/pbs-plus/internal/config"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/pattern"
)

func (database *Database) RegisterExclusionPlugin() {
	plugin := &configLib.SectionPlugin[types.Exclusion]{
		TypeName:   "exclusion",
		FolderPath: database.paths["exclusions"],
		Validate: func(config types.Exclusion) error {
			if !pattern.IsValidPattern(config.Path) {
				return fmt.Errorf("invalid exclusion pattern: %s", config.Path)
			}
			return nil
		},
	}

	database.exclusionsConfig = configLib.NewSectionConfig(plugin)
}

func (database *Database) CreateExclusion(exclusion types.Exclusion) error {
	exclusion.Path = strings.ReplaceAll(exclusion.Path, "\\", "/")

	if !pattern.IsValidPattern(exclusion.Path) {
		return fmt.Errorf("CreateExclusion: invalid path pattern -> %s", exclusion.Path)
	}

	filename := "global"
	if exclusion.JobID != "" {
		filename = utils.EncodePath(exclusion.JobID)
	}

	configPath := filepath.Join(database.paths["exclusions"], filename+".cfg")

	// Read existing exclusions
	var configData *configLib.ConfigData[types.Exclusion]
	existing, err := database.exclusionsConfig.Parse(configPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("CreateExclusion: error reading existing config: %w", err)
	}

	if existing != nil {
		configData = existing
	} else {
		configData = &configLib.ConfigData[types.Exclusion]{
			Sections: make(map[string]*configLib.Section[types.Exclusion]),
			Order:    make([]string, 0),
			FilePath: configPath,
		}
	}

	// Add new exclusion
	sectionID := fmt.Sprintf("excl-%s", exclusion.Path)
	configData.Sections[sectionID] = &configLib.Section[types.Exclusion]{
		Type:       "exclusion",
		ID:         sectionID,
		Properties: exclusion,
	}
	configData.Order = append(configData.Order, sectionID)

	if err := database.exclusionsConfig.Write(configData); err != nil {
		return fmt.Errorf("CreateExclusion: error writing config: %w", err)
	}

	return nil
}

func (database *Database) GetAllJobExclusions(jobId string) ([]types.Exclusion, error) {
	configPath := filepath.Join(database.paths["exclusions"], utils.EncodePath(jobId)+".cfg")
	configData, err := database.exclusionsConfig.Parse(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []types.Exclusion{}, nil
		}
		return nil, fmt.Errorf("GetAllJobExclusions: error reading config: %w", err)
	}

	var exclusions []types.Exclusion
	seenPaths := make(map[string]bool)

	for _, sectionID := range configData.Order {
		section := configData.Sections[sectionID]
		if seenPaths[section.Properties.Path] {
			continue // Skip duplicates
		}
		seenPaths[section.Properties.Path] = true

		exclusions = append(exclusions, section.Properties)
	}
	return exclusions, nil
}

func (database *Database) GetAllGlobalExclusions() ([]types.Exclusion, error) {
	configPath := filepath.Join(database.paths["exclusions"], "global.cfg")
	configData, err := database.exclusionsConfig.Parse(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []types.Exclusion{}, nil
		}
		return nil, fmt.Errorf("GetAllGlobalExclusions: error reading config: %w", err)
	}

	var exclusions []types.Exclusion
	seenPaths := make(map[string]bool)

	for _, sectionID := range configData.Order {
		section := configData.Sections[sectionID]
		if seenPaths[section.Properties.Path] {
			continue // Skip duplicates
		}
		seenPaths[section.Properties.Path] = true

		exclusions = append(exclusions, section.Properties)
	}
	return exclusions, nil
}

func (database *Database) GetExclusion(path string) (*types.Exclusion, error) {
	// Check global exclusions first
	globalPath := filepath.Join(database.paths["exclusions"], "global.cfg")
	if configData, err := database.exclusionsConfig.Parse(globalPath); err == nil {
		sectionID := fmt.Sprintf("excl-%s", path)
		if section, exists := configData.Sections[sectionID]; exists {
			return &section.Properties, nil
		}
	}

	// Check job-specific exclusions
	files, err := os.ReadDir(database.paths["exclusions"])
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("GetExclusion: error reading directory: %w", err)
	}

	for _, file := range files {
		if file.Name() == "global" {
			continue
		}
		configPath := filepath.Join(database.paths["exclusions"], file.Name())
		configData, err := database.exclusionsConfig.Parse(configPath)
		if err != nil {
			continue
		}

		sectionID := fmt.Sprintf("excl-%s", path)
		if section, exists := configData.Sections[sectionID]; exists {
			return &section.Properties, nil
		}
	}

	return nil, fmt.Errorf("GetExclusion: exclusion not found for path: %s", path)
}

func (database *Database) UpdateExclusion(exclusion types.Exclusion) error {
	exclusion.Path = strings.ReplaceAll(exclusion.Path, "\\", "/")
	if !pattern.IsValidPattern(exclusion.Path) {
		return fmt.Errorf("UpdateExclusion: invalid path pattern -> %s", exclusion.Path)
	}

	configPath := filepath.Join(database.paths["exclusions"], "global.cfg")
	if exclusion.JobID != "" {
		configPath = filepath.Join(database.paths["exclusions"], utils.EncodePath(exclusion.JobID)+".cfg")
	}

	configData, err := database.exclusionsConfig.Parse(configPath)
	if err != nil {
		return fmt.Errorf("UpdateExclusion: error reading config: %w", err)
	}

	sectionID := fmt.Sprintf("excl-%s", exclusion.Path)
	_, exists := configData.Sections[sectionID]
	if !exists {
		return fmt.Errorf("UpdateExclusion: exclusion not found for path: %s", exclusion.Path)
	}

	// Update properties
	configData.Sections[sectionID].Properties = exclusion
	return database.exclusionsConfig.Write(configData)
}

func (database *Database) DeleteExclusion(path string) error {
	path = strings.ReplaceAll(path, "\\", "/")
	sectionID := fmt.Sprintf("excl-%s", path)

	// Try job-specific exclusions first
	files, err := os.ReadDir(database.paths["exclusions"])
	if err != nil {
		return fmt.Errorf("DeleteExclusion: error reading directory: %w", err)
	}

	for _, file := range files {
		if file.Name() == "global" {
			continue
		}
		configPath := filepath.Join(database.paths["exclusions"], file.Name())
		configData, err := database.exclusionsConfig.Parse(configPath)
		if err != nil {
			continue
		}

		if _, exists := configData.Sections[sectionID]; exists {
			delete(configData.Sections, sectionID)
			newOrder := make([]string, 0)
			for _, id := range configData.Order {
				if id != sectionID {
					newOrder = append(newOrder, id)
				}
			}
			configData.Order = newOrder

			// If the config is empty after deletion, remove the file
			if len(configData.Sections) == 0 {
				if err := os.Remove(configPath); err != nil {
					return fmt.Errorf("DeleteExclusion: error removing empty config file: %w", err)
				}
				return nil
			}

			// Otherwise write the updated config
			if err := database.exclusionsConfig.Write(configData); err != nil {
				return fmt.Errorf("DeleteExclusion: error writing config: %w", err)
			}
			return nil
		}
	}

	// Try global exclusion
	globalPath := filepath.Join(database.paths["exclusions"], "global.cfg")
	if configData, err := database.exclusionsConfig.Parse(globalPath); err == nil {
		if _, exists := configData.Sections[sectionID]; exists {
			delete(configData.Sections, sectionID)
			newOrder := make([]string, 0)
			for _, id := range configData.Order {
				if id != sectionID {
					newOrder = append(newOrder, id)
				}
			}
			configData.Order = newOrder

			// If the global config is empty after deletion, remove the file
			if len(configData.Sections) == 0 {
				if err := os.Remove(globalPath); err != nil {
					return fmt.Errorf("DeleteExclusion: error removing empty global config file: %w", err)
				}
				return nil
			}

			// Otherwise write the updated config
			if err := database.exclusionsConfig.Write(configData); err != nil {
				return fmt.Errorf("DeleteExclusion: error writing global config: %w", err)
			}
			return nil
		}
	}

	return fmt.Errorf("DeleteExclusion: exclusion not found for path: %s", path)
}
