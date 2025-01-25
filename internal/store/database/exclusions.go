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
	plugin := &configLib.SectionPlugin{
		FolderPath: database.paths["exclusions"],
		TypeName:   "exclusion",
		Properties: map[string]*configLib.Schema{
			"path": {
				Type:        configLib.TypeString,
				Description: "Exclusion path pattern",
				Required:    true,
			},
			"comment": {
				Type:        configLib.TypeString,
				Description: "Exclusion comment",
				Required:    false,
			},
			"job_id": {
				Type:        configLib.TypeString,
				Description: "Associated job ID",
				Required:    false,
			},
		},
		Validations: []configLib.ValidationFunc{
			func(data map[string]string) error {
				if !pattern.IsValidPattern(data["path"]) {
					return fmt.Errorf("invalid exclusion pattern: %s", data["path"])
				}
				return nil
			},
		},
	}

	database.config.RegisterPlugin(plugin)
}

func (database *Database) CreateExclusion(exclusion types.Exclusion) error {
	database.mu.Lock()
	defer database.mu.Unlock()

	exclusion.Path = strings.ReplaceAll(exclusion.Path, "\\", "/")

	if !pattern.IsValidPattern(exclusion.Path) {
		return fmt.Errorf("CreateExclusion: invalid path pattern -> %s", exclusion.Path)
	}

	sectionType := "exclusion"
	filename := utils.EncodePath(exclusion.JobID)
	sectionProperties := map[string]string{
		"path":    exclusion.Path,
		"comment": exclusion.Comment,
		"job_id":  exclusion.JobID,
	}
	plugin := database.config.GetPlugin("exclusion")

	if exclusion.JobID == "" {
		sectionProperties = map[string]string{
			"path":    exclusion.Path,
			"comment": exclusion.Comment,
		}
		filename = "global"
	}

	configPath := filepath.Join(plugin.FolderPath, filename+".cfg")

	// Read existing exclusions
	var configData *configLib.ConfigData
	existing, err := database.config.Parse(configPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("CreateExclusion: error reading existing config: %w", err)
	}

	if existing != nil {
		configData = existing
	} else {
		configData = &configLib.ConfigData{
			Sections: make(map[string]*configLib.Section),
			Order:    make([]string, 0),
			FilePath: configPath,
		}
	}

	// Add new exclusion
	sectionID := fmt.Sprintf("excl-%s", exclusion.Path)
	configData.Sections[sectionID] = &configLib.Section{
		Type:       sectionType,
		ID:         sectionID,
		Properties: sectionProperties,
	}
	configData.Order = append(configData.Order, sectionID)

	if err := database.config.Write(configData); err != nil {
		return fmt.Errorf("CreateExclusion: error writing config: %w", err)
	}

	return nil
}

func (database *Database) GetAllJobExclusions(jobId string) ([]types.Exclusion, error) {
	database.mu.RLock()
	defer database.mu.RUnlock()
	plugin := database.config.GetPlugin("exclusion")
	configPath := filepath.Join(plugin.FolderPath, utils.EncodePath(jobId)+".cfg")
	configData, err := database.config.Parse(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []types.Exclusion{}, nil
		}
		return nil, fmt.Errorf("GetAllJobExclusions: error reading config: %w", err)
	}
	var exclusions []types.Exclusion
	seenPaths := make(map[string]bool)

	for _, sectionID := range configData.Order {
		section, exists := configData.Sections[sectionID]
		if !exists || section.Type != "exclusion" {
			continue
		}
		path := section.Properties["path"]
		if seenPaths[path] {
			continue // Skip duplicates
		}
		seenPaths[path] = true
		exclusions = append(exclusions, types.Exclusion{
			Path:    path,
			Comment: section.Properties["comment"],
			JobID:   jobId,
		})
	}
	return exclusions, nil
}

func (database *Database) GetAllGlobalExclusions() ([]types.Exclusion, error) {
	database.mu.RLock()
	defer database.mu.RUnlock()
	plugin := database.config.GetPlugin("exclusion")
	configPath := filepath.Join(plugin.FolderPath, "global.cfg")
	configData, err := database.config.Parse(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []types.Exclusion{}, nil
		}
		return nil, fmt.Errorf("GetAllGlobalExclusions: error reading config: %w", err)
	}
	var exclusions []types.Exclusion
	seenPaths := make(map[string]bool)

	for _, sectionID := range configData.Order {
		section, exists := configData.Sections[sectionID]
		if !exists || section.Type != "exclusion" {
			continue
		}
		path := section.Properties["path"]
		if seenPaths[path] {
			continue // Skip duplicates
		}
		seenPaths[path] = true
		exclusions = append(exclusions, types.Exclusion{
			Path:    path,
			Comment: section.Properties["comment"],
		})
	}
	return exclusions, nil
}

func (database *Database) GetExclusion(path string) (*types.Exclusion, error) {
	database.mu.RLock()
	defer database.mu.RUnlock()

	// Check global exclusions first
	plugin := database.config.GetPlugin("exclusion")
	globalPath := filepath.Join(plugin.FolderPath, "global.cfg")
	if configData, err := database.config.Parse(globalPath); err == nil {
		sectionID := fmt.Sprintf("excl-%s", path)
		if section, exists := configData.Sections[sectionID]; exists && section.Type == "exclusion" {
			return &types.Exclusion{
				Path:    section.Properties["path"],
				Comment: section.Properties["comment"],
			}, nil
		}
	}

	// Check job-specific exclusions
	files, err := os.ReadDir(plugin.FolderPath)
	if err != nil {
		return nil, fmt.Errorf("GetExclusion: error reading directory: %w", err)
	}

	for _, file := range files {
		if file.Name() == "global" {
			continue
		}
		configPath := filepath.Join(plugin.FolderPath, file.Name())
		configData, err := database.config.Parse(configPath)
		if err != nil {
			continue
		}

		sectionID := fmt.Sprintf("excl-%s", path)
		if section, exists := configData.Sections[sectionID]; exists && section.Type == "exclusion" {
			return &types.Exclusion{
				Path:    section.Properties["path"],
				Comment: section.Properties["comment"],
				JobID:   section.Properties["job_id"],
			}, nil
		}
	}

	return nil, fmt.Errorf("GetExclusion: exclusion not found for path: %s", path)
}

func (database *Database) UpdateExclusion(exclusion types.Exclusion) error {
	database.mu.Lock()
	defer database.mu.Unlock()

	exclusion.Path = strings.ReplaceAll(exclusion.Path, "\\", "/")
	if !pattern.IsValidPattern(exclusion.Path) {
		return fmt.Errorf("UpdateExclusion: invalid path pattern -> %s", exclusion.Path)
	}

	// Try to update in global exclusions first
	plugin := database.config.GetPlugin("exclusion")
	configPath := filepath.Join(plugin.FolderPath, "global.cfg")
	if exclusion.JobID != "" {
		configPath = filepath.Join(plugin.FolderPath, utils.EncodePath(exclusion.JobID)+".cfg")
	}

	configData, err := database.config.Parse(configPath)
	if err != nil {
		return fmt.Errorf("UpdateExclusion: error reading config: %w", err)
	}

	sectionID := fmt.Sprintf("excl-%s", exclusion.Path)
	section, exists := configData.Sections[sectionID]
	if !exists {
		return fmt.Errorf("UpdateExclusion: exclusion not found for path: %s", exclusion.Path)
	}

	// Update properties
	if exclusion.JobID == "" {
		section.Properties = map[string]string{
			"path":    exclusion.Path,
			"comment": exclusion.Comment,
		}
	} else {
		section.Properties = map[string]string{
			"path":    exclusion.Path,
			"comment": exclusion.Comment,
			"job_id":  exclusion.JobID,
		}
	}

	return database.config.Write(configData)
}

func (database *Database) DeleteExclusion(path string) error {
	database.mu.Lock()
	defer database.mu.Unlock()

	path = strings.ReplaceAll(path, "\\", "/")
	plugin := database.config.GetPlugin("exclusion")
	sectionID := fmt.Sprintf("excl-%s", path)

	// Try job-specific exclusions first
	files, err := os.ReadDir(plugin.FolderPath)
	if err != nil {
		return fmt.Errorf("DeleteExclusion: error reading directory: %w", err)
	}

	for _, file := range files {
		if file.Name() == "global" {
			continue
		}
		configPath := filepath.Join(plugin.FolderPath, file.Name())
		configData, err := database.config.Parse(configPath)
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
			if err := database.config.Write(configData); err != nil {
				return fmt.Errorf("DeleteExclusion: error writing config: %w", err)
			}
			return nil
		}
	}

	// Try global exclusion
	globalPath := filepath.Join(plugin.FolderPath, "global.cfg")
	if configData, err := database.config.Parse(globalPath); err == nil {
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
			if err := database.config.Write(configData); err != nil {
				return fmt.Errorf("DeleteExclusion: error writing global config: %w", err)
			}
			return nil
		}
	}

	return fmt.Errorf("DeleteExclusion: exclusion not found for path: %s", path)
}
