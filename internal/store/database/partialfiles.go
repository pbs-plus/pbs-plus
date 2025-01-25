package database

import (
	"fmt"
	"os"
	"path/filepath"

	configLib "github.com/sonroyaalmerol/pbs-plus/internal/config"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/pattern"
)

func (database *Database) RegisterPartialFilePlugin() {
	plugin := &configLib.SectionPlugin{
		FolderPath: database.paths["partialfiles"],
		TypeName:   "partialfile",
		Properties: map[string]*configLib.Schema{
			"path": {
				Type:        configLib.TypeString,
				Description: "File path",
				Required:    true,
			},
			"comment": {
				Type:        configLib.TypeString,
				Description: "File comment",
				Required:    false,
			},
		},
	}

	database.config.RegisterPlugin(plugin)
}

func (database *Database) CreatePartialFile(partialFile types.PartialFile) error {
	database.mu.Lock()
	defer database.mu.Unlock()

	if !pattern.IsValidPattern(partialFile.Path) {
		return fmt.Errorf("CreatePartialFile: invalid path pattern -> %s", partialFile.Path)
	}

	configData := &configLib.ConfigData{
		Sections: map[string]*configLib.Section{
			partialFile.Path: {
				Type: "partialfile",
				ID:   partialFile.Path,
				Properties: map[string]string{
					"path":    partialFile.Path,
					"comment": partialFile.Comment,
				},
			},
		},
		Order: []string{partialFile.Path},
	}

	if err := database.config.Write(configData); err != nil {
		return fmt.Errorf("CreatePartialFile: error writing config: %w", err)
	}

	return nil
}

func (database *Database) GetPartialFile(path string) (*types.PartialFile, error) {
	database.mu.RLock()
	defer database.mu.RUnlock()

	plugin := database.config.GetPlugin("partialfile")
	configPath := filepath.Join(plugin.FolderPath, utils.EncodePath(path)+".cfg")
	configData, err := database.config.Parse(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("GetPartialFile: error reading config: %w", err)
	}

	section, exists := configData.Sections[path]
	if !exists {
		return nil, nil
	}

	return &types.PartialFile{
		Path:    section.Properties["path"],
		Comment: section.Properties["comment"],
	}, nil
}

func (database *Database) UpdatePartialFile(partialFile types.PartialFile) error {
	return database.CreatePartialFile(partialFile)
}

func (database *Database) DeletePartialFile(path string) error {
	database.mu.Lock()
	defer database.mu.Unlock()

	plugin := database.config.GetPlugin("partialfile")
	configPath := filepath.Join(plugin.FolderPath, utils.EncodePath(path)+".cfg")
	if err := os.Remove(configPath); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("DeletePartialFile: error deleting file: %w", err)
		}
	}

	return nil
}

func (database *Database) GetAllPartialFiles() ([]types.PartialFile, error) {
	database.mu.RLock()
	defer database.mu.RUnlock()

	plugin := database.config.GetPlugin("partialfile")
	files, err := os.ReadDir(plugin.FolderPath)
	if err != nil {
		return nil, fmt.Errorf("GetAllPartialFiles: error reading directory: %w", err)
	}

	var partialFiles []types.PartialFile
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		configPath := filepath.Join(plugin.FolderPath, file.Name())
		configData, err := database.config.Parse(configPath)
		if err != nil {
			syslog.L.Errorf("GetAllPartialFiles: error getting partial file: %v", err)
			continue
		}

		for _, section := range configData.Sections {
			partialFiles = append(partialFiles, types.PartialFile{
				Path:    section.Properties["path"],
				Comment: section.Properties["comment"],
			})
		}
	}

	return partialFiles, nil
}
