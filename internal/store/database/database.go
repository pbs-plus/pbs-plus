package database

import (
	"fmt"
	"os"
	"sync"

	"github.com/sonroyaalmerol/pbs-plus/internal/auth/token"
	configLib "github.com/sonroyaalmerol/pbs-plus/internal/config"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/constants"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

var defaultPaths = map[string]string{
	"init":         "/etc/proxmox-backup/pbs-plus/.init",
	"jobs":         "/etc/proxmox-backup/pbs-plus/jobs.d",
	"targets":      "/etc/proxmox-backup/pbs-plus/targets.d",
	"exclusions":   "/etc/proxmox-backup/pbs-plus/exclusions.d",
	"partialfiles": "/etc/proxmox-backup/pbs-plus/partialfiles.d",
	"tokens":       "/etc/proxmox-backup/pbs-plus/tokens.d",
}

type Database struct {
	mu           sync.RWMutex
	config       *configLib.SectionConfig
	TokenManager *token.Manager
	paths        map[string]string
}

func Initialize(paths map[string]string) (*Database, error) {
	if paths == nil {
		paths = defaultPaths
	}

	alreadyInitialized := false

	if _, err := os.Stat(paths["init"]); !os.IsNotExist(err) {
		alreadyInitialized = true
	}

	for key, path := range paths {
		if key == "cert" || key == "key" {
			continue
		}

		if err := os.MkdirAll(path, 0750); err != nil {
			return nil, fmt.Errorf("Initialize: error creating directory %s: %w", path, err)
		}
	}

	minLength := 3
	idSchema := &configLib.Schema{
		Type:        configLib.TypeString,
		Description: "Section identifier",
		Required:    true,
		MinLength:   &minLength,
	}

	config := configLib.NewSectionConfig(idSchema)

	database := &Database{
		config: config,
		paths:  paths,
	}

	database.RegisterExclusionPlugin()
	database.RegisterJobPlugin()
	database.RegisterPartialFilePlugin()
	database.RegisterTargetPlugin()
	database.RegisterTokenPlugin()

	if !alreadyInitialized {
		for _, exclusion := range constants.DefaultExclusions {
			err := database.CreateExclusion(types.Exclusion{
				Path:    exclusion,
				Comment: "Generated exclusion from default list",
			})
			if err != nil {
				syslog.L.Errorf("Initialize: error creating default exclusion: %v", err)
			}
		}
	}

	return database, nil
}
