//go:build linux

package database

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/sonroyaalmerol/pbs-plus/internal/auth/token"
	configLib "github.com/sonroyaalmerol/pbs-plus/internal/config"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/constants"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

var defaultPaths = map[string]string{
	"init":       "/etc/proxmox-backup/pbs-plus/.init",
	"jobs":       "/etc/proxmox-backup/pbs-plus/jobs.d",
	"targets":    "/etc/proxmox-backup/pbs-plus/targets.d",
	"exclusions": "/etc/proxmox-backup/pbs-plus/exclusions.d",
	"tokens":     "/etc/proxmox-backup/pbs-plus/tokens.d",
}

type Database struct {
	jobsConfig       *configLib.SectionConfig[types.Job]
	targetsConfig    *configLib.SectionConfig[types.Target]
	exclusionsConfig *configLib.SectionConfig[types.Exclusion]
	tokensConfig     *configLib.SectionConfig[types.AgentToken]
	TokenManager     *token.Manager
	paths            map[string]string
}

func Initialize(paths map[string]string) (*Database, error) {
	if paths == nil {
		paths = defaultPaths
	}

	dirEntries, err := os.ReadDir(filepath.Dir(paths["init"]))
	if err != nil {
		return nil, err
	}

	hasLegacy := false

	for _, dirEntry := range dirEntries {
		switch dirEntry.Name() {
		case "jobs.d":
			fallthrough
		case "targets.d":
			fallthrough
		case "exclusions.d":
			fallthrough
		case "tokens.d":
			hasLegacy = true
			break
		}
	}

	if !hasLegacy {
		return nil, nil
	}

	// Check if paths map contains required keys
	requiredPaths := []string{"init", "jobs", "targets", "exclusions", "tokens"}
	for _, key := range requiredPaths {
		if _, exists := paths[key]; !exists {
			return nil, fmt.Errorf("Initialize: missing required path key: %s", key)
		}
	}

	alreadyInitialized := false
	if _, err := os.Stat(paths["init"]); err == nil {
		alreadyInitialized = true
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("Initialize: error checking init file: %w", err)
	}

	for key, path := range paths {
		if key == "cert" || key == "key" {
			continue
		}
		if path == "" {
			return nil, fmt.Errorf("Initialize: empty path for key: %s", key)
		}
		if err := os.MkdirAll(path, 0750); err != nil {
			return nil, fmt.Errorf("Initialize: error creating directory %s: %w", path, err)
		}
	}

	database := &Database{
		paths: paths,
	}

	database.RegisterExclusionPlugin()
	database.RegisterJobPlugin()
	database.RegisterTargetPlugin()
	database.RegisterTokenPlugin()

	if !alreadyInitialized {
		for _, exclusion := range constants.DefaultExclusions {
			if err := database.CreateExclusion(types.Exclusion{
				Path:    exclusion,
				Comment: "Generated exclusion from default list",
			}); err != nil {
				syslog.L.Error(err).WithField("path", exclusion).Write()
			}
		}

		// Create init file to mark initialization
		if err := os.WriteFile(paths["init"], []byte("initialized"), 0640); err != nil {
			return nil, fmt.Errorf("Initialize: error creating init file: %w", err)
		}
	}

	return database, nil
}
