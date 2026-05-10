//go:build agent && unix

package conf

import (
	"fmt"
	"os"
	"sync"
)

const legacyStatePrefix = "/var/lib/pbs-plus"
const legacyLogsPrefix = "/var/log/pbs-plus"

var pathInitOnce sync.Once

func init() {
	pathInitOnce.Do(initPaths)
}

// initPaths selects the appropriate paths based on what actually exists and is writable.
// Priority: legacy path (if exists) > new path (if exists/writable) > legacy path (fallback)
// This ensures the agent keeps working during updates and migrations.
func initPaths() {
	// Check if legacy paths exist (do NOT create them)
	legacyStateExists := dirExists(legacyStatePrefix)
	// Check if new paths exist (do NOT create them)
	newStateExists := dirExists(StatePrefix)

	// Decision logic:
	// 1. If legacy exists and new doesn't: keep using legacy (no migration happened yet)
	// 2. If new exists: use new (migration happened or fresh install)
	// 3. If neither exists: use new (fresh install, will be created on demand)
	// 4. If legacy exists but new creation failed: keep using legacy (fallback)

	useLegacyPaths := false

	if legacyStateExists {
		if newStateExists {
			// Migration completed or new path was manually created, use new paths
			fmt.Printf("[pbs-plus] using new agent paths: state=%s\n", StatePrefix)
		} else {
			// Legacy exists but new doesn't - migration hasn't happened or failed
			// Keep using legacy paths to ensure continuity
			useLegacyPaths = true
			fmt.Printf("[pbs-plus] fallback to legacy paths - migration may be pending: legacy=%s new=%s\n",
				legacyStatePrefix, StatePrefix)
		}
	}

	if useLegacyPaths {
		// Override with legacy paths
		StatePrefix = legacyStatePrefix
		ScriptsBasePath = legacyStatePrefix + "/scripts"
		SecretsKeyPath = legacyStatePrefix + "/.secret.key"
		BackupLogsBasePath = legacyLogsPrefix
		RestoreLogsBasePath = legacyLogsPrefix + "/restores"
	} else {
		// Use new agent-specific paths (already set in constants.go, just ensure they're consistent)
		ScriptsBasePath = StatePrefix + "/scripts"
		SecretsKeyPath = StatePrefix + "/.secret.key"
		BackupLogsBasePath = "/var/log/pbs-plus-agent"
		RestoreLogsBasePath = "/var/log/pbs-plus-agent/restores"
	}
}

// dirExists returns true if the path exists and is a directory.
// It does NOT attempt to create the directory.
func dirExists(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.IsDir()
}
