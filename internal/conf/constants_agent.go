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

// initPaths selects paths based on what exists, preferring new paths when
// migration has occurred and falling back to legacy paths for continuity.
func initPaths() {
	// 1. Legacy exists, new doesn't: keep legacy (migration pending)
	// 2. New exists: use new (migration done or fresh install)
	// 3. Neither exists: use new (fresh install)
	// 4. Legacy exists, new creation failed: keep legacy (fallback)
	useLegacyPaths := false

	legacyStateExists := dirExists(legacyStatePrefix)
	newStateExists := dirExists(StatePrefix)

	useLegacyPaths := false

	if legacyStateExists {
		if newStateExists {
			fmt.Printf("[pbs-plus] using new agent paths: state=%s\n", StatePrefix)
		} else {
			useLegacyPaths = true
			fmt.Printf("[pbs-plus] fallback to legacy paths - migration may be pending: legacy=%s new=%s\n",
				legacyStatePrefix, StatePrefix)
		}
	}

	if useLegacyPaths {
		StatePrefix = legacyStatePrefix
		ScriptsBasePath = legacyStatePrefix + "/scripts"
		SecretsKeyPath = legacyStatePrefix + "/.secret.key"
		BackupLogsBasePath = legacyLogsPrefix
		RestoreLogsBasePath = legacyLogsPrefix + "/restores"
	} else {
		ScriptsBasePath = StatePrefix + "/scripts"
		SecretsKeyPath = StatePrefix + "/.secret.key"
		BackupLogsBasePath = "/var/log/pbs-plus-agent"
		RestoreLogsBasePath = "/var/log/pbs-plus-agent/restores"
	}
}

// dirExists returns true if path exists and is a directory.
func dirExists(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.IsDir()
}
