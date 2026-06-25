//go:build linux

package updater

import (
	"os"
	"os/exec"

	"github.com/pbs-plus/pbs-plus/internal/log"
)

func cleanUp() error {
	legacyPaths := []string{
		"/etc/systemd/system/pbs-plus-updater.service",
		"/lib/systemd/system/pbs-plus-updater.service",
	}

	foundLegacy := false
	for _, p := range legacyPaths {
		if _, err := os.Stat(p); err == nil {
			foundLegacy = true
			if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
				log.Error(err, "")
			}
			log.Info("removed legacy unit file", "path", p)
		}
	}

	if foundLegacy {
		if _, err := exec.LookPath("systemctl"); err == nil {
			if err := exec.Command("systemctl", "daemon-reload").Run(); err != nil {
				log.Error(err, "")
			}
			if err := exec.Command("systemctl", "reset-failed").Run(); err != nil {
				log.Error(err, "")
			}
		}
	}

	return nil
}
