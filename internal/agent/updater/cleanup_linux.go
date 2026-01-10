//go:build linux

package updater

import (
	"os"
	"os/exec"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
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
			_ = os.Remove(p)
			syslog.L.Info().WithField("path", p).WithMessage("removed legacy unit file").Write()
		}
	}

	if foundLegacy {
		if _, err := exec.LookPath("systemctl"); err == nil {
			_ = exec.Command("systemctl", "daemon-reload").Run()
			_ = exec.Command("systemctl", "reset-failed").Run()
		}
	}

	return nil
}
