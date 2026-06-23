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
			if err := os.Remove(p); err != nil {
				syslog.L.Error(err).Write()
			}
			syslog.L.Info().WithField("path", p).WithMessage("removed legacy unit file").Write()
		}
	}

	if foundLegacy {
		if _, err := exec.LookPath("systemctl"); err == nil {
			if err := exec.Command("systemctl", "daemon-reload").Run(); err != nil {
				syslog.L.Error(err).Write()
			}
			if err := exec.Command("systemctl", "reset-failed").Run(); err != nil {
				syslog.L.Error(err).Write()
			}
		}
	}

	return nil
}
