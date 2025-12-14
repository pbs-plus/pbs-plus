//go:build linux

package updater

import (
	"bytes"
	"fmt"
	"os/exec"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

func restartCallback(cfg Config) bool {
	err := cleanUp()
	if err != nil {
		syslog.L.Error(err).WithMessage("update cleanup error, non-fatal").Write()
	}

	if !hasSystemd() {
		syslog.L.Error(fmt.Errorf("not a systemd-based system")).WithMessage("manual service restart required for update").Write()
		return false
	}

	if cfg.SystemdUnit != "" {
		syslog.L.Info().WithMessage(fmt.Sprintf("restarting systemd unit %q", cfg.SystemdUnit)).Write()
		cmd := exec.Command("systemctl", "restart", cfg.SystemdUnit)

		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out

		if err := cmd.Run(); err != nil {
			syslog.L.Error(err).WithMessage(fmt.Sprintf("failed to restart systemd unit %q: %s", cfg.SystemdUnit, out.String())).Write()
			return false
		} else {
			syslog.L.Info().WithMessage(fmt.Sprintf("successfully restarted systemd unit %q", cfg.SystemdUnit)).Write()
		}
	} else {
		syslog.L.Info().WithMessage("Restart requested, but no systemd unit configured. Exiting for external restart.").Write()
	}

	return false
}
