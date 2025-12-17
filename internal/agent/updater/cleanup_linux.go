//go:build linux

package updater

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

func hasSystemd() bool {
	if _, err := os.Stat("/run/systemd/system"); err == nil {
		_, errCtl := exec.LookPath("systemctl")
		return errCtl == nil
	}
	if data, err := os.ReadFile("/proc/1/comm"); err == nil {
		if strings.TrimSpace(string(data)) == "systemd" {
			_, errCtl := exec.LookPath("systemctl")
			return errCtl == nil
		}
	}
	return false
}

func systemdUnitExists(unit string) bool {
	cmd := exec.Command("systemctl", "status", unit)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		cmd2 := exec.Command("systemctl", "list-unit-files", unit)
		out.Reset()
		cmd2.Stdout = &out
		cmd2.Stderr = &out
		if err2 := cmd2.Run(); err2 != nil {
			return false
		}
		return strings.Contains(out.String(), unit)
	}
	return true
}

func removeSystemdUnit(unit string) {
	_ = exec.Command("systemctl", "stop", unit).Run()
	_ = exec.Command("systemctl", "disable", unit).Run()

	paths := []string{
		filepath.Join("/etc/systemd/system", unit),
		filepath.Join("/lib/systemd/system", unit),
		filepath.Join("/usr/lib/systemd/system", unit),
	}
	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			if err := os.Remove(p); err != nil {
				syslog.L.Error(err).WithField("path", p).WithMessage("failed to remove unit file").Write()
			} else {
				syslog.L.Info().WithField("path", p).WithMessage("removed unit file").Write()
			}
		}
	}

	if err := exec.Command("systemctl", "daemon-reload").Run(); err != nil {
		syslog.L.Error(err).WithMessage("failed to daemon-reload").Write()
	}
	_ = exec.Command("systemctl", "reset-failed").Run()
}

func cleanUp() error {
	if hasSystemd() {
		const unit = "pbs-plus-updater.service"
		if systemdUnitExists(unit) {
			syslog.L.Info().
				WithMessage("removing legacy updater systemd unit").
				WithField("unit", unit).
				Write()
			removeSystemdUnit(unit)
		}
	}

	return nil
}
