//go:build unix

package updater

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

func restartCallback(c Config) bool {
	if c.Service != nil {
		c.Service.Restart()
		return false
	}

	exePath, err := os.Executable()
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to get executable path for restart").Write()
		return true
	}

	dir := filepath.Dir(exePath)
	filename := filepath.Base(exePath)

	if strings.HasPrefix(filename, ".") && strings.HasSuffix(filename, ".old") {
		newBase := strings.TrimSuffix(filename, ".old")
		newBase = strings.TrimPrefix(newBase, ".")

		exePath = filepath.Join(dir, newBase)
	}

	cmd := exec.Command(exePath, os.Args[1:]...)

	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true,
	}

	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.Stdin = nil

	if err := cmd.Start(); err != nil {
		syslog.L.Error(err).
			WithMessage("failed to spawn self restart helper").
			WithField("target", exePath).
			Write()
	} else {
		syslog.L.Info().
			WithMessage("spawned self restart helper process").
			WithField("target", exePath).
			Write()
		_ = cmd.Process.Release()
	}

	return false
}
