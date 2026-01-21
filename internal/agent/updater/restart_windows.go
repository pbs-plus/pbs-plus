//go:build windows

package updater

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

func restartCallback(_ Config) bool {
	exePath, err := os.Executable()
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to get executable path for restart").Write()
		return false
	}

	dir := filepath.Dir(exePath)
	filename := filepath.Base(exePath)

	if strings.HasPrefix(filename, ".") && strings.HasSuffix(filename, ".old") {
		newBase := strings.TrimSuffix(filename, ".old")
		newBase = strings.TrimPrefix(newBase, ".")

		exePath = filepath.Join(dir, newBase)
	}

	cmdArgs := append([]string{"restart"}, os.Args[1:]...)
	cmd := exec.Command(exePath, cmdArgs...)

	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP | 0x00000008,
	}

	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.Stdin = nil

	if err := cmd.Start(); err != nil {
		syslog.L.Error(err).
			WithMessage("failed to spawn self restart helper").
			WithField("path", exePath).
			Write()
	} else {
		syslog.L.Info().
			WithMessage("spawned self restart helper process").
			WithField("path", exePath).
			Write()
		_ = cmd.Process.Release()
	}

	return false
}
