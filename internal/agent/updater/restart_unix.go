//go:build unix

package updater

import (
	"os"
	"os/exec"
	"syscall"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

func restartCallback(_ Config) {
	exePath, err := os.Executable()
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to get executable path for restart").Write()
		return
	}
	args := append([]string{exePath, "restart"}, os.Args[1:]...)
	cmd := exec.Command(exePath, args[1:]...)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true,
	}
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.Stdin = nil
	if err := cmd.Start(); err != nil {
		syslog.L.Error(err).WithMessage("failed to spawn self restart helper").Write()
	} else {
		syslog.L.Info().WithMessage("spawned self restart helper process").Write()
		_ = cmd.Process.Release()
	}
}
