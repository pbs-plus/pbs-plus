//go:build windows

package updater

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/pbs-plus/pbs-plus/internal/log"
)

func restartCallback(_ Config) bool {
	exePath, err := os.Executable()
	if err != nil {
		log.Error(err, "failed to get executable path for restart")
		return false
	}

	dir := filepath.Dir(exePath)
	filename := filepath.Base(exePath)

	if strings.HasPrefix(filename, ".") && strings.HasSuffix(filename, ".old") {
		newBase := strings.TrimSuffix(filename, ".old")
		newBase = strings.TrimPrefix(newBase, ".")

		exePath = filepath.Join(dir, newBase)
	}

	cmd := exec.Command(exePath, "restart")

	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP | 0x00000008,
	}

	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.Stdin = nil

	if err := cmd.Start(); err != nil {
		log.Error(err,
			"failed to spawn self restart helper",
			"path", exePath)

	} else {
		log.Info("spawned self restart helper process",
			"path", exePath)

		_ = cmd.Process.Release()
	}

	return false
}
