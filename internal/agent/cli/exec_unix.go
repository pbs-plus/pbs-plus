//go:build unix && !linux

package cli

import (
	"os/exec"
	"syscall"
)

func setProcAttributes(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}
}

func setSnapshotProcAttributes(cmd *exec.Cmd) bool {
	setProcAttributes(cmd)
	return false
}
