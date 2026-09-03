//go:build linux

package cli

import (
	"os"
	"os/exec"
	"syscall"
)

func setProcAttributes(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// setSnapshotProcAttributes gives the backup child a private mount namespace so
// its snapshot mounts stay invisible to the host and disappear when it exits.
func setSnapshotProcAttributes(cmd *exec.Cmd) bool {
	attr := &syscall.SysProcAttr{Setpgid: true}
	if os.Geteuid() != 0 {
		cmd.SysProcAttr = attr
		return false
	}
	attr.Unshareflags = syscall.CLONE_NEWNS
	cmd.SysProcAttr = attr
	return true
}
