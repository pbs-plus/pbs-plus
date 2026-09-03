//go:build linux

package cli

import (
	"os"
	"os/exec"
	"syscall"
)

func setProcAttributes(cmd *exec.Cmd) {
	attr := &syscall.SysProcAttr{Setpgid: true}
	if os.Geteuid() == 0 {
		attr.Unshareflags = syscall.CLONE_NEWNS
	}
	cmd.SysProcAttr = attr
}
