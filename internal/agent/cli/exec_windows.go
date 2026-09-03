//go:build windows

package cli

import "os/exec"

func setProcAttributes(cmd *exec.Cmd) {
}

func setSnapshotProcAttributes(cmd *exec.Cmd) bool {
	return false
}
