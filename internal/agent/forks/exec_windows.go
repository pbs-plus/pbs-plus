//go:build windows
// +build windows

package forks

import "os/exec"

func setProcAttributes(cmd *exec.Cmd) {
	// Windows doesn't need special process group handling
	// The process will be created normally
}
