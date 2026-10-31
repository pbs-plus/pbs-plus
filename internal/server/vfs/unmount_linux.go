//go:build linux

package vfs

import (
	"fmt"
	"os"
	"os/exec"
)

// ForceUnmount lazily detaches a mountpoint whose FUSE server is already gone: a plain umount on one hangs forever and wedges every later statfs on the host.
func ForceUnmount(mountpoint string) error {
	if mountpoint == "" {
		return nil
	}
	var lastErr error
	for _, args := range [][]string{
		{"fusermount3", "-uz", mountpoint},
		{"fusermount", "-uz", mountpoint},
		{"umount", "-lf", mountpoint},
	} {
		cmd := exec.Command(args[0], args[1:]...)
		cmd.Env = os.Environ()
		if err := cmd.Run(); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}
	return fmt.Errorf("failed to lazily detach %s: %w", mountpoint, lastErr)
}
