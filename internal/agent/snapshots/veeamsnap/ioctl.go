//go:build linux

package veeamsnap

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func ioctl(fd uintptr, request uintptr, arg uintptr) error {
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, fd, request, arg)
	if errno != 0 {
		return fmt.Errorf("ioctl failed: %v", errno)
	}
	return nil
}
