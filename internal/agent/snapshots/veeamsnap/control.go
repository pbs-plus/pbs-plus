//go:build linux

package veeamsnap

import (
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Control represents the veeamsnap control device
type Control struct {
	fd *os.File
}

// OpenControl opens the veeamsnap control device
func OpenControl() (*Control, error) {
	fd, err := os.OpenFile(VeeamSnapControlDevice, unix.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open control device: %w", err)
	}

	return &Control{fd: fd}, nil
}

// Close closes the control device
func (c *Control) Close() error {
	if c.fd != nil {
		return c.fd.Close()
	}
	return nil
}

// GetVersion retrieves the veeamsnap module version
func (c *Control) GetVersion() (*Version, error) {
	var version Version
	if err := ioctl(c.fd.Fd(), IOCTL_GETVERSION, uintptr(unsafe.Pointer(&version))); err != nil {
		return nil, fmt.Errorf("failed to get version: %w", err)
	}
	return &version, nil
}

// GetCompatibilityFlags retrieves the compatibility flags
func (c *Control) GetCompatibilityFlags() (*CompatibilityFlags, error) {
	var flags CompatibilityFlags
	if err := ioctl(c.fd.Fd(), IOCTL_COMPATIBILITY_FLAGS, uintptr(unsafe.Pointer(&flags))); err != nil {
		return nil, fmt.Errorf("failed to get compatibility flags: %w", err)
	}
	return &flags, nil
}

// PrintState prints the current state (debug)
func (c *Control) PrintState() error {
	return ioctl(c.fd.Fd(), IOCTL_PRINTSTATE, 0)
}
