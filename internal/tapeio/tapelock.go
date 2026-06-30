package tapeio

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

const (
	driveLockDir = "/var/run/proxmox-backup/drive-lock"

	defaultDriveLockTimeout = 10 * time.Second
)

type TapeDeviceLock struct {
	f *os.File
}

func escapeUnitPath(devicePath string) string {
	unit := devicePath
	for len(unit) > 0 && unit[0] == '/' {
		unit = unit[1:]
	}
	if unit == "" {
		return "-"
	}
	var b []byte
	for i := 0; i < len(unit); i++ {
		c := unit[i]
		if c == '/' {
			b = append(b, '-')
			continue
		}
		if (i == 0 && c == '.') ||
			!(c == '_' || c == '.' ||
				(c >= '0' && c <= '9') ||
				(c >= 'A' && c <= 'Z') ||
				(c >= 'a' && c <= 'z')) {
			b = append(b, '\\', 'x')
			const hex = "0123456789abcdef"
			b = append(b, hex[c>>4], hex[c&0xf])
			continue
		}
		b = append(b, c)
	}
	return string(b)
}

func lockFilePath(devicePath string) string {
	return filepath.Join(driveLockDir, escapeUnitPath(devicePath))
}

func LockTapeDevice(devicePath string) (*TapeDeviceLock, error) {
	return LockTapeDeviceTimeout(devicePath, defaultDriveLockTimeout)
}

func LockTapeDeviceTimeout(devicePath string, timeout time.Duration) (*TapeDeviceLock, error) {
	if devicePath == "" {
		return nil, errors.New("tapeio: device path required for drive lock")
	}
	path := lockFilePath(devicePath)
	if err := os.MkdirAll(driveLockDir, 0o770); err != nil {
		return nil, fmt.Errorf("tapeio: create drive lock dir %s: %w", driveLockDir, err)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o660)
	if err != nil {
		return nil, fmt.Errorf("tapeio: open drive lock %s: %w", path, err)
	}

	if err := flockWait(f, unix.LOCK_EX, timeout); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("tapeio: drive %s is locked by another operation: %w", devicePath, err)
	}
	return &TapeDeviceLock{f: f}, nil
}

func (l *TapeDeviceLock) Close() error {
	if l == nil || l.f == nil {
		return nil
	}
	return l.f.Close()
}

func (l *TapeDeviceLock) Path() string {
	if l == nil || l.f == nil {
		return ""
	}
	return l.f.Name()
}

func flockWait(f *os.File, how int, timeout time.Duration) error {
	if timeout <= 0 {
		return flockNB(f, how)
	}
	deadline := time.Now().Add(timeout)
	for {
		if err := flockNB(f, how); err == nil {
			return nil
		} else if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("drive lock timed out after %s", timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func flockNB(f *os.File, how int) error {
	_, _, errno := unix.Syscall(unix.SYS_FLOCK, f.Fd(), uintptr(how|unix.LOCK_NB), 0)
	if errno != 0 {
		return errno
	}
	return nil
}
