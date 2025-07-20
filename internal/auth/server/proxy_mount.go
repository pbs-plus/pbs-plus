//go:build linux

package server

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"github.com/pbs-plus/pbs-plus/internal/utils"
)

const proxyCert = "/etc/proxmox-backup/proxy.pem"
const proxyKey = "/etc/proxmox-backup/proxy.key"

func (c *Config) Mount() error {

	// Check if something is already mounted at the target path
	if utils.IsMounted(proxyCert) {
		if err := syscall.Unmount(proxyCert, 0); err != nil {
			return fmt.Errorf("failed to unmount existing file: %w", err)
		}
	}
	if utils.IsMounted(proxyKey) {
		if err := syscall.Unmount(proxyKey, 0); err != nil {
			return fmt.Errorf("failed to unmount existing file: %w", err)
		}
	}

	// Create backup directory if it doesn't exist
	backupDir := filepath.Join(os.TempDir(), "pbs-plus-backups")
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	// Create backup filename with timestamp
	backupPath := filepath.Join(backupDir, fmt.Sprintf("%s.backup", filepath.Base(proxyCert)))
	backupKeyPath := filepath.Join(backupDir, fmt.Sprintf("%s.backup", filepath.Base(proxyKey)))

	// Read existing file
	original, err := os.ReadFile(proxyCert)
	if err != nil {
		return fmt.Errorf("failed to read original file: %w", err)
	}
	originalKey, err := os.ReadFile(proxyKey)
	if err != nil {
		return fmt.Errorf("failed to read original file: %w", err)
	}

	// Create backup
	if err := os.WriteFile(backupPath, original, 0644); err != nil {
		return fmt.Errorf("failed to create backup: %w", err)
	}
	if err := os.WriteFile(backupKeyPath, originalKey, 0644); err != nil {
		return fmt.Errorf("failed to create backup: %w", err)
	}

	// Perform bind mount
	if err := syscall.Mount(c.CertFile, proxyCert, "", syscall.MS_BIND, ""); err != nil {
		return fmt.Errorf("failed to mount file: %w", err)
	}
	if err := syscall.Mount(c.KeyFile, proxyKey, "", syscall.MS_BIND, ""); err != nil {
		return fmt.Errorf("failed to mount file: %w", err)
	}

	return nil
}

func (c *Config) Unmount() error {
	// Unmount the file
	if err := syscall.Unmount(proxyCert, 0); err != nil {
	}
	if err := syscall.Unmount(proxyKey, 0); err != nil {
	}

	umount := exec.Command("umount", "-lf", proxyCert)
	umount.Env = os.Environ()
	if err := umount.Run(); err != nil {
	}

	umount = exec.Command("umount", "-lf", proxyKey)
	umount.Env = os.Environ()
	if err := umount.Run(); err != nil {
	}

	return nil
}
