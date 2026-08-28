package proxmox

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func EnsureNamespacePath(datastorePath, namespace string) error {
	if namespace == "" {
		return nil
	}

	parts := strings.Split(namespace, "/")
	fullPath := datastorePath

	for _, ns := range parts {
		fullPath = filepath.Join(fullPath, "ns", ns)
		if err := os.MkdirAll(fullPath, 0o755); err != nil {
			return fmt.Errorf("create namespace dir %q: %w", fullPath, err)
		}
		if err := ChownBackupUser(fullPath); err != nil {
			return fmt.Errorf("chown namespace dir %q: %w", fullPath, err)
		}
	}

	return nil
}

func NamespacePath(datastorePath, namespace string) string {
	fullPath := datastorePath
	for ns := range strings.SplitSeq(namespace, "/") {
		if ns == "" {
			continue
		}
		fullPath = filepath.Join(fullPath, "ns", ns)
	}
	return fullPath
}

// EnsureGroupPath creates the namespace chain, backup-type dir and optional
// group dir on the datastore, chowning every level to the backup user so the
// stock proxmox-backup-proxy can create snapshot dirs inside them.
func EnsureGroupPath(datastorePath, namespace, backupType, backupID string) error {
	if datastorePath == "" {
		return nil
	}
	if backupID != "" && backupType == "" {
		return fmt.Errorf("backup type required when backup id is set")
	}
	if err := EnsureNamespacePath(datastorePath, namespace); err != nil {
		return err
	}

	cur := NamespacePath(datastorePath, namespace)
	for _, part := range []string{backupType, backupID} {
		if part == "" {
			continue
		}
		cur = filepath.Join(cur, part)
		if err := os.MkdirAll(cur, 0o755); err != nil {
			return fmt.Errorf("create backup dir %q: %w", cur, err)
		}
		if err := ChownBackupUser(cur); err != nil {
			return fmt.Errorf("chown backup dir %q: %w", cur, err)
		}
	}

	return nil
}
