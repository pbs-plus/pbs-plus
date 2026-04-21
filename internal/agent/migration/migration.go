//go:build agent && unix

package migration

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// OldStatePath is the legacy path used before v0.79.0.
// This is a variable instead of constant to allow for unit testing.
var OldStatePath = "/var/lib/pbs-plus"

// TryMigrate attempts to migrate data from legacy paths to new paths.
// This is a best-effort operation - if it fails (e.g., due to permissions),
// the agent continues to use the legacy paths automatically.
// Returns true if migration was successful, false otherwise.
func TryMigrate() bool {
	// Check if this is even needed
	oldInfo, err := os.Stat(OldStatePath)
	if err != nil {
		// Old path doesn't exist - nothing to migrate
		if os.IsNotExist(err) {
			return true // Consider this success - nothing to migrate
		}
		fmt.Printf("[pbs-plus] migration check failed: %v\n", err)
		return false
	}

	if !oldInfo.IsDir() {
		// Old path exists but is not a directory - skip
		return true
	}

	// Check if new directory already exists and is populated
	newPath := "/var/lib/pbs-plus-agent"
	newInfo, err := os.Stat(newPath)
	if err == nil && newInfo.IsDir() {
		entries, err := os.ReadDir(newPath)
		if err == nil && len(entries) > 0 {
			// New directory already has content, migration complete or manual setup
			fmt.Printf("[pbs-plus] migration: new path already exists with content, skipping\n")
			return true
		}
	}

	// Try to create the new state directory
	if err := os.MkdirAll(newPath, 0700); err != nil {
		fmt.Printf("[pbs-plus] migration: cannot create new directory %s (will use legacy): %v\n",
			newPath, err)
		return false
	}

	// Test write permission in new directory
	testFile := filepath.Join(newPath, ".migration_test")
	if f, err := os.Create(testFile); err != nil {
		fmt.Printf("[pbs-plus] migration: cannot write to new directory %s (will use legacy): %v\n",
			newPath, err)
		return false
	} else {
		_ = f.Close()
		_ = os.Remove(testFile)
	}

	// Check if we can create backup of old path
	backupPath := OldStatePath + ".backup.legacy"
	if _, err := os.Stat(backupPath); err == nil {
		fmt.Printf("[pbs-plus] migration: backup already exists, migration may have been partially completed\n")
		return true
	}

	fmt.Printf("[pbs-plus] migration: migrating data from %s to %s\n", OldStatePath, newPath)

	// Perform the migration
	if err := migrateDirectory(OldStatePath, newPath); err != nil {
		fmt.Printf("[pbs-plus] migration: failed during migration (will use legacy): %v\n", err)
		return false
	}

	// Migration successful - try to rename old to backup
	if err := os.Rename(OldStatePath, backupPath); err != nil {
		fmt.Printf("[pbs-plus] migration: data migrated but failed to backup old directory: %v\n", err)
		// Don't return false - data is migrated, just backup failed
	} else {
		fmt.Printf("[pbs-plus] migration: complete, backed up old directory to %s\n", backupPath)
	}

	return true
}

func migrateDirectory(src, dst string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		info, err := entry.Info()
		if err != nil {
			fmt.Printf("[pbs-plus] migration warning: skipping file %s: %v\n", srcPath, err)
			continue
		}

		if entry.IsDir() {
			if err := os.MkdirAll(dstPath, info.Mode()); err != nil {
				return fmt.Errorf("failed to create directory %s: %w", dstPath, err)
			}
			if err := migrateDirectory(srcPath, dstPath); err != nil {
				return err
			}
		} else {
			if err := copyFile(srcPath, dstPath); err != nil {
				return fmt.Errorf("failed to copy file %s to %s: %w", srcPath, dstPath, err)
			}
		}

		// Preserve permissions
		if err := os.Chmod(dstPath, info.Mode()); err != nil {
			fmt.Printf("[pbs-plus] migration warning: failed to set permissions on %s: %v\n", dstPath, err)
		}
	}

	return nil
}

func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = sourceFile.Close() }()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() { _ = destFile.Close() }()

	if _, err := io.Copy(destFile, sourceFile); err != nil {
		return err
	}

	return destFile.Sync()
}

// LegacyPathsAvailable returns true if the legacy paths still exist
// This can be used by the RPM postinst script to check if migration is needed
func LegacyPathsAvailable() bool {
	info, err := os.Stat(OldStatePath)
	if err != nil {
		return false
	}
	return info.IsDir()
}
