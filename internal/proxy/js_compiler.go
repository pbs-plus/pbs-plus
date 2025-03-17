//go:build linux

package proxy

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"github.com/fsnotify/fsnotify"
)

//go:embed all:views/custom
var customJsFS embed.FS

//go:embed all:views/pre
var preJsFS embed.FS

const backupDir = "/var/lib/pbs-plus/backups"

var jsReplacer = strings.NewReplacer(
	"Proxmox.window.TaskViewer", "PBS.plusWindow.TaskViewer",
	"Proxmox.panel.LogView", "PBS.plusPanel.LogView",
)

// ComputeContentChecksum computes the SHA-256 checksum of data.
func ComputeContentChecksum(data []byte) string {
	hasher := sha256.New()
	hasher.Write(data)
	return hex.EncodeToString(hasher.Sum(nil))
}

// BackupFile creates a backup of targetPath and returns the backup file path.
func BackupFile(targetPath string) (string, error) {
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create backup directory: %w", err)
	}

	backupPath := filepath.Join(backupDir, fmt.Sprintf("%s.backup", filepath.Base(targetPath)))
	content, err := os.ReadFile(targetPath)
	if err != nil {
		return "", fmt.Errorf("failed to read file for backup: %w", err)
	}

	if err := os.WriteFile(backupPath, content, 0644); err != nil {
		return "", fmt.Errorf("failed to write backup: %w", err)
	}
	return backupPath, nil
}

// RestoreBackup restores targetPath from backupPath.
func RestoreBackup(targetPath, backupPath string) error {
	content, err := os.ReadFile(backupPath)
	if err != nil {
		return fmt.Errorf("failed to read backup file: %w", err)
	}
	if err := os.WriteFile(targetPath, content, 0644); err != nil {
		return fmt.Errorf("failed to restore file: %w", err)
	}
	log.Printf("Restored original file %s from backup.", targetPath)
	return nil
}

// ReplaceFile writes newContent directly to targetPath.
func ReplaceFile(targetPath string, newContent []byte) error {
	if err := os.WriteFile(targetPath, newContent, 0644); err != nil {
		return fmt.Errorf("failed to write new content: %w", err)
	}
	return nil
}

// sortedWalk returns the contents of all files in an embedded FS, sorted alphabetically.
func sortedWalk(embedded fs.FS, root string) ([][]byte, error) {
	var results [][]byte
	var queue []string
	queue = append(queue, root)

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		entries, err := fs.ReadDir(embedded, cur)
		if err != nil {
			return nil, err
		}
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].Name() < entries[j].Name()
		})
		for _, entry := range entries {
			path := filepath.Join(cur, entry.Name())
			if entry.IsDir() {
				queue = append(queue, path)
			} else {
				data, err := fs.ReadFile(embedded, path)
				if err != nil {
					return nil, err
				}
				results = append(results, data)
			}
		}
	}
	return results, nil
}

// compileJS concatenates all JavaScript files found in the embedded FS (alphabetically),
// / joining them with newline characters.
func compileJS(embedded *embed.FS) []byte {
	parts, err := sortedWalk(*embedded, ".")
	if err != nil {
		log.Printf("failed to walk embedded FS: %v", err)
		return nil
	}
	return bytes.Join(parts, []byte("\n"))
}

func ModifyJS(original []byte) []byte {
	replaced := []byte(jsReplacer.Replace(string(original)))
	preJS := compileJS(&preJsFS)
	compiledJS := compileJS(&customJsFS)
	return bytes.Join([][]byte{preJS, replaced, compiledJS}, []byte("\n"))
}

func ModifyLib(original []byte) []byte {
	oldString := `if (!newopts.url.match(/^\/api2/))`
	newString := `if (!newopts.url.match(/^\/api2/) && !newopts.url.match(/^[a-z][a-z\d+\-.]*:/i))`
	newContent := strings.Replace(string(original), oldString, newString, 1)
	return []byte(newContent)
}

// WatchAndReplace watches targetPath for changes, applies modifications (if needed), and restores
// the backup if the program terminates. The file watcher’s event loop is enclosed in a goroutine.
func WatchAndReplace(targetPath string, modifyFunc func([]byte) []byte) error {
	// Create an initial backup.
	backupPath, err := BackupFile(targetPath)
	if err != nil {
		return fmt.Errorf("backup error: %w", err)
	}

	// Set up a signal handler to restore the backup upon termination.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("Termination signal (%v) received. Restoring backup for %s...", sig, targetPath)
		if err := RestoreBackup(targetPath, backupPath); err != nil {
			log.Printf("Error restoring backup: %v", err)
		}
		os.Exit(0)
	}()

	// Read the current file.
	original, err := os.ReadFile(targetPath)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	// Determine if an initial modification is needed.
	origChecksum := ComputeContentChecksum(original)
	modifiedContent := modifyFunc(original)
	modChecksum := ComputeContentChecksum(modifiedContent)

	if origChecksum == modChecksum {
		log.Printf("File %s is already modified; skipping initial modification.", targetPath)
	} else {
		if err := ReplaceFile(targetPath, modifiedContent); err != nil {
			return fmt.Errorf("failed to replace file: %w", err)
		}
		log.Printf("File %s modified initially.", targetPath)
		origChecksum = modChecksum
	}

	// Create a file watcher.
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("failed to create watcher: %w", err)
	}

	if err := watcher.Add(targetPath); err != nil {
		return fmt.Errorf("failed to add file to watcher: %w", err)
	}
	log.Printf("Watching file: %s", targetPath)

	// Enclose the watcher event loop in a goroutine.
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					log.Printf("Watcher events channel closed")
					return
				}
				if event.Op&(fsnotify.Write|fsnotify.Create) != 0 {
					log.Printf("Change detected on %s", targetPath)
					newData, err := os.ReadFile(targetPath)
					if err != nil {
						log.Printf("Error reading file: %v", err)
						continue
					}
					newChecksum := ComputeContentChecksum(newData)
					if newChecksum == origChecksum {
						log.Printf("No effective change on %s, skipping.", targetPath)
						continue
					}

					// Update backup.
					if _, err := BackupFile(targetPath); err != nil {
						log.Printf("Error backing up file: %v", err)
					}

					// Apply modification.
					updatedModified := modifyFunc(newData)
					newModChecksum := ComputeContentChecksum(updatedModified)
					if err := ReplaceFile(targetPath, updatedModified); err != nil {
						log.Printf("Error replacing file: %v", err)
						continue
					}
					log.Printf("File %s updated.", targetPath)
					origChecksum = newModChecksum
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					log.Printf("Watcher errors channel closed")
					return
				}
				log.Printf("Watcher error: %v", err)
			}
		}
	}()

	return nil
}
