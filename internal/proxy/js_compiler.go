//go:build linux

package proxy

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
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

// computeChecksum computes the SHA-256 checksum of data.
func computeChecksum(data []byte) string {
	hasher := sha256.New()
	hasher.Write(data)
	return hex.EncodeToString(hasher.Sum(nil))
}

// createOriginalBackup creates a backup that preserves the original file.
// This backup is only created once and is never overwritten.
func createOriginalBackup(targetPath string) (string, error) {
	backupPath := filepath.Join(
		backupDir,
		fmt.Sprintf("%s.original", filepath.Base(targetPath)),
	)
	if _, err := os.Stat(backupPath); err == nil {
		// Original backup already exists.
		return backupPath, nil
	}

	content, err := os.ReadFile(targetPath)
	if err != nil {
		return "", fmt.Errorf("failed to read file for original backup: %w", err)
	}

	if err := os.WriteFile(backupPath, content, 0644); err != nil {
		return "", fmt.Errorf("failed to write original backup: %w", err)
	}
	return backupPath, nil
}

// createTimestampBackup creates a backup of the current target file using a
// timestamp to avoid overwriting.
func createTimestampBackup(targetPath string) (string, error) {
	timestamp := time.Now().Format("20060102_150405")
	backupPath := filepath.Join(
		backupDir,
		fmt.Sprintf("%s.%s.backup", filepath.Base(targetPath), timestamp),
	)

	content, err := os.ReadFile(targetPath)
	if err != nil {
		return "", fmt.Errorf("failed to read file for timestamp backup: %w", err)
	}

	if err := os.WriteFile(backupPath, content, 0644); err != nil {
		return "", fmt.Errorf("failed to write timestamp backup: %w", err)
	}
	return backupPath, nil
}

// atomicReplaceFile writes newContent to targetPath in an atomic manner.
// It writes to a temporary file and then renames it.
func atomicReplaceFile(targetPath string, newContent []byte) error {
	dir := filepath.Dir(targetPath)
	tmpFile, err := os.CreateTemp(dir, "tmp")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	tempName := tmpFile.Name()

	if _, err := tmpFile.Write(newContent); err != nil {
		tmpFile.Close()
		return fmt.Errorf("failed to write temporary file: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temporary file: %w", err)
	}

	if err := os.Rename(tempName, targetPath); err != nil {
		return fmt.Errorf("failed to rename temporary file: %w", err)
	}

	return nil
}

// sortedWalk returns the contents of all files in an embedded FS, sorting
// the file paths alphabetically over the entire tree.
func sortedWalk(embedded fs.FS, root string) ([][]byte, error) {
	var filePaths []string
	err := fs.WalkDir(embedded, root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			filePaths = append(filePaths, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(filePaths)

	var results [][]byte
	for _, fp := range filePaths {
		data, err := fs.ReadFile(embedded, fp)
		if err != nil {
			return nil, err
		}
		results = append(results, data)
	}
	return results, nil
}

// compileJS concatenates all JavaScript files found in the embedded FS (alphabetically),
// joining them with newline characters.
func compileJS(embedded *embed.FS) []byte {
	parts, err := sortedWalk(*embedded, ".")
	if err != nil {
		syslog.L.Error(err).Write()
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

// WatchAndReplace watches targetPath for changes, applies modifications (if needed),
// and restores the original backup when the program terminates.
// File events are debounced to avoid processing rapid, successive events.
// If the file is removed or renamed, the watcher is re-added once it reappears.
func WatchAndReplace(
	targetPath string,
	modifyFunc func([]byte) []byte,
) error {
	// Create backup directory if needed.
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	// Create and store the original backup.
	originalBackup, err := createOriginalBackup(targetPath)
	if err != nil {
		return fmt.Errorf("original backup error: %w", err)
	}

	// Set up a signal handler to restore the original backup upon termination.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		syslog.L.Info().WithMessage(
			fmt.Sprintf("Termination signal (%v) received. Restoring backup for %s...",
				sig, targetPath),
		).Write()
		if err := restoreBackup(targetPath, originalBackup); err != nil {
			syslog.L.Error(err).Write()
		}
	}()

	// Read the current file.
	original, err := os.ReadFile(targetPath)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}
	origChecksum := computeChecksum(original)

	// Determine if an initial modification is needed.
	modifiedContent := modifyFunc(original)
	modChecksum := computeChecksum(modifiedContent)
	if origChecksum != modChecksum {
		if err := atomicReplaceFile(targetPath, modifiedContent); err != nil {
			return fmt.Errorf("failed to replace file atomically: %w", err)
		}
		syslog.L.Info().WithMessage(
			fmt.Sprintf("File %s modified initially.", targetPath),
		).Write()
		origChecksum = modChecksum
	} else {
		syslog.L.Info().WithMessage(
			fmt.Sprintf("File %s is already modified; skipping initial modification.",
				targetPath),
		).Write()
	}

	// Create a file watcher.
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("failed to create watcher: %w", err)
	}

	if err := watcher.Add(targetPath); err != nil {
		return fmt.Errorf("failed to add file to watcher: %w", err)
	}
	syslog.L.Info().WithMessage(fmt.Sprintf("Watching file: %s", targetPath)).Write()

	// Debounce setup.
	eventTriggerChan := make(chan struct{}, 1)
	var debounceTimer *time.Timer
	const debounceDuration = 100 * time.Millisecond
	var debounceMu sync.Mutex

	// processChange reads the file, applies the modification, and updates it.
	processChange := func() {
		newData, err := os.ReadFile(targetPath)
		if err != nil {
			syslog.L.Error(err).Write()
			return
		}
		newChecksum := computeChecksum(newData)
		if newChecksum == origChecksum {
			syslog.L.Info().WithMessage(
				fmt.Sprintf("No effective change on %s, skipping.", targetPath),
			).Write()
			return
		}

		// Create a timestamped backup to preserve the current state.
		if _, err := createTimestampBackup(targetPath); err != nil {
			syslog.L.Error(err).Write()
		}

		updatedModified := modifyFunc(newData)
		newModChecksum := computeChecksum(updatedModified)
		if err := atomicReplaceFile(targetPath, updatedModified); err != nil {
			syslog.L.Error(err).Write()
			return
		}
		syslog.L.Info().WithMessage(fmt.Sprintf("File %s updated.", targetPath)).Write()
		origChecksum = newModChecksum
	}

	// Event loop.
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					syslog.L.Info().WithMessage("Watcher events channel closed").Write()
					return
				}

				// Handle deletion or renaming:
				if event.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {
					syslog.L.Info().WithMessage(
						fmt.Sprintf("File %s was removed or renamed. Waiting for re-creation...", targetPath),
					).Write()
					// Poll until the file reappears, then re-add it to the watcher.
					for {
						time.Sleep(100 * time.Millisecond)
						if _, err := os.Stat(targetPath); err == nil {
							if err = watcher.Add(targetPath); err != nil {
								syslog.L.Error(err).Write()
							} else {
								syslog.L.Info().WithMessage(
									fmt.Sprintf("Re-added watcher for file: %s", targetPath),
								).Write()
							}
							break
						}
					}
					continue
				}

				// For Write and Create events, trigger the debounce timer.
				if event.Op&(fsnotify.Write|fsnotify.Create) != 0 {
					debounceMu.Lock()
					if debounceTimer != nil {
						debounceTimer.Stop()
					}
					debounceTimer = time.AfterFunc(debounceDuration, func() {
						select {
						case eventTriggerChan <- struct{}{}:
						default:
						}
					})
					debounceMu.Unlock()
				}

			case err, ok := <-watcher.Errors:
				if !ok {
					syslog.L.Info().WithMessage("Watcher errors channel closed").Write()
					return
				}
				syslog.L.Error(err).Write()

			case <-eventTriggerChan:
				processChange()
			}
		}
	}()

	return nil
}

// restoreBackup restores targetPath from backupPath.
func restoreBackup(targetPath, backupPath string) error {
	content, err := os.ReadFile(backupPath)
	if err != nil {
		return fmt.Errorf("failed to read backup file: %w", err)
	}
	if err := os.WriteFile(targetPath, content, 0644); err != nil {
		return fmt.Errorf("failed to restore file: %w", err)
	}
	syslog.L.Info().WithMessage(
		fmt.Sprintf("Restored original file %s from backup.", targetPath),
	).Write()
	return nil
}
