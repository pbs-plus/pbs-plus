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

// computeChecksum computes the SHA256 checksum of data.
func computeChecksum(data []byte) string {
	hasher := sha256.New()
	hasher.Write(data)
	return hex.EncodeToString(hasher.Sum(nil))
}

// createOriginalBackup creates a backup that preserves the original file,
// stored under a ".original" suffix. This file is created only once.
func createOriginalBackup(targetPath string) (string, error) {
	backupPath := filepath.Join(backupDir,
		fmt.Sprintf("%s.original", filepath.Base(targetPath)))
	if _, err := os.Stat(backupPath); err == nil {
		return backupPath, nil
	}

	info, err := os.Stat(targetPath)
	if err != nil {
		return "", fmt.Errorf("failed to get file metadata: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return "", fmt.Errorf("failed to get file ownership information")
	}

	content, err := os.ReadFile(targetPath)
	if err != nil {
		return "", fmt.Errorf("failed to read file for original backup: %w", err)
	}
	if err := os.WriteFile(backupPath, content, info.Mode().Perm()); err != nil {
		return "", fmt.Errorf("failed to write original backup: %w", err)
	}

	if err := os.Chown(backupPath, int(stat.Uid), int(stat.Gid)); err != nil {
		return "", fmt.Errorf("failed to set ownership for original backup: %w", err)
	}
	if err := os.Chmod(backupPath, info.Mode()); err != nil {
		return "", fmt.Errorf("failed to set permissions for original backup: %w", err)
	}

	return backupPath, nil
}

// createTimestampBackup creates a timestamped backup to avoid overwriting.
func createTimestampBackup(targetPath string) (string, error) {
	ts := time.Now().Format("20060102_150405")
	backupPath := filepath.Join(backupDir,
		fmt.Sprintf("%s.%s.backup", filepath.Base(targetPath), ts))
	content, err := os.ReadFile(targetPath)
	if err != nil {
		return "", fmt.Errorf("failed to read file for timestamp backup: %w", err)
	}
	if err := os.WriteFile(backupPath, content, 0644); err != nil {
		return "", fmt.Errorf("failed to write timestamp backup: %w", err)
	}
	return backupPath, nil
}

// atomicReplaceFile writes newContent to targetPath atomically by writing to
// a temporary file and then renaming it.
func atomicReplaceFile(targetPath string, newContent []byte) error {
	// Get file metadata (permissions and ownership)
	info, err := os.Stat(targetPath)
	if err != nil {
		return fmt.Errorf("failed to get file metadata: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("failed to get file ownership information")
	}

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

	// Set ownership and permissions for the temporary file
	if err := os.Chown(tempName, int(stat.Uid), int(stat.Gid)); err != nil {
		return fmt.Errorf("failed to set ownership for temporary file: %w", err)
	}
	if err := os.Chmod(tempName, info.Mode()); err != nil {
		return fmt.Errorf("failed to set permissions for temporary file: %w", err)
	}

	if err := os.Rename(tempName, targetPath); err != nil {
		return fmt.Errorf("failed to rename temporary file: %w", err)
	}
	return nil
}

// restoreBackup restores targetPath from backupPath.
func restoreBackup(targetPath, backupPath string) error {
	// Get file metadata (permissions and ownership) from the target file
	info, err := os.Stat(targetPath)
	if err != nil {
		return fmt.Errorf("failed to get file metadata: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("failed to get file ownership information")
	}

	backupContent, err := os.ReadFile(backupPath)
	if err != nil {
		return fmt.Errorf("failed to read backup file: %w", err)
	}
	if err := os.WriteFile(targetPath, backupContent, info.Mode().Perm()); err != nil {
		return fmt.Errorf("failed to restore file: %w", err)
	}

	// Set ownership and permissions for the restored file
	if err := os.Chown(targetPath, int(stat.Uid), int(stat.Gid)); err != nil {
		return fmt.Errorf("failed to set ownership for restored file: %w", err)
	}
	if err := os.Chmod(targetPath, info.Mode()); err != nil {
		return fmt.Errorf("failed to set permissions for restored file: %w", err)
	}

	syslog.L.Info().WithMessage(
		fmt.Sprintf("Restored original file %s from backup.", targetPath),
	).Write()
	return nil
}

// sortedWalk recursively collects file paths in embedded FS, globally sorted.
func sortedWalk(embedded fs.FS, root string) ([][]byte, error) {
	var filePaths []string
	err := fs.WalkDir(embedded, root, func(path string, d fs.DirEntry,
		err error) error {
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
	for _, p := range filePaths {
		data, err := fs.ReadFile(embedded, p)
		if err != nil {
			return nil, err
		}
		results = append(results, data)
	}
	return results, nil
}

// compileJS concatenates all JavaScript files found in the embedded FS,
// joining them with newline characters.
func compileJS(embedded *embed.FS) []byte {
	parts, err := sortedWalk(*embedded, ".")
	if err != nil {
		syslog.L.Error(err).Write()
		return nil
	}
	return bytes.Join(parts, []byte("\n"))
}

// ModifyJS applies a string replacer between application JS and PBS.plus,
// and appends the contents of "pre" and "custom" JS files.
func ModifyJS(original []byte) []byte {
	replaced := []byte(jsReplacer.Replace(string(original)))
	preJS := compileJS(&preJsFS)
	customJS := compileJS(&customJsFS)
	return bytes.Join([][]byte{preJS, replaced, customJS}, []byte("\n"))
}

// ModifyLib applies a one-off string replacement.
func ModifyLib(original []byte) []byte {
	oldStr := `if (!newopts.url.match(/^\/api2/))`
	newStr := `if (!newopts.url.match(/^\/api2/) && !newopts.url.match(/^[a-z][a-z\d+\-.]*:/i))`
	return []byte(strings.Replace(string(original), oldStr, newStr, 1))
}

// checkAndRestoreOnStartup compares targetPath with the original backup.
// If they differ (e.g. after a hard reboot), the original is restored.
func checkAndRestoreOnStartup(targetPath, originalBackup string) (string, error) {
	current, err := os.ReadFile(targetPath)
	if err != nil {
		return "", err
	}
	origContent, err := os.ReadFile(originalBackup)
	if err != nil {
		return "", err
	}
	if computeChecksum(current) != computeChecksum(origContent) {
		syslog.L.Info().WithMessage(
			fmt.Sprintf("Detected leftover modification on %s; restoring original file.",
				targetPath),
		).Write()
		if err := atomicReplaceFile(targetPath, origContent); err != nil {
			return "", err
		}
		return computeChecksum(origContent), nil
	}
	return computeChecksum(current), nil
}

// WatchAndReplace watches targetPath for changes, applies modifications via
// modifyFunc, and ensures that the file is restored on shutdown (or on startup
// in case of a hard reboot). It uses debounced, atomic updates and re-adds the
// watcher on removal/rename events.
func WatchAndReplace(targetPath string,
	modifyFunc func([]byte) []byte) error {
	// Ensure backup directory exists.
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	// Create original backup once.
	originalBackup, err := createOriginalBackup(targetPath)
	if err != nil {
		return fmt.Errorf("original backup error: %w", err)
	}

	// Set up signal handler for graceful termination.
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

	// On startup, restore the file if a hard reboot left it modified.
	origChecksum, err := checkAndRestoreOnStartup(targetPath, originalBackup)
	if err != nil {
		return fmt.Errorf("startup restore error: %w", err)
	}

	// Read the current file and apply initial modification if needed.
	initialContent, err := os.ReadFile(targetPath)
	if err != nil {
		return fmt.Errorf("failed to read target file: %w", err)
	}
	modifiedContent := modifyFunc(initialContent)
	modChecksum := computeChecksum(modifiedContent)
	if origChecksum != modChecksum {
		if err := atomicReplaceFile(targetPath, modifiedContent); err != nil {
			return fmt.Errorf("failed to apply initial modification: %w", err)
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

	// Set up file watcher.
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("failed to create watcher: %w", err)
	}
	if err := watcher.Add(targetPath); err != nil {
		return fmt.Errorf("failed to add file to watcher: %w", err)
	}
	syslog.L.Info().WithMessage(
		fmt.Sprintf("Watching file: %s", targetPath),
	).Write()

	// Debounce settings.
	eventTriggerChan := make(chan struct{}, 1)
	var debounceTimer *time.Timer
	var debounceMu sync.Mutex
	const debounceDuration = 100 * time.Millisecond

	processChange := func() {
		data, err := os.ReadFile(targetPath)
		if err != nil {
			syslog.L.Error(err).Write()
			return
		}
		if computeChecksum(data) == origChecksum {
			syslog.L.Info().WithMessage(
				fmt.Sprintf("No effective change on %s, skipping.", targetPath),
			).Write()
			return
		}
		// Create a timestamped backup.
		if _, err := createTimestampBackup(targetPath); err != nil {
			syslog.L.Error(err).Write()
		}
		updated := modifyFunc(data)
		newChk := computeChecksum(updated)
		if err := atomicReplaceFile(targetPath, updated); err != nil {
			syslog.L.Error(err).Write()
			return
		}
		syslog.L.Info().WithMessage(
			fmt.Sprintf("File %s updated.", targetPath),
		).Write()
		origChecksum = newChk
	}

	// Watcher event loop.
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					syslog.L.Info().WithMessage("Watcher events channel closed").Write()
					return
				}
				// If removed or renamed, wait for re-creation.
				if event.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {
					syslog.L.Info().WithMessage(
						fmt.Sprintf("File %s removed/renamed; waiting for re-creation...",
							targetPath),
					).Write()
					for {
						time.Sleep(100 * time.Millisecond)
						if _, err := os.Stat(targetPath); err == nil {
							if err := watcher.Add(targetPath); err != nil {
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
				// For Write and Create events, debounce the change handling.
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
