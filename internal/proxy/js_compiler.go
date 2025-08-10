//go:build linux

package proxy

import (
	"bytes"
	"embed"
	"fmt"
	"io/fs"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

//go:embed all:views/custom
var customJsFS embed.FS

//go:embed all:views/pre
var preJsFS embed.FS

const backupDir = "/var/lib/pbs-plus/backups"

// Legacy file paths from the old method
var legacyJSPaths = []string{
	"/usr/share/javascript/proxmox-backup/js/proxmox-backup-gui.js",
	"/usr/share/javascript/proxmox-widget-toolkit/proxmoxlib.js",
}

// compileJS concatenates all JavaScript files found in the embedded FS
func compileJS(embedded *embed.FS) []byte {
	parts, err := sortedWalk(*embedded, ".")
	if err != nil {
		syslog.L.Error(err).Write()
		return nil
	}
	return bytes.Join(parts, []byte("\n"))
}

// sortedWalk recursively collects file paths in embedded FS, globally sorted
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
	for _, p := range filePaths {
		data, err := fs.ReadFile(embedded, p)
		if err != nil {
			return nil, err
		}
		results = append(results, data)
	}
	return results, nil
}

// restoreLegacyFiles restores any JavaScript files that were modified by the old method
func restoreLegacyFiles() error {
	var restoredAny bool

	for _, jsPath := range legacyJSPaths {
		backupPath := filepath.Join(backupDir, fmt.Sprintf("%s.original", filepath.Base(jsPath)))

		// Check if backup exists
		if _, err := os.Stat(backupPath); os.IsNotExist(err) {
			continue
		}

		// Check if the current file exists
		if _, err := os.Stat(jsPath); os.IsNotExist(err) {
			continue
		}

		// Read current file and backup to compare
		currentContent, err := os.ReadFile(jsPath)
		if err != nil {
			syslog.L.Error(err).WithMessage(fmt.Sprintf("Failed to read current file %s", jsPath)).Write()
			continue
		}

		backupContent, err := os.ReadFile(backupPath)
		if err != nil {
			syslog.L.Error(err).WithMessage(fmt.Sprintf("Failed to read backup file %s", backupPath)).Write()
			continue
		}

		// If files are different, restore the original
		if !bytes.Equal(currentContent, backupContent) {
			if err := restoreBackup(jsPath, backupPath); err != nil {
				syslog.L.Error(err).WithMessage(fmt.Sprintf("Failed to restore legacy file %s", jsPath)).Write()
				continue
			}

			syslog.L.Info().WithMessage(
				fmt.Sprintf("Restored legacy modified file %s to original state", jsPath),
			).Write()
			restoredAny = true
		}
	}

	if restoredAny {
		syslog.L.Info().WithMessage("Legacy JavaScript modifications have been cleaned up").Write()
	} else {
		syslog.L.Info().WithMessage("No legacy JavaScript modifications found to clean up").Write()
	}

	return nil
}

// writeJSFiles writes the compiled JS content to actual files
func writeJSFiles(jsDir string) error {
	if err := os.MkdirAll(jsDir, 0755); err != nil {
		return fmt.Errorf("failed to create JS directory: %w", err)
	}

	// Write pre JS file
	preJS := compileJS(&preJsFS)
	if len(preJS) > 0 {
		preJSPath := filepath.Join(jsDir, "pbs-plus-pre.js")
		if err := os.WriteFile(preJSPath, preJS, 0644); err != nil {
			return fmt.Errorf("failed to write pre JS file: %w", err)
		}
		syslog.L.Info().WithMessage(
			fmt.Sprintf("Pre JS file written to %s", preJSPath),
		).Write()
	}

	// Write custom JS file
	customJS := compileJS(&customJsFS)
	if len(customJS) > 0 {
		customJSPath := filepath.Join(jsDir, "pbs-plus-custom.js")
		if err := os.WriteFile(customJSPath, customJS, 0644); err != nil {
			return fmt.Errorf("failed to write custom JS file: %w", err)
		}
		syslog.L.Info().WithMessage(
			fmt.Sprintf("Custom JS file written to %s", customJSPath),
		).Write()
	}

	return nil
}

// modifyHBS modifies the HBS template to include pre and custom JS file references
func modifyHBS(original []byte) []byte {
	content := string(original)

	// Find the main PBS JS script line
	mainScriptLine := `<script type="text/javascript" src="/js/proxmox-backup-gui.js"></script>`

	if !strings.Contains(content, mainScriptLine) {
		syslog.L.Error(fmt.Errorf("main script line not found in HBS template")).Write()
		return original
	}

	// Check if our modifications are already present
	if strings.Contains(content, "pbs-plus-pre.js") && strings.Contains(content, "pbs-plus-custom.js") {
		syslog.L.Info().WithMessage("HBS template already contains PBS-Plus modifications").Write()
		return original
	}

	// Create script tags for pre and custom JS files
	preScriptTag := `<script type="text/javascript" src="/js/pbs-plus-pre.js"></script>`
	customScriptTag := `<script type="text/javascript" src="/js/pbs-plus-custom.js"></script>`

	// Replace the main script line with pre + main + custom
	newScriptSection := fmt.Sprintf("%s\n%s\n%s", preScriptTag, mainScriptLine, customScriptTag)

	modifiedContent := strings.Replace(content, mainScriptLine, newScriptSection, 1)

	return []byte(modifiedContent)
}

// createOriginalBackup creates a backup that preserves the original file
func createOriginalBackup(targetPath string, force bool) (string, error) {
	backupPath := filepath.Join(backupDir, fmt.Sprintf("%s.original", filepath.Base(targetPath)))

	if !force {
		if _, err := os.Stat(backupPath); err == nil {
			return backupPath, nil
		}
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

// atomicReplaceFile writes newContent to targetPath atomically
func atomicReplaceFile(targetPath string, newContent []byte) error {
	info, err := os.Stat(targetPath)
	if err != nil {
		return fmt.Errorf("failed to get file metadata: %w", err)
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

	if err := os.Chmod(tempName, info.Mode()); err != nil {
		return fmt.Errorf("failed to set permissions for temporary file: %w", err)
	}

	if err := os.Rename(tempName, targetPath); err != nil {
		return fmt.Errorf("failed to rename temporary file: %w", err)
	}

	return nil
}

// restoreBackup restores targetPath from backupPath
func restoreBackup(targetPath, backupPath string) error {
	backupContent, err := os.ReadFile(backupPath)
	if err != nil {
		return fmt.Errorf("failed to read backup file: %w", err)
	}

	if err := os.WriteFile(targetPath, backupContent, 0644); err != nil {
		return fmt.Errorf("failed to restore file: %w", err)
	}

	syslog.L.Info().WithMessage(
		fmt.Sprintf("Restored original file %s from backup.", targetPath),
	).Write()
	return nil
}

// cleanupJSFiles removes the generated JS files
func cleanupJSFiles(jsDir string) {
	preJSPath := filepath.Join(jsDir, "pbs-plus-pre.js")
	customJSPath := filepath.Join(jsDir, "pbs-plus-custom.js")

	if err := os.Remove(preJSPath); err != nil && !os.IsNotExist(err) {
		syslog.L.Error(err).WithMessage("Failed to remove pre JS file").Write()
	} else if err == nil {
		syslog.L.Info().WithMessage("Pre JS file removed").Write()
	}

	if err := os.Remove(customJSPath); err != nil && !os.IsNotExist(err) {
		syslog.L.Error(err).WithMessage("Failed to remove custom JS file").Write()
	} else if err == nil {
		syslog.L.Info().WithMessage("Custom JS file removed").Write()
	}
}

func ModifyPBSHandlebars(hbsPath, jsDir string) error {
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	// FIRST: Clean up any legacy JavaScript file modifications
	syslog.L.Info().WithMessage("Checking for legacy JavaScript modifications to clean up...").Write()
	if err := restoreLegacyFiles(); err != nil {
		return fmt.Errorf("failed to restore legacy files: %w", err)
	}

	// Write JS files to the specified directory
	if err := writeJSFiles(jsDir); err != nil {
		return fmt.Errorf("failed to write JS files: %w", err)
	}

	// Create original backup of HBS file
	originalBackup, err := createOriginalBackup(hbsPath, false)
	if err != nil {
		return fmt.Errorf("original backup error: %w", err)
	}

	// Watch and modify the HBS file
	if err := watchAndReplaceHBS(hbsPath, modifyHBS); err != nil {
		return err
	}

	// Set up signal handler for graceful termination
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		syslog.L.Info().WithMessage(
			fmt.Sprintf("Termination signal (%v) received. Cleaning up...", sig),
		).Write()

		// Restore HBS backup
		if err := restoreBackup(hbsPath, originalBackup); err != nil {
			syslog.L.Error(err).Write()
		}

		// Clean up JS files
		cleanupJSFiles(jsDir)

		os.Exit(0)
	}()

	return nil
}

// watchAndReplaceHBS watches HBS template file for changes and applies modifications
func watchAndReplaceHBS(targetPath string, modifyFunc func([]byte) []byte) error {
	// Apply initial modification
	content, err := os.ReadFile(targetPath)
	if err != nil {
		return fmt.Errorf("failed to read target file: %w", err)
	}

	modifiedContent := modifyFunc(content)
	if !bytes.Equal(content, modifiedContent) {
		if err := atomicReplaceFile(targetPath, modifiedContent); err != nil {
			return fmt.Errorf("failed to apply initial modification: %w", err)
		}

		syslog.L.Info().WithMessage(
			fmt.Sprintf("HBS template %s modified with JS file references.", targetPath),
		).Write()
	} else {
		syslog.L.Info().WithMessage(
			fmt.Sprintf("HBS template %s already up to date.", targetPath),
		).Write()
	}

	// Set up file watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("failed to create watcher: %w", err)
	}

	if err := watcher.Add(targetPath); err != nil {
		return fmt.Errorf("failed to add file to watcher: %w", err)
	}

	syslog.L.Info().WithMessage(
		fmt.Sprintf("Watching HBS template: %s", targetPath),
	).Write()

	// Watch for changes
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}

				if event.Op&(fsnotify.Write|fsnotify.Create) != 0 {
					// Debounce rapid changes
					time.Sleep(100 * time.Millisecond)

					content, err := os.ReadFile(targetPath)
					if err != nil {
						syslog.L.Error(err).Write()
						continue
					}

					modifiedContent := modifyFunc(content)
					if !bytes.Equal(content, modifiedContent) {
						if err := atomicReplaceFile(targetPath, modifiedContent); err != nil {
							syslog.L.Error(err).Write()
							continue
						}

						syslog.L.Info().WithMessage(
							fmt.Sprintf("HBS template %s updated with modifications.", targetPath),
						).Write()
					}
				}

			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				syslog.L.Error(err).Write()
			}
		}
	}()

	return nil
}
