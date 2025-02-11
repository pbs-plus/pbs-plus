//go:build linux

package proxy

import (
	"bytes"
	"embed"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

//go:embed all:views/custom
var customJsFS embed.FS

//go:embed all:views/pre
var preJsFS embed.FS

const backupDirName = "pbs-plus-backups"

var jsReplacer = strings.NewReplacer(
	"Proxmox.window.TaskViewer", "PBS.plusWindow.TaskViewer",
	"Proxmox.panel.LogView", "PBS.plusPanel.LogView",
)

// sortedWalk performs a breadth-first traversal of the given FS starting at rootPath,
// listing files grouped by directory depth and sorting entries alphabetically.
func sortedWalk(embedded fs.FS, rootPath string) ([][]byte, error) {
	var results [][]byte

	// Queue holds directories to explore.
	type entry struct {
		path string
	}
	queue := []entry{{path: rootPath}}

	// While queue is not empty.
	for len(queue) > 0 {
		// Pop the first directory.
		cur := queue[0]
		queue = queue[1:]

		// Read and sort directory entries.
		entries, err := fs.ReadDir(embedded, cur.path)
		if err != nil {
			return nil, err
		}

		// Sort entries by Name.
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].Name() < entries[j].Name()
		})

		// Process files first then add directories to the queue.
		for _, e := range entries {
			// Build the complete path.
			entryPath := filepath.Join(cur.path, e.Name())
			if e.IsDir() {
				queue = append(queue, entry{path: entryPath})
			} else {
				data, err := fs.ReadFile(embedded, entryPath)
				if err != nil {
					return nil, err
				}
				results = append(results, data)
			}
		}
	}

	return results, nil
}

// compileJS walks the embedded FS in breadth-first, alphanumerical order (shallow files
// first) and concatenates all files with newline separators.
func compileJS(embedded *embed.FS) []byte {
	parts, err := sortedWalk(embedded, ".")
	if err != nil {
		log.Println("failed to walk embed FS:", err)
		return nil
	}
	return bytes.Join(parts, []byte("\n"))
}

// mountWithBackup performs a backup of the original file and then bind mounts the new
// content over the target. It writes a temporary file in the backup directory.
func mountWithBackup(targetPath string, newContent, original []byte) error {
	// Unmount if something is already mounted.
	if utils.IsMounted(targetPath) {
		if err := syscall.Unmount(targetPath, 0); err != nil {
			return fmt.Errorf("failed to unmount existing file: %w", err)
		}
	}

	// Create backup directory.
	backupDir := filepath.Join(os.TempDir(), backupDirName)
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	// Create backup file name (could add timestamp for uniqueness if desired).
	backupPath := filepath.Join(backupDir,
		fmt.Sprintf("%s.backup", filepath.Base(targetPath)))

	// Backup the original file.
	if err := os.WriteFile(backupPath, original, 0644); err != nil {
		return fmt.Errorf("failed to create backup: %w", err)
	}

	// Write the new content to a temporary file (this is the file we bind mount).
	tempFile := filepath.Join(backupDir, filepath.Base(targetPath))
	if err := os.WriteFile(tempFile, newContent, 0644); err != nil {
		return fmt.Errorf("failed to write new content: %w", err)
	}

	// Bind mount the temporary file over the target.
	if err := syscall.Mount(tempFile, targetPath, "", syscall.MS_BIND, ""); err != nil {
		return fmt.Errorf("failed to mount file: %w", err)
	}

	return nil
}

// MountCompiledJS reads the original file, applies custom mappings, concatenates pre,
// modified original, and custom JS, then bind mounts the new file.
func MountCompiledJS(targetPath string) (func(), error) {
	// Read the original file.
	original, err := os.ReadFile(targetPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read original file: %w", err)
	}

	// Apply the custom mapping in one pass using strings.Replacer.
	modified := []byte(jsReplacer.Replace(string(original)))

	preJS := compileJS(&preJsFS)
	compiledJS := compileJS(&customJsFS)

	// Concatenate preJS, modified original, and compiledJS with newlines.
	newContent := bytes.Join([][]byte{preJS, modified, compiledJS}, []byte("\n"))

	return func() {
		unmountModdedFile(targetPath)
	}, mountWithBackup(targetPath, newContent, original)
}

// MountModdedProxmoxLib makes a simple text replacement (without custom mapping) in the
// original file and then bind mounts the modified file.
func MountModdedProxmoxLib(targetPath string) (func(), error) {
	original, err := os.ReadFile(targetPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read original file: %w", err)
	}

	oldString := `if (!newopts.url.match(/^\/api2/))`
	newString := `if (!newopts.url.match(/^\/api2/) && !newopts.url.match(/^[a-z][a-z\d+\-.]*:/i))`
	newContent := strings.Replace(string(original), oldString, newString, 1)

	return func() {
		unmountModdedFile(targetPath)
	}, mountWithBackup(targetPath, []byte(newContent), original)
}

// UnmountModdedFile unmounts the file and, if a backup exists, restores the original.
func unmountModdedFile(targetPath string) {
	if err := syscall.Unmount(targetPath, 0); err != nil {
		log.Printf("failed to unmount file: %v", err)
		return
	}

	backupDir := filepath.Join(os.TempDir(), backupDirName)
	backupPath := filepath.Join(backupDir,
		fmt.Sprintf("%s.backup", filepath.Base(targetPath)))

	if backup, err := os.ReadFile(backupPath); err == nil {
		if err := os.WriteFile(targetPath, backup, 0644); err != nil {
			log.Printf("failed to restore backup: %v", err)
			return
		}
		// Clean up the backup directory.
		os.RemoveAll(backupDir)
	}
}
