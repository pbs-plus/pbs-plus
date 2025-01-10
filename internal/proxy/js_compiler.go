//go:build linux

package proxy

import (
	"bufio"
	"embed"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

//go:embed all:views
var customJsFS embed.FS

func compileCustomJS() []byte {
	result := []byte(`
const pbsFullUrl = window.location.href;
const pbsUrl = new URL(pbsFullUrl);
const pbsPlusBaseUrl = ` + "`${pbsUrl.protocol}//${pbsUrl.hostname}:8008`" + `;

function getCookie(cName) {
	const name = cName + "=";
  const cDecoded = decodeURIComponent(document.cookie);
  const cArr = cDecoded.split('; ');
  let res;
  cArr.forEach(val => {
    if (val.indexOf(name) === 0) res = val.substring(name.length);
  })
  return res
}

var pbsPlusTokenHeaders = {
	"Content-Type": "application/json",
};

if (Proxmox.CSRFPreventionToken) {
	pbsPlusTokenHeaders["Csrfpreventiontoken"] = Proxmox.CSRFPreventionToken;
}

fetch(pbsPlusBaseUrl + "/plus/token", {
	method: "POST",
	body: JSON.stringify({
		"pbs_auth_cookie": getCookie("PBSAuthCookie"),
	}),
	headers: pbsPlusTokenHeaders,
})
`)

	err := fs.WalkDir(customJsFS, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		content, err := customJsFS.ReadFile(path)
		if err != nil {
			return err
		}
		result = append(result, content...)
		result = append(result, []byte("\n")...)
		return nil
	})
	if err != nil {
		log.Println(err)
	}
	return result
}

// MountCompiledJS creates a backup of the target file and mounts the compiled JS over it
func MountCompiledJS(targetPath string) error {
	// Check if something is already mounted at the target path
	if isMounted(targetPath) {
		if err := syscall.Unmount(targetPath, 0); err != nil {
			return fmt.Errorf("failed to unmount existing file: %w", err)
		}
	}

	// Create backup directory if it doesn't exist
	backupDir := filepath.Join(os.TempDir(), "pbs-plus-backups")
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	// Create backup filename with timestamp
	backupPath := filepath.Join(backupDir, fmt.Sprintf("%s.backup", filepath.Base(targetPath)))

	// Read existing file
	original, err := os.ReadFile(targetPath)
	if err != nil {
		return fmt.Errorf("failed to read original file: %w", err)
	}

	// Create backup
	if err := os.WriteFile(backupPath, original, 0644); err != nil {
		return fmt.Errorf("failed to create backup: %w", err)
	}

	// Create new file with compiled JS
	compiledJS := compileCustomJS()

	newContent := make([]byte, len(original)+1+len(compiledJS))
	copy(newContent, original)
	newContent[len(original)] = '\n' // Add newline
	copy(newContent[len(original)+1:], compiledJS)

	tempFile := filepath.Join(backupDir, filepath.Base(targetPath))
	if err := os.WriteFile(tempFile, newContent, 0644); err != nil {
		return fmt.Errorf("failed to write new content: %w", err)
	}

	// Perform bind mount
	if err := syscall.Mount(tempFile, targetPath, "", syscall.MS_BIND, ""); err != nil {
		return fmt.Errorf("failed to mount file: %w", err)
	}

	return nil
}

func MountModdedProxmoxLib(targetPath string) error {
	// Check if something is already mounted at the target path
	if isMounted(targetPath) {
		if err := syscall.Unmount(targetPath, 0); err != nil {
			return fmt.Errorf("failed to unmount existing file: %w", err)
		}
	}

	// Create backup directory if it doesn't exist
	backupDir := filepath.Join(os.TempDir(), "pbs-plus-backups")
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	// Create backup filename with timestamp
	backupPath := filepath.Join(backupDir, fmt.Sprintf("%s.backup", filepath.Base(targetPath)))

	// Read existing file
	original, err := os.ReadFile(targetPath)
	if err != nil {
		return fmt.Errorf("failed to read original file: %w", err)
	}

	// Create backup
	if err := os.WriteFile(backupPath, original, 0644); err != nil {
		return fmt.Errorf("failed to create backup: %w", err)
	}

	oldString := `if (!newopts.url.match(/^\/api2/))`
	newString := `if (!newopts.url.match(/^\/api2/) && !newopts.url.match(/^[a-z][a-z\d+\-.]*:/i))`

	// Perform the replacement
	newContent := strings.Replace(string(original), oldString, newString, 1)

	tempFile := filepath.Join(backupDir, filepath.Base(targetPath))
	if err := os.WriteFile(tempFile, []byte(newContent), 0644); err != nil {
		return fmt.Errorf("failed to write new content: %w", err)
	}

	// Perform bind mount
	if err := syscall.Mount(tempFile, targetPath, "", syscall.MS_BIND, ""); err != nil {
		return fmt.Errorf("failed to mount file: %w", err)
	}

	return nil
}

// UnmountCompiledJS unmounts the file and restores the original
func UnmountModdedFile(targetPath string) error {
	// Unmount the file
	if err := syscall.Unmount(targetPath, 0); err != nil {
		return fmt.Errorf("failed to unmount file: %w", err)
	}

	// Path to backup file
	backupDir := filepath.Join(os.TempDir(), "pbs-plus-backups")
	backupPath := filepath.Join(backupDir, fmt.Sprintf("%s.backup", filepath.Base(targetPath)))

	// Restore from backup if it exists
	if _, err := os.Stat(backupPath); err == nil {
		backup, err := os.ReadFile(backupPath)
		if err != nil {
			return fmt.Errorf("failed to read backup: %w", err)
		}

		if err := os.WriteFile(targetPath, backup, 0644); err != nil {
			return fmt.Errorf("failed to restore backup: %w", err)
		}

		// Clean up backup files
		os.RemoveAll(backupDir)
	}

	return nil
}

func isMounted(path string) bool {
	// Open /proc/self/mountinfo to check mounts
	mountInfoFile, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return false
	}
	defer mountInfoFile.Close()

	scanner := bufio.NewScanner(mountInfoFile)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) >= 5 && fields[4] == path {
			return true
		}
	}

	return false
}
