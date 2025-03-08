package snapshots

import (
	"fmt"
	"os/exec"
	"runtime"
	"strings"
)

// detectFilesystem detects the filesystem type of the given source path
func detectFilesystem(sourcePath string) (string, error) {
	switch runtime.GOOS {
	case "linux":
		// Use `lsblk` to detect the filesystem type on Linux
		cmd := exec.Command("lsblk", "-no", "FSTYPE", sourcePath)
		output, err := cmd.Output()
		if err != nil {
			return "", fmt.Errorf("failed to detect filesystem type: %w", err)
		}
		return strings.TrimSpace(string(output)), nil

	case "darwin":
		// Use `diskutil` to detect the filesystem type on macOS
		cmd := exec.Command("diskutil", "info", sourcePath)
		output, err := cmd.Output()
		if err != nil {
			return "", fmt.Errorf("failed to detect filesystem type: %w", err)
		}
		for _, line := range strings.Split(string(output), "\n") {
			if strings.Contains(line, "File System Personality") {
				parts := strings.Split(line, ":")
				if len(parts) > 1 {
					return strings.TrimSpace(parts[1]), nil
				}
			}
		}
		return "", fmt.Errorf("could not determine filesystem type from diskutil output")

	case "windows":
		// On Windows, assume NTFS or ReFS and use VSS
		return "ntfs", nil

	default:
		return "", fmt.Errorf("unsupported operating system: %s", runtime.GOOS)
	}
}
