package snapshots

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
)

// detectFilesystem detects the filesystem type of the given source path
func detectFilesystem(mountPoint string) (string, error) {
	switch runtime.GOOS {
	case "linux":
		// Use /proc/mounts to find the filesystem type for the given mount point
		mountsFile, err := os.Open("/proc/mounts")
		if err != nil {
			return "", fmt.Errorf("failed to open /proc/mounts: %w", err)
		}
		defer mountsFile.Close()

		scanner := bufio.NewScanner(mountsFile)
		for scanner.Scan() {
			fields := strings.Fields(scanner.Text())
			if len(fields) >= 3 {
				mount := fields[1]
				fsType := fields[2]
				if mount == mountPoint {
					return fsType, nil
				}
			}
		}
		if err := scanner.Err(); err != nil {
			return "", fmt.Errorf("failed to read /proc/mounts: %w", err)
		}
		return "", fmt.Errorf("mount point %s not found in /proc/mounts", mountPoint)

	case "darwin":
		// Use `diskutil` to detect the filesystem type on macOS
		cmd := exec.Command("diskutil", "info", mountPoint)
		output, err := cmd.Output()
		if err != nil {
			return "", fmt.Errorf("failed to detect filesystem type: %w", err)
		}
		for line := range strings.SplitSeq(string(output), "\n") {
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
