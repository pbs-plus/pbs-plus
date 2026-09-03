package snapshots

import (
	"fmt"
	"os/exec"
	"runtime"
	"strings"
)

func detectFilesystem(mountPoint string) (string, error) {
	switch runtime.GOOS {
	case "linux":
		mount, err := FindMount(mountPoint)
		if err != nil {
			return "", err
		}
		return mount.FSType, nil

	case "darwin":
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
		return "ntfs", nil

	default:
		return "", fmt.Errorf("unsupported operating system: %s", runtime.GOOS)
	}
}
