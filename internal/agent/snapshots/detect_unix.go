//go:build unix

package snapshots

import (
	"fmt"
	"os"
	"strings"
)

func getFsType(path string) (string, string, error) {
	data, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return "", "", err
	}

	lines := strings.Split(string(data), "\n")
	var bestMount string
	var fsType string

	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}

		mountPoint := fields[1]
		if strings.HasPrefix(path, mountPoint) {
			if len(mountPoint) > len(bestMount) {
				bestMount = mountPoint
				fsType = fields[2]
			}
		}
	}

	if fsType == "" {
		return "", "", fmt.Errorf("could not find mount point for path: %s", path)
	}

	return strings.ToLower(fsType), bestMount, nil
}
