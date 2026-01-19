//go:build unix

package snapshots

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func getFsType(path string) (fsType string, mountPoint string, err error) {
	data, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return "", "", err
	}

	lines := strings.Split(string(data), "\n")
	var bestMount string
	var bestDevice string

	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}

		mPoint := fields[1]
		if strings.HasPrefix(path, mPoint) {
			if len(mPoint) > len(bestMount) {
				bestDevice = fields[0]
				bestMount = mPoint
				fsType = fields[2]
			}
		}
	}

	if fsType == "" {
		return "", "", fmt.Errorf("could not find mount point for path: %s", path)
	}

	if strings.HasPrefix(bestDevice, "/dev/mapper/") || strings.HasPrefix(bestDevice, "/dev/stack/") {
		fsType = "lvm"
	} else {
		if _, err := os.Stat(bestDevice); err == nil {
			if realPath, err := filepath.EvalSymlinks(bestDevice); err == nil {
				if strings.Contains(realPath, "dm-") {
					fsType = "lvm"
				}
			}
		}
	}

	return strings.ToLower(fsType), bestMount, nil
}
