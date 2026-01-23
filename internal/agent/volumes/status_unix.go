//go:build !windows

package volumes

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func isVolumeLocked(drive string) (bool, error) {
	f, err := os.Open("/proc/mounts")
	if err != nil {
		return false, nil
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 {
			if fields[0] == drive || fields[1] == drive {
				return false, nil
			}
		}
	}

	return false, nil
}

func CheckDriveStatus(drive string, subpath string) (TargetStatus, error) {
	prefix := drive

	if strings.ToLower(drive) == "root" {
		prefix = "/"
	}

	fullPath := filepath.Join(prefix, subpath)

	locked, err := isVolumeLocked(fullPath)
	if err != nil {
		return TargetStatus{}, fmt.Errorf("platform check failed: %w", err)
	}

	if locked {
		return TargetStatus{
			IsReachable: false,
			IsLocked:    true,
			Message:     "volume is encrypted and locked",
		}, nil
	}

	if _, err := os.Stat(fullPath); !os.IsNotExist(err) {
		return TargetStatus{IsReachable: true, Message: "ok"}, nil
	} else {
		return TargetStatus{IsReachable: false, Message: err.Error()}, err
	}
}
