//go:build !windows

package volumes

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
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
	cleanDrive := strings.TrimSuffix(drive, "/")
	cleanDrive = strings.TrimSuffix(cleanDrive, "\\")

	locked, err := isVolumeLocked(cleanDrive)
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

	fullPath := filepath.Join(cleanDrive, subpath)
	if runtime.GOOS != "windows" && !filepath.IsAbs(fullPath) {
		fullPath = "/" + fullPath
	}

	_, err = os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return TargetStatus{IsReachable: false, Message: "path does not exist"}, nil
		}
		return TargetStatus{IsReachable: false, Message: err.Error()}, err
	}

	return TargetStatus{IsReachable: true, Message: "ok"}, nil
}
