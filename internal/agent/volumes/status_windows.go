//go:build windows

package volumes

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"golang.org/x/sys/windows"
)

var bitlockerLockedRegex = regexp.MustCompile(`(?i)Lock\s+Status:\s+Locked`)

func isVolumeLocked(drive string) (bool, error) {
	cleanDrive := strings.TrimRight(strings.TrimSpace(drive), `/\`)

	if len(cleanDrive) == 1 {
		cleanDrive += ":"
	}

	if len(cleanDrive) >= 2 && cleanDrive[1] == ':' {
		cleanDrive = cleanDrive[:2]
	} else {
		return false, nil
	}

	cmd := exec.Command("manage-bde", "-status", cleanDrive)
	out, err := cmd.CombinedOutput()

	if err != nil {
		return false, nil
	}

	if bitlockerLockedRegex.Match(out) {
		return true, nil
	}

	return false, nil
}

func CheckDriveStatus(drive string, subpath string) (TargetStatus, error) {
	cleanDrive := filepath.VolumeName(drive)
	if cleanDrive == "" {
		if len(drive) == 1 {
			cleanDrive = drive + ":"
		} else {
			return TargetStatus{
				IsReachable: false,
				Message:     fmt.Sprintf("invalid drive format (%s)", drive),
			}, nil
		}
	}
	rootPath := cleanDrive + "\\"

	pathPtr, err := windows.UTF16PtrFromString(rootPath)
	if err != nil {
		return TargetStatus{IsReachable: false, Message: "invalid path encoding"}, nil
	}

	// DRIVE_REMOVABLE = 2, DRIVE_FIXED = 3, DRIVE_CDROM = 5
	driveType := windows.GetDriveType(pathPtr)
	if driveType == windows.DRIVE_NO_ROOT_DIR {
		return TargetStatus{
			IsReachable: false,
			Message:     fmt.Sprintf("drive not found (code %d %s)", driveType, rootPath),
		}, nil
	}

	oldMode := windows.SetErrorMode(windows.SEM_FAILCRITICALERRORS)
	defer windows.SetErrorMode(oldMode)

	var volName [windows.MAX_PATH + 1]uint16
	maxRetries := 5
	retryDelay := 500 * time.Millisecond

	var volErr error
	for i := 0; i < maxRetries; i++ {
		volErr = windows.GetVolumeInformation(
			pathPtr,
			&volName[0],
			uint32(len(volName)),
			nil,
			nil,
			nil,
			nil,
			0,
		)

		if volErr == windows.ERROR_NOT_READY {
			time.Sleep(retryDelay)
			continue
		}
		break
	}

	if volErr != nil {
		return TargetStatus{
			IsReachable: false,
			Message:     fmt.Sprintf("drive not ready: %v", volErr),
		}, nil
	}

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

	fullPath := filepath.Join(rootPath, subpath)
	fullPathPtr, _ := windows.UTF16PtrFromString(fullPath)
	_, statErr := windows.GetFileAttributes(fullPathPtr)
	if statErr != nil {
		return TargetStatus{IsReachable: false, Message: "subpath not found"}, nil
	}

	return TargetStatus{
		IsReachable: true,
		Message:     fmt.Sprintf("ok (Type: %d)", driveType),
	}, nil
}
