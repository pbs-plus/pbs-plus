//go:build windows

package volumes

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

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
		return TargetStatus{IsReachable: false, Message: "invalid drive format"}, nil
	}
	rootPath := cleanDrive + "\\"

	// DRIVE_REMOVABLE = 2, DRIVE_FIXED = 3, DRIVE_CDROM = 5
	driveType := windows.GetDriveType(windows.StringToUTF16Ptr(rootPath))
	if driveType == windows.DRIVE_NO_ROOT_DIR {
		return TargetStatus{IsReachable: false, Message: "drive not found"}, nil
	}

	oldMode := windows.SetErrorMode(windows.SEM_FAILCRITICALERRORS)
	defer windows.SetErrorMode(oldMode)

	var volName [windows.MAX_PATH + 1]uint16
	err := windows.GetVolumeInformation(
		windows.StringToUTF16Ptr(rootPath),
		&volName[0],
		uint32(len(volName)),
		nil,
		nil,
		nil,
		nil,
		0,
	)

	if err != nil {
		return TargetStatus{
			IsReachable: false,
			Message:     fmt.Sprintf("drive not ready: %v", err),
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
	_, statErr := windows.GetFileAttributes(windows.StringToUTF16Ptr(fullPath))
	if statErr != nil {
		return TargetStatus{IsReachable: false, Message: "subpath not found"}, nil
	}

	return TargetStatus{
		IsReachable: true,
		Message:     fmt.Sprintf("ok (Type: %d)", driveType),
	}, nil
}
