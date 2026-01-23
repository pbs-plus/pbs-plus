//go:build windows

package volumes

import (
	"fmt"
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
)

func isDriveLetterAssigned(driveLetter string) bool {
	if len(driveLetter) == 0 {
		return false
	}
	letter := strings.ToUpper(driveLetter)[:1][0]
	if letter < 'A' || letter > 'Z' {
		return false
	}
	bitmask, _ := windows.GetLogicalDrives()
	shift := letter - 'A'
	return (bitmask & (1 << shift)) != 0
}

func CheckDriveStatus(drive string, subpath string) (TargetStatus, error) {
	cleanDrive := filepath.VolumeName(drive)
	if cleanDrive == "" {
		if len(drive) == 1 {
			cleanDrive = drive + ":"
		} else {
			return TargetStatus{IsReachable: false, Message: "invalid drive format"}, nil
		}
	}

	driveLetterOnly := strings.TrimRight(cleanDrive, ":")
	if !isDriveLetterAssigned(driveLetterOnly) {
		return TargetStatus{
			IsReachable: false,
			Message:     fmt.Sprintf("drive %s: is not assigned/mapped", driveLetterOnly),
		}, nil
	}

	rootPath := cleanDrive + "\\"
	pathPtr, err := windows.UTF16PtrFromString(rootPath)
	if err != nil {
		return TargetStatus{IsReachable: false, Message: "invalid path encoding"}, nil
	}

	driveType := windows.GetDriveType(pathPtr)
	if driveType == windows.DRIVE_NO_ROOT_DIR {
		return TargetStatus{IsReachable: false, Message: "drive has no root directory"}, nil
	}

	oldMode := windows.SetErrorMode(windows.SEM_FAILCRITICALERRORS)
	defer windows.SetErrorMode(oldMode)

	if subpath != "" {
		fullPath := filepath.Join(rootPath, subpath)
		fullPathPtr, err := windows.UTF16PtrFromString(fullPath)
		if err != nil {
			return TargetStatus{IsReachable: false, Message: "invalid subpath encoding"}, nil
		}

		_, statErr := windows.GetFileAttributes(fullPathPtr)
		if statErr != nil {
			return TargetStatus{
				IsReachable: false,
				Message:     fmt.Sprintf("subpath not found: %s", subpath),
			}, nil
		}
	}

	return TargetStatus{
		IsReachable: true,
		Message:     fmt.Sprintf("ready (Type: %d)", driveType),
	}, nil
}
