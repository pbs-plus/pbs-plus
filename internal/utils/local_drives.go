//go:build windows

package utils

import (
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

// GetDriveType returns a human-readable string describing the type of drive
func getDriveTypeString(dt uint32) string {
	switch dt {
	case windows.DRIVE_UNKNOWN:
		return "Unknown"
	case windows.DRIVE_NO_ROOT_DIR:
		return "No Root Directory"
	case windows.DRIVE_REMOVABLE:
		return "Removable"
	case windows.DRIVE_FIXED:
		return "Fixed"
	case windows.DRIVE_REMOTE:
		return "Network"
	case windows.DRIVE_CDROM:
		return "CD-ROM"
	case windows.DRIVE_RAMDISK:
		return "RAM Disk"
	default:
		return "Unknown"
	}
}

// GetLocalDrives returns a slice of DriveInfo containing both the drive letter and type
func GetLocalDrives() ([]DriveInfo, error) {
	var drives []DriveInfo

	for _, drive := range "ABCDEFGHIJKLMNOPQRSTUVWXYZ" {
		path := fmt.Sprintf("%c:\\", drive)

		// First check if the drive exists by trying to open it
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		f.Close()

		// Convert path to UTF16 for Windows API
		pathUtf16, err := windows.UTF16PtrFromString(path)
		if err != nil {
			return nil, fmt.Errorf("failed to convert path to UTF16: %v", err)
		}

		// Get drive type using Windows API
		driveType := windows.GetDriveType(pathUtf16)

		drives = append(drives, DriveInfo{
			Letter: string(drive),
			Type:   getDriveTypeString(driveType),
		})
	}

	return drives, nil
}
