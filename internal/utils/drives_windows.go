//go:build windows

package utils

import (
	"fmt"
	"runtime"

	"golang.org/x/sys/windows"
)

// DriveInfo contains detailed information about a drive

// getDriveTypeString returns a human-readable string describing the type of drive
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

// humanizeBytes converts a byte count into a human-readable string with appropriate units (KB, MB, GB, TB)
func humanizeBytes(bytes uint64) string {
	const unit = 1000
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := unit, 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	var unitSymbol string
	switch exp {
	case 0:
		unitSymbol = "KB"
	case 1:
		unitSymbol = "MB"
	case 2:
		unitSymbol = "GB"
	case 3:
		unitSymbol = "TB"
	case 4:
		unitSymbol = "PB"
	default:
		unitSymbol = "??"
	}
	return fmt.Sprintf("%.2f %s", float64(bytes)/float64(div), unitSymbol)
}

// GetLocalDrives returns a slice of DriveInfo containing detailed information about each local drive
func GetLocalDrives() ([]DriveInfo, error) {
	var drives []DriveInfo

	for _, drive := range "ABCDEFGHIJKLMNOPQRSTUVWXYZ" {
		path := fmt.Sprintf("%c:\\", drive)
		pathUtf16, err := windows.UTF16PtrFromString(path)
		if err != nil {
			continue // Skip invalid paths
		}

		driveType := windows.GetDriveType(pathUtf16)
		if driveType == windows.DRIVE_NO_ROOT_DIR {
			continue // Drive not present
		}

		var (
			volumeNameStr string
			fileSystemStr string
			totalBytes    uint64
			freeBytes     uint64
		)

		// Retrieve volume information
		var volumeName [windows.MAX_PATH + 1]uint16
		var fileSystemName [windows.MAX_PATH + 1]uint16
		if err := windows.GetVolumeInformation(
			pathUtf16,
			&volumeName[0],
			uint32(len(volumeName)),
			nil,
			nil,
			nil,
			&fileSystemName[0],
			uint32(len(fileSystemName)),
		); err == nil {
			volumeNameStr = windows.UTF16ToString(volumeName[:])
			fileSystemStr = windows.UTF16ToString(fileSystemName[:])
		}

		// Retrieve disk space information
		var totalFreeBytes uint64
		if err := windows.GetDiskFreeSpaceEx(
			pathUtf16,
			nil,
			&totalBytes,
			&totalFreeBytes,
		); err == nil {
			freeBytes = totalFreeBytes
		}

		usedBytes := totalBytes - freeBytes

		// Humanize byte counts
		totalHuman := humanizeBytes(totalBytes)
		usedHuman := humanizeBytes(usedBytes)
		freeHuman := humanizeBytes(freeBytes)

		drives = append(drives, DriveInfo{
			Letter:          string(drive),
			Type:            getDriveTypeString(driveType),
			VolumeName:      volumeNameStr,
			FileSystem:      fileSystemStr,
			TotalBytes:      totalBytes,
			UsedBytes:       usedBytes,
			FreeBytes:       freeBytes,
			Total:           totalHuman,
			Used:            usedHuman,
			Free:            freeHuman,
			OperatingSystem: runtime.GOOS,
		})
	}

	return drives, nil
}
