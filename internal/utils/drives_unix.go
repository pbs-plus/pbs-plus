//go:build unix

package utils

import (
	"fmt"
	"syscall"
)

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
	// Get disk space information for the root filesystem
	var stat syscall.Statfs_t
	if err := syscall.Statfs("/", &stat); err != nil {
		return nil, fmt.Errorf("failed to get filesystem stats for root: %w", err)
	}

	totalBytes := stat.Blocks * uint64(stat.Bsize)
	freeBytes := stat.Bfree * uint64(stat.Bsize)
	usedBytes := totalBytes - freeBytes

	// Humanize byte counts
	totalHuman := humanizeBytes(totalBytes)
	usedHuman := humanizeBytes(usedBytes)
	freeHuman := humanizeBytes(freeBytes)

	// Create a single drive representing the entire Linux filesystem
	drive := DriveInfo{
		Letter:     "Root",
		Type:       "Fixed",
		VolumeName: "Root",
		FileSystem: "Root Filesystem",
		TotalBytes: totalBytes,
		UsedBytes:  usedBytes,
		FreeBytes:  freeBytes,
		Total:      totalHuman,
		Used:       usedHuman,
		Free:       freeHuman,
	}

	return []DriveInfo{drive}, nil
}
