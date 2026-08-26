//go:build unix

package agent

import (
	"fmt"
	"syscall"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
)

// GetLocalDrives returns a slice of DriveInfo containing detailed information about each local drive
func GetLocalDrives() ([]fswire.DriveInfo, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs("/", &stat); err != nil {
		return nil, fmt.Errorf("failed to get filesystem stats for root: %w", err)
	}

	totalBytes := stat.Blocks * uint64(stat.Bsize)
	freeBytes := stat.Bfree * uint64(stat.Bsize)
	usedBytes := totalBytes - freeBytes

	totalHuman := fswire.HumanizeBytes(totalBytes)
	usedHuman := fswire.HumanizeBytes(usedBytes)
	freeHuman := fswire.HumanizeBytes(freeBytes)

	drive := fswire.DriveInfo{
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

	return []fswire.DriveInfo{drive}, nil
}
