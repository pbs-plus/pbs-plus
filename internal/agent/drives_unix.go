//go:build unix

package agent

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"syscall"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
	"github.com/pbs-plus/pbs-plus/internal/agent/snapshots"
)

// GetLocalDrives returns a slice of DriveInfo containing detailed information about each local drive
func GetLocalDrives() ([]fswire.DriveInfo, error) {
	mounts, err := snapshots.Mounts()
	if err != nil {
		return rootDriveOnly()
	}

	pseudo, err := pseudoFilesystems()
	if err != nil {
		return rootDriveOnly()
	}

	seen := make(map[string]struct{}, len(mounts))
	drives := make([]fswire.DriveInfo, 0, len(mounts))
	for _, mount := range mounts {
		if _, skip := pseudo[mount.FSType]; skip {
			continue
		}
		if _, dup := seen[mount.MountPoint]; dup {
			continue
		}
		seen[mount.MountPoint] = struct{}{}

		drive, err := driveInfo(mount.MountPoint, mount.FSType, mount.Source)
		if err != nil {
			continue
		}
		drives = append(drives, drive)
	}

	if len(drives) == 0 {
		return rootDriveOnly()
	}
	return drives, nil
}

func rootDriveOnly() ([]fswire.DriveInfo, error) {
	drive, err := driveInfo("/", "Root Filesystem", "Root")
	if err != nil {
		return nil, err
	}
	return []fswire.DriveInfo{drive}, nil
}

func driveInfo(mountPoint string, fsType string, volumeName string) (fswire.DriveInfo, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(mountPoint, &stat); err != nil {
		return fswire.DriveInfo{}, fmt.Errorf("failed to get filesystem stats for %s: %w", mountPoint, err)
	}

	totalBytes := stat.Blocks * uint64(stat.Bsize)
	freeBytes := stat.Bfree * uint64(stat.Bsize)
	usedBytes := totalBytes - freeBytes

	return fswire.DriveInfo{
		Letter:     mountPoint,
		Type:       "Fixed",
		VolumeName: volumeName,
		FileSystem: fsType,
		TotalBytes: totalBytes,
		UsedBytes:  usedBytes,
		FreeBytes:  freeBytes,
		Total:      fswire.HumanizeBytes(totalBytes),
		Used:       fswire.HumanizeBytes(usedBytes),
		Free:       fswire.HumanizeBytes(freeBytes),
	}, nil
}

func pseudoFilesystems() (map[string]struct{}, error) {
	f, err := os.Open("/proc/filesystems")
	if err != nil {
		return nil, err
	}
	defer f.Close()

	types := make(map[string]struct{})
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 2 && fields[0] == "nodev" {
			types[fields[1]] = struct{}{}
		}
	}
	return types, scanner.Err()
}
