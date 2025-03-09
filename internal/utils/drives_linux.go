//go:build linux

package utils

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"syscall"
)

// getDriveType dynamically determines the type of drive based on its mount point and filesystem type
func getDriveType(mountPoint, fsType string) string {
	// Check if the mount point is a removable device
	if strings.HasPrefix(mountPoint, "/media/") || strings.HasPrefix(mountPoint, "/mnt/") {
		return "Removable"
	}

	// Check if the filesystem type indicates a network drive
	switch fsType {
	case "nfs", "cifs", "smbfs", "fuse.sshfs":
		return "Network"
	case "iso9660":
		return "CD-ROM"
	}

	// Default to "Fixed" for other cases
	return "Fixed"
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

	// List of filesystem types to ignore
	excludedFsTypes := map[string]bool{
		"tmpfs":           true,
		"devtmpfs":        true,
		"proc":            true,
		"sysfs":           true,
		"securityfs":      true,
		"cgroup2":         true,
		"pstore":          true,
		"efivarfs":        true,
		"bpf":             true,
		"debugfs":         true,
		"mqueue":          true,
		"hugetlbfs":       true,
		"fusectl":         true,
		"configfs":        true,
		"ramfs":           true,
		"fuse.gvfsd-fuse": true,
	}

	// Use the `df` command to get information about mounted filesystems
	output, err := exec.Command("df", "-T").Output()
	if err != nil {
		return nil, fmt.Errorf("failed to execute df command: %w", err)
	}

	// Parse /proc/mounts to get filesystem types for each mount point
	fsTypes := make(map[string]string)
	mountsFile, err := os.Open("/proc/mounts")
	if err != nil {
		return nil, fmt.Errorf("failed to open /proc/mounts: %w", err)
	}
	defer mountsFile.Close()

	scanner := bufio.NewScanner(mountsFile)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 3 {
			mountPoint := fields[1]
			fsType := fields[2]
			fsTypes[mountPoint] = fsType
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read /proc/mounts: %w", err)
	}

	// Process the output of `df -T`
	lines := strings.Split(string(output), "\n")
	for _, line := range lines[1:] { // Skip the header line
		fields := strings.Fields(line)
		if len(fields) < 7 {
			continue // Skip malformed lines
		}

		// Parse the fields from the `df` output
		fsType := fields[1]
		mountPoint := fields[6]

		// Check if the filesystem type is in the excluded list
		if excludedFsTypes[fsType] {
			continue
		}

		// Get disk space information
		var stat syscall.Statfs_t
		if err := syscall.Statfs(mountPoint, &stat); err != nil {
			continue // Skip if we can't get stats for the mount point
		}

		totalBytes := stat.Blocks * uint64(stat.Bsize)
		freeBytes := stat.Bfree * uint64(stat.Bsize)
		usedBytes := totalBytes - freeBytes

		// Humanize byte counts
		totalHuman := humanizeBytes(totalBytes)
		usedHuman := humanizeBytes(usedBytes)
		freeHuman := humanizeBytes(freeBytes)

		// Determine the filesystem type dynamically
		if dynamicFsType, ok := fsTypes[mountPoint]; ok {
			fsType = dynamicFsType
		}

		// Append the drive information
		drives = append(drives, DriveInfo{
			Letter:          mountPoint,
			Type:            getDriveType(mountPoint, fsType), // Dynamically determine the drive type
			VolumeName:      "",                               // Linux doesn't have a direct equivalent for volume names
			FileSystem:      fsType,
			TotalBytes:      totalBytes,
			UsedBytes:       usedBytes,
			FreeBytes:       freeBytes,
			Total:           totalHuman,
			Used:            usedHuman,
			Free:            freeHuman,
			OperatingSystem: runtime.GOOS, // Add the operating system name
		})
	}

	return drives, nil
}
