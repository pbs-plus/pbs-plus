//go:build unix

package agent

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"golang.org/x/sys/unix"
)

// filesystems we consider "backup-worthy" data volumes.
var backupFilesystems = map[string]bool{
	"ext2":  true,
	"ext3":  true,
	"ext4":  true,
	"xfs":   true,
	"btrfs": true,
	"zfs":   true,
}

// filesystems we always skip (virtual / kernel interfaces).
var skipFilesystems = map[string]bool{
	"proc":       true,
	"sysfs":      true,
	"devtmpfs":   true,
	"devpts":     true,
	"tmpfs":      true,
	"cgroup":     true,
	"cgroup2":    true,
	"pstore":     true,
	"debugfs":    true,
	"tracefs":    true,
	"fusectl":    true,
	"configfs":   true,
	"securityfs": true,
	"hugetlbfs":  true,
	"mqueue":     true,
	"bpf":        true,
}

// GetLocalDrives returns a slice of DriveInfo for each mount point on a
// known backup-worthy filesystem, mirroring how Windows enumerates
// drive letters per volume.
func GetLocalDrives() ([]types.DriveInfo, error) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return nil, fmt.Errorf("open /proc/self/mountinfo: %w", err)
	}
	defer func() { _ = f.Close() }()

	var drives []types.DriveInfo
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		entry, ok := parseMountinfoLine(scanner.Text())
		if !ok {
			continue
		}

		if skipFilesystems[entry.fsType] {
			continue
		}
		if !backupFilesystems[entry.fsType] {
			continue
		}

		// Skip system paths and non-directory mounts (Docker bind-mounted files).
		if !isDataMountPoint(entry.mountPoint) {
			continue
		}

		var stat unix.Statfs_t
		if err := unix.Statfs(entry.mountPoint, &stat); err != nil {
			continue
		}

		totalBytes := stat.Blocks * uint64(stat.Bsize)
		freeBytes := stat.Bfree * uint64(stat.Bsize)
		usedBytes := totalBytes - freeBytes

		drives = append(drives, types.DriveInfo{
			Letter:     entry.mountPoint,
			Type:       "Fixed",
			VolumeName: entry.mountPoint,
			FileSystem: entry.fsType,
			TotalBytes: totalBytes,
			UsedBytes:  usedBytes,
			FreeBytes:  freeBytes,
			Total:      types.HumanizeBytes(totalBytes),
			Used:       types.HumanizeBytes(usedBytes),
			Free:       types.HumanizeBytes(freeBytes),
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read /proc/self/mountinfo: %w", err)
	}
	return drives, nil
}

// mountEntry is a parsed line from /proc/self/mountinfo.
type mountEntry struct {
	mountPoint string
	fsType     string
	device     string
}

// parseMountinfoLine parses one line of /proc/self/mountinfo.
func parseMountinfoLine(line string) (mountEntry, bool) {
	if line == "" {
		return mountEntry{}, false
	}
	before, after, ok := strings.Cut(line, " - ")
	if !ok {
		return mountEntry{}, false
	}
	prefixFields := strings.Fields(before)
	suffixFields := strings.Fields(after)
	if len(prefixFields) < 5 || len(suffixFields) < 2 {
		return mountEntry{}, false
	}
	return mountEntry{
		mountPoint: unescapeOctal(prefixFields[4]),
		fsType:     suffixFields[0],
		device:     suffixFields[1],
	}, true
}

// systemPathPrefixes are mount points that should never be treated as
// backup-worthy data volumes.
var systemPathPrefixes = []string{
	"/dev",
	"/proc",
	"/sys",
	"/run",
	"/etc",
	"/snap",
}

// isDataMountPoint checks whether a mount point represents a real data
// volume worth backing up. Excludes system paths, non-directories
// (Docker bind-mounted files like /etc/resolv.conf), and common
// container runtime artifacts.
func isDataMountPoint(mp string) bool {
	for _, prefix := range systemPathPrefixes {
		if mp == prefix || strings.HasPrefix(mp, prefix+"/") {
			return false
		}
	}

	// Skip non-directories — Docker bind-mounts individual files
	// like /etc/hostname, /etc/resolv.conf, /etc/hosts as ext4
	// mounts in /proc/self/mountinfo.
	fi, err := os.Stat(mp)
	if err != nil || !fi.IsDir() {
		return false
	}

	return true
}

// unescapeOctal replaces \xxx octal escape sequences.
func unescapeOctal(s string) string {
	if !strings.ContainsRune(s, '\\') {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) &&
			s[i+1] >= '0' && s[i+1] <= '3' &&
			s[i+2] >= '0' && s[i+2] <= '7' &&
			s[i+3] >= '0' && s[i+3] <= '7' {
			b.WriteByte((s[i+1]-'0')*64 + (s[i+2]-'0')*8 + (s[i+3] - '0'))
			i += 3
		} else {
			b.WriteByte(s[i])
		}
	}
	return b.String()
}
