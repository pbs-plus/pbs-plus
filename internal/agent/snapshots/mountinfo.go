//go:build linux

package snapshots

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// mountEntry holds a parsed line from /proc/self/mountinfo.
type mountEntry struct {
	mountPoint string // field 5, unescaped
	device     string // field 10 (mount source after "-" separator)
	fsType     string // field 9 (filesystem type after "-" separator)
}

// resolveMountPoint returns the mount entry for the mount that contains
// the given path. Uses longest-prefix matching against /proc/self/mountinfo
// so subdirectories of a mount resolve to their containing mount.
func resolveMountPoint(path string) (mountEntry, error) {
	path = strings.TrimRight(path, "/")
	if path == "" {
		path = "/"
	}

	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return mountEntry{}, fmt.Errorf("open /proc/self/mountinfo: %w", err)
	}
	defer func() { _ = f.Close() }()

	var best mountEntry
	bestLen := -1

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		entry, ok := parseMountinfoLine(scanner.Text())
		if !ok {
			continue
		}

		mp := strings.TrimRight(entry.mountPoint, "/")
		if mp == "" {
			mp = "/"
		}

		// Match: the mount point must be a prefix of path, and path
		// must either equal the mount point or have a '/' after the
		// prefix (to avoid /mnt matching /mnt2).
		if !strings.HasPrefix(path, mp) {
			continue
		}
		if len(path) > len(mp) && path[len(mp)] != '/' {
			continue
		}

		if len(mp) > bestLen {
			best = entry
			bestLen = len(mp)
		}
	}

	if err := scanner.Err(); err != nil {
		return mountEntry{}, fmt.Errorf("read /proc/self/mountinfo: %w", err)
	}

	if bestLen < 0 {
		return mountEntry{}, fmt.Errorf("no mount found containing %s", path)
	}
	return best, nil
}

// parseMountinfoLine parses one line of /proc/self/mountinfo.
//
// Format (fields separated by spaces, escape sequences are \xxx octal):
//
//	36 35 98:0 /mnt /mnt rw,relatime shared:1 - ext4 /dev/sda1 rw
//
// Fields 1-4: mount ID, parent ID, major:minor, root
// Field  5:   mount point
// Field  6:   mount options
// Field  7+:  optional tagged fields (e.g. shared:N, master:N)
// Separator:  "-"
// Field  after sep: filesystem type
// Field  after sep+1: mount source (device)
// Field  after sep+2: super options
func parseMountinfoLine(line string) (mountEntry, bool) {
	if line == "" {
		return mountEntry{}, false
	}

	// Find the " - " separator that marks the end of optional fields.
	before, after, ok := strings.Cut(line, " - ")
	if !ok {
		return mountEntry{}, false
	}

	prefix := before
	suffix := after // after " - "

	// Split prefix into space-separated fields.
	prefixFields := strings.Fields(prefix)
	if len(prefixFields) < 5 {
		return mountEntry{}, false
	}

	suffixFields := strings.Fields(suffix)
	if len(suffixFields) < 2 {
		return mountEntry{}, false
	}

	return mountEntry{
		mountPoint: unescapeOctal(prefixFields[4]),
		fsType:     suffixFields[0],
		device:     suffixFields[1],
	}, true
}

// unescapeOctal replaces \xxx octal escape sequences (used in
// /proc/self/mountinfo for spaces, tabs, newlines, backslashes)
// with the corresponding byte.
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

// blockDeviceForPath returns the block device backing the mount that
// contains the given path. Handles subdirectories, escaped mount points,
// and all mount types (raw partitions, LVM mapper devices, etc.).
func blockDeviceForPath(path string) (string, error) {
	entry, err := resolveMountPoint(path)
	if err != nil {
		return "", err
	}

	dev := entry.device
	if dev == "" || !strings.HasPrefix(dev, "/dev/") {
		return "", fmt.Errorf("mount %s is not backed by a block device (device=%q)", entry.mountPoint, dev)
	}
	return dev, nil
}
