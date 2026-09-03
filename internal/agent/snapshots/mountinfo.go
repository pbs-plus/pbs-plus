package snapshots

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// MountEntry is one line of /proc/self/mountinfo.
type MountEntry struct {
	Root       string
	MountPoint string
	Options    string
	FSType     string
	Source     string
	SuperOpts  string
}

// Mounts returns every mount visible to this process, in kernel order, so later entries shadow earlier ones on the same mount point.
func Mounts() ([]MountEntry, error) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return nil, fmt.Errorf("failed to open /proc/self/mountinfo: %w", err)
	}
	defer f.Close()

	var entries []MountEntry
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		entry, ok := parseMountInfoLine(scanner.Text())
		if ok {
			entries = append(entries, entry)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read /proc/self/mountinfo: %w", err)
	}
	return entries, nil
}

func parseMountInfoLine(line string) (MountEntry, bool) {
	before, after, ok := strings.Cut(line, " - ")
	if !ok {
		return MountEntry{}, false
	}

	left := strings.Fields(before)
	right := strings.Fields(after)
	if len(left) < 6 || len(right) < 2 {
		return MountEntry{}, false
	}

	entry := MountEntry{
		Root:       unescapeOctal(left[3]),
		MountPoint: unescapeOctal(left[4]),
		Options:    left[5],
		FSType:     right[0],
		Source:     unescapeOctal(right[1]),
	}
	if len(right) > 2 {
		entry.SuperOpts = right[2]
	}
	return entry, true
}

// unescapeOctal decodes the \040 style escapes the kernel writes for space, tab, newline and backslash.
func unescapeOctal(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			var v byte
			ok := true
			for _, c := range []byte(s[i+1 : i+4]) {
				if c < '0' || c > '7' {
					ok = false
					break
				}
				v = v<<3 | (c - '0')
			}
			if ok {
				b.WriteByte(v)
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// FindMount returns the mount holding path: the longest mount point that prefixes it.
// A target like /var/lib/postgresql is rarely a mount point itself, so an exact match misses the volume the data lives on.
func FindMount(path string) (MountEntry, error) {
	entries, err := Mounts()
	if err != nil {
		return MountEntry{}, err
	}

	resolved := path
	if abs, err := filepath.Abs(path); err == nil {
		resolved = abs
	}
	if real, err := filepath.EvalSymlinks(resolved); err == nil {
		resolved = real
	}
	return pickMount(entries, resolved)
}

// pickMount takes the last entry on the longest matching mount point, which is where path resolution actually lands when mounts shadow each other.
func pickMount(entries []MountEntry, path string) (MountEntry, error) {
	resolved := filepath.Clean(path)

	best := MountEntry{}
	bestLen := -1
	for _, entry := range entries {
		mp := filepath.Clean(entry.MountPoint)
		if mp == "/" {
			if !strings.HasPrefix(resolved, "/") {
				continue
			}
		} else if mp != resolved && !strings.HasPrefix(resolved, mp+"/") {
			continue
		}
		if len(mp) >= bestLen {
			best = entry
			bestLen = len(mp)
		}
	}
	if bestLen < 0 {
		return MountEntry{}, fmt.Errorf("no mount found for %q", path)
	}
	return best, nil
}
