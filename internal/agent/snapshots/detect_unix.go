//go:build unix

package snapshots

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// detectFilesystem returns the normalized filesystem type for a path
// using unix.Statfs (Unix-specific).
func detectFilesystem(path string) (string, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return "", fmt.Errorf("statfs %s: %w", path, err)
	}
	fsType := fsTypeFromMagic(int64(st.Type))
	if fsType == "" {
		return "", fmt.Errorf("unknown filesystem magic 0x%X at %s", st.Type, path)
	}
	return fsType, nil
}
