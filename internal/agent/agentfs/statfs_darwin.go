//go:build darwin

package agentfs

import "golang.org/x/sys/unix"

func getNameLenPlatform(statfs unix.Statfs_t) uint64 {
	// macOS may or may not have this field depending on the version
	// Check if the field exists and return appropriate value
	return 255 // Default for HFS+/APFS
}
