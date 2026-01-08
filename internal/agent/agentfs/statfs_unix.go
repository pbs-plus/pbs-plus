//go:build unix && !linux

package agentfs

import "golang.org/x/sys/unix"

func getNameLenPlatform(statfs unix.Statfs_t) uint64 {
	// FreeBSD doesn't provide Namelen in statfs
	// Return a reasonable default (255 is common for most filesystems)
	return 255
}
