//go:build unix && !linux

package agentfs

import "golang.org/x/sys/unix"

func getNameLenPlatform(statfs unix.Statfs_t) uint64 {
	// FreeBSD doesn't provide Namelen in statfs
	return 255
}
