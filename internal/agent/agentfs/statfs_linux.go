//go:build linux

package agentfs

import "golang.org/x/sys/unix"

func getNameLenPlatform(statfs unix.Statfs_t) uint64 {
	return uint64(statfs.Namelen)
}
