//go:build unix && !freebsd

package agentfs

import "golang.org/x/sys/unix"

func isNoAttr(err error) bool {
	if err == nil {
		return false
	}
	return err == unix.ENODATA || err == unix.EOPNOTSUPP
}
