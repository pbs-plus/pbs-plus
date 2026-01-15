//go:build freebsd

package agentfs

import "golang.org/x/sys/unix"

func isNoAttr(err error) bool {
	if err == nil {
		return false
	}
	return err == unix.ENOATTR || err == unix.EOPNOTSUPP
}
