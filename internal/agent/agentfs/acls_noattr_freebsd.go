//go:build freebsd

package agentfs

import "golang.org/x/sys/unix"

// isNoAttr checks for missing extended attributes in a cross-platform way.
func isNoAttr(err error) bool {
	if err == nil {
		return false
	}
	return err == unix.ENOATTR || err == unix.EOPNOTSUPP
}
