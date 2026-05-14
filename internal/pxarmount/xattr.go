package pxarmount

import "strings"

// isACLXattr reports whether the xattr name is ACL-related.
// Covers Linux POSIX ACLs and Windows NT ACLs (SMB acl_xattr module).
func isACLXattr(attr string) bool {
	switch attr {
	case "system.posix_acl_access",
		"system.posix_acl_default",
		"security.NTACL",
		"security.XDACL",
		"security.ACL":
		return true
	}
	return strings.HasPrefix(attr, "system.posix_acl_") ||
		strings.HasPrefix(attr, "security.ntfs_") ||
		strings.HasPrefix(attr, "user.NTACL")
}
