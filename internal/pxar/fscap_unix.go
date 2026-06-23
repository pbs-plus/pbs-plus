//go:build unix

package pxar

import (
	"path/filepath"
	"syscall"
)

func getFilesystemCapabilities(path string) filesystemCapabilities {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return filesystemCapabilities{}
	}

	var stat syscall.Statfs_t
	if err := syscall.Statfs(absPath, &stat); err != nil {
		return filesystemCapabilities{}
	}

	fsType := getFsType(&stat)

	cap := filesystemCapabilities{}

	switch fsType {
	case "ext2", "ext3", "ext4":
		cap.supportsACLs = true
		cap.supportsPersistentACLs = true
		cap.supportsXAttrs = true
		cap.supportsChown = true

	case "xfs", "btrfs", "zfs":
		cap.supportsACLs = true
		cap.supportsPersistentACLs = true
		cap.supportsXAttrs = true
		cap.supportsChown = true

	case "tmpfs":
		cap.supportsACLs = true
		cap.supportsPersistentACLs = false
		cap.supportsXAttrs = true
		cap.supportsChown = true

	case "vfat", "exfat", "msdos":
		cap.supportsACLs = false
		cap.supportsPersistentACLs = false
		cap.supportsXAttrs = false
		cap.supportsChown = false

	case "ntfs", "fuseblk":
		cap.supportsACLs = false
		cap.supportsPersistentACLs = false
		cap.supportsXAttrs = true
		cap.supportsChown = false

	case "nfs", "nfs4":
		cap.supportsACLs = true
		cap.supportsPersistentACLs = true
		cap.supportsXAttrs = false
		cap.supportsChown = true

	case "cifs", "smb", "smbfs":
		cap.supportsACLs = false
		cap.supportsPersistentACLs = false
		cap.supportsXAttrs = false
		cap.supportsChown = false

	default:
		cap.supportsACLs = false
		cap.supportsPersistentACLs = false
		cap.supportsXAttrs = false
		cap.supportsChown = true
		cap.prefersSequentialOps = true
	}

	return cap
}

func getFsType(stat *syscall.Statfs_t) string {
	return getFsTypeFromMagic(stat)
}
