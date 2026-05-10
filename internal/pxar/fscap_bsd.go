//go:build !linux && !windows

package pxar

import "syscall"

func getFsTypeFromMagic(stat *syscall.Statfs_t) string {
	fsTypeName := make([]byte, 0, len(stat.Fstypename))
	for i := 0; i < len(stat.Fstypename); i++ {
		if stat.Fstypename[i] == 0 {
			break
		}
		fsTypeName = append(fsTypeName, byte(stat.Fstypename[i]))
	}

	fsType := string(fsTypeName)

	switch fsType {
	case "ufs", "ffs":
		return "ufs"
	case "zfs":
		return "zfs"
	case "ext2fs":
		return "ext2"
	case "msdosfs":
		return "vfat"
	case "ntfs":
		return "ntfs"
	case "nfs":
		return "nfs"
	case "apfs":
		return "apfs"
	case "hfs":
		return "hfs"
	case "msdos":
		return "vfat"
	case "exfat":
		return "exfat"
	case "smbfs":
		return "cifs"
	default:
		return fsType
	}
}
