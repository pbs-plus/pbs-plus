//go:build linux

package pxar

import "syscall"

func getFsTypeFromMagic(stat *syscall.Statfs_t) string {
	// Linux filesystem magic numbers from statfs(2)
	switch stat.Type {
	case 0xEF53:
		return "ext4"
	case 0x58465342:
		return "xfs"
	case 0x9123683E:
		return "btrfs"
	case 0x2FC12FC1:
		return "zfs"
	case 0x01021994:
		return "tmpfs"
	case 0x4d44:
		return "msdos"
	case 0x4006:
		return "vfat"
	case 0x2011BAB0:
		return "exfat"
	case 0x5346544e:
		return "ntfs"
	case 0x65735546:
		return "fuseblk"
	case 0x6969:
		return "nfs"
	case 0xFF534D42:
		return "cifs"
	default:
		return "unknown"
	}
}
