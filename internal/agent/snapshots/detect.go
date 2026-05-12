package snapshots

// fsTypeFromMagic maps a statfs f_type to a normalized filesystem name.
//
// Only includes filesystems the snapshot manager has provider chains for.
// Unknown magic numbers return "" so the caller gets a clear error.
func fsTypeFromMagic(magic int64) string {
	switch magic {
	// ext2/ext3/ext4 all share this magic.
	case 0xEF53:
		return "ext4"

	// XFS.
	case 0x58465342:
		return "xfs"

	// BTRFS.
	case 0x9123683E:
		return "btrfs"

	// ZFS.
	case 0x2FC12FC1:
		return "zfs"

	// NTFS (Linux ntfs3 / ntfs-3g).
	case 0x5346544E:
		return "ntfs"

	// FUSE (ntfs-3g via fuse, exfat-fuse, etc.).
	// Normalized to "ntfs" by the manager for the common case.
	case 0x65735546:
		return "fuseblk"

	// FAT32.
	case 0x4D44:
		return "fat32"

	// exFAT.
	case 0x2011BAB0:
		return "exfat"

	// HFS+.
	case 0x482B:
		return "hfs+"

	// APFS (macOS).
	case 0x41505346:
		return "apfs"

	default:
		return ""
	}
}
