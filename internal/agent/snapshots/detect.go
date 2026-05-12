package snapshots

import (
	"fmt"
	"runtime"

	"golang.org/x/sys/unix"
)

// detectFilesystem returns the normalized filesystem type for a path.
//
// Uses statfs(2) to get the filesystem magic number directly from the
// kernel — works on any path (not just mount points), handles symlinks,
// and is immune to /proc/mounts escaping issues.
func detectFilesystem(path string) (string, error) {
	switch runtime.GOOS {
	case "linux":
		var st unix.Statfs_t
		if err := unix.Statfs(path, &st); err != nil {
			return "", fmt.Errorf("statfs %s: %w", path, err)
		}
		fsType := fsTypeFromMagic(int64(st.Type))
		if fsType == "" {
			return "", fmt.Errorf("unknown filesystem magic 0x%X at %s", st.Type, path)
		}
		return fsType, nil

	case "darwin":
		return detectFilesystemDarwin(path)

	case "windows":
		// Windows reports everything as NTFS; ReFS volumes are
		// indistinguishable at this level without an additional API call.
		return "ntfs", nil

	default:
		return "", fmt.Errorf("unsupported operating system: %s", runtime.GOOS)
	}
}

// detectFilesystemDarwin uses diskutil for macOS filesystem detection.
func detectFilesystemDarwin(path string) (string, error) {
	return "", fmt.Errorf("darwin filesystem detection not implemented")
}

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
	case 0x5346544E: // NTFS_SB_MAGIC
		return "ntfs"

	// FUSE (ntfs-3g via fuse, exfat-fuse, etc.).
	// We can't distinguish the actual FS behind FUSE from statfs alone.
	// The manager normalizes "fuseblk" → "ntfs" for the common case.
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
