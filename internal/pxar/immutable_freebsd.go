//go:build freebsd

package pxar

import "golang.org/x/sys/unix"

func applyImmutable(path string, set bool) error {
	// On FreeBSD, immutable is controlled via flags (UF_IMMUTABLE or SF_IMMUTABLE)
	// We use UF_IMMUTABLE (User immutable) which is the closest equivalent
	// to Linux's FS_IMMUTABLE_FL.
	const UF_IMMUTABLE = 0x0002

	// Get current flags
	var stat unix.Stat_t
	if err := unix.Stat(path, &stat); err != nil {
		return err
	}

	flags := uint32(stat.Flags)

	if set {
		flags |= UF_IMMUTABLE
	} else {
		flags &^= UF_IMMUTABLE
	}

	return unix.Chflags(path, int(flags))
}
