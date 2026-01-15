//go:build unix && !freebsd

package pxar

import "golang.org/x/sys/unix"

func applyImmutable(path string, set bool) error {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)

	flags, err := unix.IoctlGetUint32(fd, unix.FS_IOC_GETFLAGS)
	if err != nil {
		return err
	}

	if set {
		flags |= FS_IMMUTABLE_FL
	} else {
		flags &^= FS_IMMUTABLE_FL
	}

	return unix.IoctlSetInt(fd, unix.FS_IOC_SETFLAGS, int(flags))
}
