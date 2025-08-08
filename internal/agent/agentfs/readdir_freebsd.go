//go:build freebsd

package agentfs

import (
	"syscall"
	"unsafe"
)

// freebsdDirent matches FreeBSD's struct dirent
type freebsdDirent struct {
	Fileno uint32
	Reclen uint16
	Type   uint8
	Namlen uint8
	Name   [256]byte // d_name is up to 255 chars + null terminator
}

func getdents(fd int, buf []byte) (int, error) {
	// On FreeBSD, syscall.Getdirentries returns (n int, basep uintptr, err error)
	n, _, err := syscall.Syscall(
		syscall.SYS_GETDIRENTRIES,
		uintptr(fd),
		uintptr(unsafe.Pointer(&buf[0])),
		uintptr(len(buf)),
	)
	if err != 0 {
		return int(n), err
	}
	return int(n), nil
}

func (r *DirReaderUnix) parseDirent() (name string, typ byte, reclen int, err error) {
	if r.bufPos+int(unsafe.Sizeof(freebsdDirent{})) > r.bufEnd {
		return "", 0, 0, nil
	}

	dirent := (*freebsdDirent)(unsafe.Pointer(&r.buf[r.bufPos]))
	if dirent.Reclen == 0 || r.bufPos+int(dirent.Reclen) > r.bufEnd {
		return "", 0, 0, nil
	}

	name = string(dirent.Name[:dirent.Namlen])
	return name, dirent.Type, int(dirent.Reclen), nil
}
