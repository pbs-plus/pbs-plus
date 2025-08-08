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
	var basep uintptr
	n, _, errno := syscall.Syscall6(
		syscall.SYS_GETDIRENTRIES,
		uintptr(fd),
		uintptr(unsafe.Pointer(&buf[0])),
		uintptr(len(buf)),
		uintptr(unsafe.Pointer(&basep)),
		0, 0,
	)
	if errno != 0 {
		return int(n), errno
	}
	return int(n), nil
}

func (r *DirReaderUnix) parseDirent() (name string, typ byte, reclen int, err error) {
	// Need at least 2 bytes for reclen field
	if r.bufPos+2 > r.bufEnd {
		return "", 0, 0, nil
	}

	dirent := (*freebsdDirent)(unsafe.Pointer(&r.buf[r.bufPos]))
	reclen = int(dirent.Reclen)

	if reclen <= 0 || r.bufPos+reclen > r.bufEnd {
		// Invalid or incomplete record
		return "", 0, 0, nil
	}

	// Extract name
	name = string(dirent.Name[:dirent.Namlen])
	return name, dirent.Type, reclen, nil
}
