//go:build freebsd

package agentfs

import (
	"syscall"
	"unsafe"
)

// freebsdDirent matches FreeBSD's dirent structure
type freebsdDirent struct {
	Fileno uint32
	Reclen uint16
	Type   uint8
	Namlen uint8
	Name   [256]int8
}

func getdents(fd int, buf []byte) (int, error) {
	n, _, errno := syscall.RawSyscall(
		syscall.SYS_GETDIRENTRIES,
		uintptr(fd),
		uintptr(unsafe.Pointer(&buf[0])),
		uintptr(len(buf)),
	)
	if errno != 0 {
		return 0, errno
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

	// Convert name from int8 array to string
	nameBytes := make([]byte, dirent.Namlen)
	for i := uint8(0); i < dirent.Namlen; i++ {
		nameBytes[i] = byte(dirent.Name[i])
	}

	return string(nameBytes), dirent.Type, int(dirent.Reclen), nil
}
