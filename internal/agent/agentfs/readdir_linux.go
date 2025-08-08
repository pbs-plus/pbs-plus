//go:build linux

package agentfs

import (
	"syscall"
	"unsafe"
)

// linuxDirent64 matches the Linux getdents64 struct
type linuxDirent64 struct {
	Ino     uint64
	Off     int64
	Reclen  uint16
	Type    byte
	NameBuf [0]byte
}

func getdents(fd int, buf []byte) (int, error) {
	return syscall.Getdents(fd, buf)
}

func (r *DirReaderUnix) parseDirent() (name string, typ byte, reclen int, err error) {
	if r.bufPos+19 > r.bufEnd { // Minimum dirent64 size
		return "", 0, 0, nil
	}

	dirent := (*linuxDirent64)(unsafe.Pointer(&r.buf[r.bufPos]))
	if dirent.Reclen == 0 || r.bufPos+int(dirent.Reclen) > r.bufEnd {
		return "", 0, 0, nil
	}

	nameBytes := unsafe.Slice(
		(*byte)(unsafe.Add(unsafe.Pointer(dirent), unsafe.Offsetof(dirent.NameBuf))),
		int(dirent.Reclen)-int(unsafe.Offsetof(dirent.NameBuf)),
	)

	// Find null terminator
	nameLen := 0
	for nameLen < len(nameBytes) && nameBytes[nameLen] != 0 {
		nameLen++
	}

	return string(nameBytes[:nameLen]), dirent.Type, int(dirent.Reclen), nil
}
