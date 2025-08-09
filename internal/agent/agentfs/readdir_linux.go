//go:build linux

package agentfs

import (
	"unsafe"

	"golang.org/x/sys/unix"
)

// mirror of Linux's struct linux_dirent64
type linuxDirent64 struct {
	Ino     uint64
	Off     int64
	Reclen  uint16
	Type    byte
	NameBuf [0]byte
}

func (r *DirReaderUnix) getdents() (int, error) {
	return unix.Getdents(r.fd, r.buf)
}

func (r *DirReaderUnix) parseDirent() (string, byte, int, error) {
	// make sure we have at least the fixed header
	hdrSize := int(unsafe.Offsetof(linuxDirent64{}.NameBuf))
	if r.bufPos+hdrSize > r.bufEnd {
		return "", 0, 0, nil
	}
	dirent := (*linuxDirent64)(unsafe.Pointer(&r.buf[r.bufPos]))
	reclen := int(dirent.Reclen)
	if reclen == 0 || r.bufPos+reclen > r.bufEnd {
		return "", 0, 0, nil
	}

	nameBytes := unsafe.Slice(
		(*byte)(unsafe.Pointer(&r.buf[r.bufPos+hdrSize])),
		reclen-hdrSize,
	)
	// find null terminator
	var l int
	for l < len(nameBytes) && nameBytes[l] != 0 {
		l++
	}
	return string(nameBytes[:l]), dirent.Type, reclen, nil
}
