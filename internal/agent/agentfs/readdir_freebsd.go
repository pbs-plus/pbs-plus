//go:build freebsd

package agentfs

import (
	"syscall"
	"unsafe"
)

func (r *DirReaderUnix) getdents() (int, error) {
	// FreeBSD’s native syscall: Getdirentries(fd, buf, *basep)
	n, err := syscall.Getdirentries(r.fd, r.buf, &r.basep)
	return n, err
}

func (r *DirReaderUnix) parseDirent() (string, byte, int, error) {
	// ensure at least up to Name field
	nameOff := unsafe.Offsetof(syscall.Dirent{}.Name)
	if r.bufPos+int(nameOff) > r.bufEnd {
		return "", 0, 0, nil
	}
	dirent := (*syscall.Dirent)(unsafe.Pointer(&r.buf[r.bufPos]))
	reclen := int(dirent.Reclen)
	if reclen == 0 || r.bufPos+reclen > r.bufEnd {
		return "", 0, 0, nil
	}

	// Namlen is the length of the name (no trailing NUL)
	namelen := int(dirent.Namlen)
	if namelen > len(dirent.Name) {
		namelen = len(dirent.Name)
	}
	// Dirent.Name is [256]int8
	raw := (*[256]byte)(unsafe.Pointer(&dirent.Name[0]))
	return string(raw[:namelen]), dirent.Type, reclen, nil
}
