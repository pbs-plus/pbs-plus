//go:build freebsd

package agentfs

import (
	"syscall"
	"unsafe"
)

func (r *DirReaderUnix) getdents() (int, error) {
	for {
		n, err := syscall.Getdirentries(r.fd, r.buf, &r.basep)
		if err == syscall.EINTR || err == syscall.EAGAIN {
			continue
		}
		return n, err
	}
}

func (r *DirReaderUnix) parseDirent() ([]byte, byte, int, bool, error) {
	nameOff := int(unsafe.Offsetof(syscall.Dirent{}.Name))
	if r.bufPos+nameOff > r.bufEnd {
		return nil, 0, 0, false, nil
	}
	de := (*syscall.Dirent)(unsafe.Pointer(&r.buf[r.bufPos]))
	reclen := int(de.Reclen)
	if reclen < nameOff || r.bufPos+reclen > r.bufEnd {
		return nil, 0, 0, false, syscall.EBADF
	}

	namelen := int(de.Namlen)
	if namelen < 0 {
		return nil, 0, 0, false, syscall.EBADF
	}
	if namelen > len(de.Name) {
		namelen = len(de.Name)
	}
	raw := (*[256]byte)(unsafe.Pointer(&de.Name[0]))[:namelen:namelen]
	return raw, byte(de.Type), reclen, true, nil
}
