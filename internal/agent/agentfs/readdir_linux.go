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

var linuxHdrSize = int(unsafe.Offsetof(linuxDirent64{}.NameBuf))

func (r *DirReaderUnix) getdents() (int, error) {
	for {
		n, err := unix.Getdents(r.fd, r.buf)
		if err == unix.EINTR || err == unix.EAGAIN {
			continue
		}
		return n, err
	}
}

// parseDirent parses one record at r.bufPos.
// Returns:
// - name bytes (not including trailing NUL)
// - type byte
// - reclen
// - ok=false if insufficient data for a full entry (caller should refill/advance)
// - perr for malformed entries
func (r *DirReaderUnix) parseDirent() ([]byte, byte, int, bool, error) {
	// Ensure fixed header exists
	if r.bufPos+linuxHdrSize > r.bufEnd {
		return nil, 0, 0, false, nil
	}

	dirent := (*linuxDirent64)(unsafe.Pointer(&r.buf[r.bufPos]))
	reclen := int(dirent.Reclen)
	if reclen < linuxHdrSize || r.bufPos+reclen > r.bufEnd {
		// Malformed entry; signal error to caller
		return nil, 0, 0, false, unix.EBADF
	}

	nameStart := r.bufPos + linuxHdrSize
	nameEnd := r.bufPos + reclen

	// Scan until NUL
	b := r.buf[nameStart:nameEnd]
	i := 0
	for i < len(b) && b[i] != 0 {
		i++
	}
	return b[:i], dirent.Type, reclen, true, nil
}
