//go:build freebsd

package agentfs

import (
	"syscall"
	"unsafe"
)

func getdents(fd int, buf []byte) (int, error) {
	var basep int64 = 0 // FreeBSD requires this parameter

	n, _, errno := syscall.Syscall6(
		syscall.SYS_GETDIRENTRIES,
		uintptr(fd),
		uintptr(unsafe.Pointer(&buf[0])),
		uintptr(len(buf)),
		uintptr(unsafe.Pointer(&basep)), // FreeBSD needs this basep parameter
		0,
		0,
	)

	if errno != 0 {
		return 0, errno
	}
	return int(n), nil
}

func (r *DirReaderUnix) parseDirent() (name string, typ byte, reclen int, err error) {
	// FreeBSD dirent structure (from the search results):
	// struct dirent {
	//     ino_t d_fileno;      /* 8 bytes on 64-bit */
	//     off_t d_off;         /* 8 bytes */
	//     uint16_t d_reclen;   /* 2 bytes */
	//     uint8_t d_type;      /* 1 byte */
	//     uint16_t d_namlen;   /* 2 bytes */
	//     char d_name[];       /* variable length, null terminated */
	// };

	minSize := 8 + 8 + 2 + 1 + 2 // fileno + off + reclen + type + namlen
	if r.bufPos+minSize > r.bufEnd {
		return "", 0, 0, nil
	}

	// Parse fields manually from buffer
	// d_fileno (8 bytes) - skip
	// d_off (8 bytes) - skip
	reclen = int(*(*uint16)(unsafe.Pointer(&r.buf[r.bufPos+16])))  // d_reclen at offset 16
	typ = r.buf[r.bufPos+18]                                       // d_type at offset 18
	namlen := int(*(*uint16)(unsafe.Pointer(&r.buf[r.bufPos+19]))) // d_namlen at offset 19

	if reclen == 0 || r.bufPos+reclen > r.bufEnd {
		return "", 0, 0, nil
	}

	// Name starts at offset 21 (after the fixed fields)
	nameStart := r.bufPos + 21
	nameEnd := nameStart + namlen

	if nameEnd > r.bufEnd {
		return "", 0, 0, nil
	}

	// Extract name (null-terminated)
	name = string(r.buf[nameStart:nameEnd])

	return name, typ, reclen, nil
}
