//go:build windows

package vssfs

import (
	"io"

	"golang.org/x/sys/windows"
)

type AtReader struct {
	handle windows.Handle
	offset int64
}

func (r *AtReader) Read(p []byte) (int, error) {
	var ov windows.Overlapped
	// Set up the OVERLAPPED structure with the desired offset.
	ov.Offset = uint32(r.offset)
	ov.OffsetHigh = uint32(r.offset >> 32)
	var bytesRead uint32
	err := windows.ReadFile(r.handle, p, &bytesRead, &ov)
	if err != nil && err != windows.ERROR_HANDLE_EOF {
		return int(bytesRead), err
	}
	r.offset += int64(bytesRead)
	if bytesRead == 0 {
		return int(bytesRead), io.EOF
	}
	return int(bytesRead), nil
}
