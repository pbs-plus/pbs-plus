//go:build windows

package vssfs

import (
	"io"
	"os"

	"golang.org/x/sys/windows"
)

type WinHandleReader struct {
	handle windows.Handle
	closed bool
}

func NewWinHandleReader(handle windows.Handle) *WinHandleReader {
	return &WinHandleReader{handle: handle}
}

func (r *WinHandleReader) Read(p []byte) (n int, err error) {
	if r.closed {
		return 0, os.ErrClosed
	}

	var bytesRead uint32
	err = windows.ReadFile(r.handle, p, &bytesRead, nil)
	if err != nil {
		return 0, err
	}

	// Return EOF when no bytes are read
	if bytesRead == 0 {
		return 0, io.EOF
	}

	return int(bytesRead), nil
}

// Close implements io.Closer
func (r *WinHandleReader) Close() error {
	if r.closed {
		return os.ErrClosed
	}
	r.closed = true
	return windows.CloseHandle(r.handle)
}
