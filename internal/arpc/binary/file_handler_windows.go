//go:build windows

package binarystream

import (
	"io"

	"golang.org/x/sys/windows"
)

// WindowsHandleReader implements io.Reader directly using a Windows handle.
type WindowsHandleReader struct {
	windows.Handle
}

// Read implements io.Reader. It calls windows.ReadFile to fill p.
// If the end of the file is reached, it returns io.EOF.
func (r *WindowsHandleReader) Read(p []byte) (int, error) {
	var bytesRead uint32
	err := windows.ReadFile(r.Handle, p, &bytesRead, nil)
	// If we hit end-of-file, windows.ReadFile may return
	// windows.ERROR_HANDLE_EOF. In that case, return io.EOF.
	if err != nil {
		if err == windows.ERROR_HANDLE_EOF {
			// Some data might have been read.
			if bytesRead > 0 {
				return int(bytesRead), nil
			}
			return int(bytesRead), io.EOF
		}
		return int(bytesRead), err
	}
	return int(bytesRead), nil
}
