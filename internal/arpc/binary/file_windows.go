//go:build windows

package binarystream

import (
	"github.com/xtaci/smux"
	"golang.org/x/sys/windows"
)

// SendData is a Windows-specific function that creates a WindowsHandleReader
// over the provided handle and then delegates to the OS-independent
// SendDataFromReader function.
func SendData(handle windows.Handle, length int, stream *smux.Stream) error {
	reader := &WindowsHandleReader{Handle: handle}
	return SendDataFromReader(reader, length, stream)
}
