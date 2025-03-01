//go:build windows

package vssfs

import (
	"fmt"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Win32StreamId represents the WIN32_STREAM_ID structure.
type Win32StreamId struct {
	StreamID   uint32 // Stream identifier (e.g. BackupData)
	Attributes uint32 // Stream attributes
	Size       uint64 // Size of the stream in bytes
	NameSize   uint32 // Size of the stream name (if any)
}

var (
	modKernel32    = windows.NewLazySystemDLL("kernel32.dll")
	procBackupRead = modKernel32.NewProc("BackupRead")
	procBackupSeek = modKernel32.NewProc("BackupSeek")
)

// backupRead is a thin wrapper over the Win32 BackupRead function.
// If abort is true then the backup context is freed.
func backupRead(handle windows.Handle, buf []byte, context *uintptr, abort bool, processSecurity bool) (uint32, error) {
	var bytesRead uint32
	abortFlag := uint32(0)
	if abort {
		abortFlag = 1
	}
	procSecFlag := uint32(0)
	if processSecurity {
		procSecFlag = 1
	}
	var bufPtr uintptr
	if len(buf) > 0 {
		bufPtr = uintptr(unsafe.Pointer(&buf[0]))
	}
	r1, _, e1 := syscall.SyscallN(procBackupRead.Addr(), 7,
		uintptr(handle),
		bufPtr,
		uintptr(uint32(len(buf))),
		uintptr(unsafe.Pointer(&bytesRead)),
		uintptr(abortFlag),
		uintptr(procSecFlag),
		uintptr(unsafe.Pointer(context)),
		0,
		0)
	if r1 == 0 {
		if e1 != 0 {
			return bytesRead, error(e1)
		}
		return bytesRead, fmt.Errorf("BackupRead failed")
	}
	return bytesRead, nil
}

// backupSeek is a thin wrapper over the Win32 BackupSeek function.
// It skips 'skip' bytes in the backup stream and returns the number of bytes skipped.
func backupSeek(handle windows.Handle, skip uint64, context *uintptr) (uint64, error) {
	lowSkip := uint32(skip & 0xFFFFFFFF)
	highSkip := uint32(skip >> 32)
	var lowSkipped, highSkipped uint32
	r1, _, e1 := syscall.SyscallN(procBackupSeek.Addr(), 6,
		uintptr(handle),
		uintptr(lowSkip),
		uintptr(highSkip),
		uintptr(unsafe.Pointer(&lowSkipped)),
		uintptr(unsafe.Pointer(&highSkipped)),
		uintptr(unsafe.Pointer(context)))
	if r1 == 0 {
		if e1 != 0 {
			return 0, error(e1)
		}
		return 0, fmt.Errorf("BackupSeek failed")
	}
	return (uint64(highSkipped) << 32) | uint64(lowSkipped), nil
}
