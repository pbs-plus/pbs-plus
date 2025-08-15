//go:build windows

package agentfs

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

// NT native imports and structs

var (
	procNtQueryInformationFile = ntdll.NewProc("NtQueryInformationFile")
	procRtlNtStatusToDosError  = ntdll.NewProc("RtlNtStatusToDosError")
)

type ioStatusBlock struct {
	Status      uintptr // NTSTATUS (use uintptr to match pointer size)
	Information uintptr
}

const fileNetworkOpenInformationClass = 34 // FileNetworkOpenInformation

type fileNetworkOpenInformation struct {
	CreationTime   int64
	LastAccessTime int64
	LastWriteTime  int64
	ChangeTime     int64
	AllocationSize int64
	EndOfFile      int64
	FileAttributes uint32
	_              uint32 // alignment
}

func ntStatusToError(status uintptr) error {
	if status == 0 {
		return nil
	}
	// Convert NTSTATUS to Win32 error for consistency with other Windows calls.
	r0, _, _ := procRtlNtStatusToDosError.Call(status)
	return windows.Errno(r0)
}

func ntQueryFileNetworkOpenInformation(
	h windows.Handle,
	out *fileNetworkOpenInformation,
) error {
	var iosb ioStatusBlock
	buf := (*byte)(unsafe.Pointer(out))
	bufLen := uint32(unsafe.Sizeof(*out))
	r0, _, _ := procNtQueryInformationFile.Call(
		uintptr(h),
		uintptr(unsafe.Pointer(&iosb)),
		uintptr(unsafe.Pointer(buf)),
		uintptr(bufLen),
		uintptr(fileNetworkOpenInformationClass),
	)
	return ntStatusToError(r0)
}
