//go:build windows

package agentfs

import (
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

var (
	ntdll                      = syscall.NewLazyDLL("ntdll.dll")
	procNtQueryInformationFile = ntdll.NewProc("NtQueryInformationFile")
	procRtlNtStatusToDosError  = ntdll.NewProc("RtlNtStatusToDosError")
)

const fileNetworkOpenInformationClass = 34

const (
	STATUS_SUCCESS               = 0x00000000
	FILE_LIST_DIRECTORY          = 0x0001
	FILE_SHARE_READ              = 0x00000001
	FILE_SHARE_WRITE             = 0x00000002
	FILE_SHARE_DELETE            = 0x00000004
	OPEN_EXISTING                = 3
	FILE_DIRECTORY_FILE          = 0x00000001
	FILE_SYNCHRONOUS_IO_NONALERT = 0x00000020
	OBJ_CASE_INSENSITIVE         = 0x00000040
	STATUS_NO_MORE_FILES         = 0x80000006
	STATUS_PENDING               = 0x00000103
)

func ntStatusToError(status uintptr) error {
	if status == 0 {
		return nil
	}
	r0, _, _ := procRtlNtStatusToDosError.Call(status)
	return windows.Errno(r0)
}

func ntQueryFileNetworkOpenInformation(
	h windows.Handle,
	out *fileNetworkOpenInformation,
) error {
	var iosb IoStatusBlock
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
