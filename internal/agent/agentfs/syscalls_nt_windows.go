//go:build windows

package agentfs

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

var (
	ntdll                      = syscall.NewLazyDLL("ntdll.dll")
	ntCreateFile               = ntdll.NewProc("NtCreateFile")
	ntQueryDirectoryFile       = ntdll.NewProc("NtQueryDirectoryFile")
	ntClose                    = ntdll.NewProc("NtClose")
	rtlInitUnicodeString       = ntdll.NewProc("RtlInitUnicodeString")
	procNtQueryInformationFile = ntdll.NewProc("NtQueryInformationFile")
	procRtlNtStatusToDosError  = ntdll.NewProc("RtlNtStatusToDosError")
)

const fileNetworkOpenInformationClass = 34 // FileNetworkOpenInformation

const (
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
	// Convert NTSTATUS to Win32 error for consistency with other Windows calls.
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

func ntCreateFileCall(handle *uintptr, objectAttributes *ObjectAttributes, ioStatusBlock *IoStatusBlock) error {
	status, _, _ := ntCreateFile.Call(
		uintptr(unsafe.Pointer(handle)),
		FILE_LIST_DIRECTORY|syscall.SYNCHRONIZE,
		uintptr(unsafe.Pointer(objectAttributes)),
		uintptr(unsafe.Pointer(ioStatusBlock)),
		0,
		0,
		FILE_SHARE_READ|FILE_SHARE_WRITE|FILE_SHARE_DELETE,
		OPEN_EXISTING,
		FILE_DIRECTORY_FILE|FILE_SYNCHRONOUS_IO_NONALERT,
		0,
		0,
	)
	if status != 0 {
		return fmt.Errorf(
			"NtCreateFile failed with status: %x",
			status,
		)
	}

	return nil
}

func ntDirectoryCall(handle uintptr, ioStatusBlock *IoStatusBlock, buffer []byte, restartScan bool) error {
	status, _, _ := ntQueryDirectoryFile.Call(
		handle,
		0,
		0,
		0,
		uintptr(unsafe.Pointer(&ioStatusBlock)),
		uintptr(unsafe.Pointer(&buffer[0])),
		uintptr(len(buffer)),
		uintptr(1), // FileInformationClass: FileDirectoryInformation
		uintptr(0), // ReturnSingleEntry: FALSE (batch)
		0,          // FileName: NULL
		uintptr(boolToInt(restartScan)),
	)

	switch status {
	case 0:
		// success
	case STATUS_NO_MORE_FILES:
		return os.ErrProcessDone
	case STATUS_PENDING:
		// Since we opened with synchronous I/O, this is unexpected but handle gracefully.
		// Treat as retryable no-op; caller can call NextBatch again.
		return os.ErrExist
	default:
		return fmt.Errorf("NtQueryDirectoryFile failed with status: %x", status)
	}

	return nil
}
