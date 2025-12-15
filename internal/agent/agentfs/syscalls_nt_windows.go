//go:build windows

package agentfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

var (
	ntdll                      = syscall.NewLazyDLL("ntdll.dll")
	ntCreateFile               = ntdll.NewProc("NtCreateFile")
	procNtCancelIoFileEx       = ntdll.NewProc("NtCancelIoFileEx")
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

func ntCancelIoFileEx(h uintptr, iosb *IoStatusBlock) error {
	r0, _, _ := procNtCancelIoFileEx.Call(
		uintptr(h),
		uintptr(unsafe.Pointer(iosb)),
		0, // IoStatusBlock (out) optional; pass 0
	)
	return ntStatusToError(r0)
}

func ntDirectoryCall(
	ctx context.Context,
	handle uintptr,
	ioStatusBlock *IoStatusBlock,
	buffer []byte,
	restartScan bool,
) error {
	evt, err := windows.CreateEvent(nil, 1, 0, nil) // manual reset, nonsignaled
	if err != nil {
		return err
	}
	defer windows.CloseHandle(evt)

	toCtx, c := context.WithTimeout(ctx, time.Minute)
	defer c()

	status, _, _ := ntQueryDirectoryFile.Call(
		uintptr(handle),
		uintptr(evt), // Event signaled on completion
		0,            // ApcRoutine
		0,            // ApcContext
		uintptr(unsafe.Pointer(ioStatusBlock)),
		uintptr(unsafe.Pointer(&buffer[0])),
		uintptr(len(buffer)),
		uintptr(1), // FileDirectoryInformation
		uintptr(0), // ReturnSingleEntry = FALSE
		0,          // FileName = NULL
		uintptr(boolToInt(restartScan)),
	)

	switch status {
	case 0: // STATUS_SUCCESS
		return nil
	case STATUS_NO_MORE_FILES:
		return os.ErrProcessDone
	case STATUS_PENDING:
		// continue to wait loop
	default:
		return fmt.Errorf("NtQueryDirectoryFile failed: 0x%x", status)
	}

	for {
		timeout := uint32(windows.INFINITE)
		if dl, ok := toCtx.Deadline(); ok {
			d := time.Until(dl)
			if d <= 0 {
				_ = ntCancelIoFileEx(handle, ioStatusBlock)
				_, _ = windows.WaitForSingleObject(evt, 10)
				return toCtx.Err()
			}
			ms := d / time.Millisecond
			if ms <= 0 {
				ms = 1
			}
			if ms > 0x7fffffff {
				ms = 0x7fffffff
			}
			timeout = uint32(ms)
		}

		wrc, werr := windows.WaitForSingleObject(evt, timeout)
		if werr != nil {
			_ = ntCancelIoFileEx(handle, ioStatusBlock)
			return werr
		}
		switch wrc {
		case windows.WAIT_OBJECT_0:
			return nil
		case uint32(windows.WAIT_TIMEOUT):
			if err := toCtx.Err(); err != nil {
				_ = ntCancelIoFileEx(handle, ioStatusBlock)
				_, _ = windows.WaitForSingleObject(evt, 10)
				return err
			}
		case windows.WAIT_ABANDONED:
			_ = ntCancelIoFileEx(handle, ioStatusBlock)
			return errors.New("wait abandoned")
		default:
			_ = ntCancelIoFileEx(handle, ioStatusBlock)
			return fmt.Errorf("unexpected wait result: %d", wrc)
		}
	}
}
