//go:build windows

package vssfs

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"

	"github.com/Microsoft/go-winio"
	"golang.org/x/sys/windows"
)

func mapWinError(err error) error {
	switch err {
	case windows.ERROR_FILE_NOT_FOUND:
		return os.ErrNotExist
	case windows.ERROR_PATH_NOT_FOUND:
		return os.ErrNotExist
	case windows.ERROR_ACCESS_DENIED:
		return os.ErrPermission
	default:
		return &os.PathError{
			Op:   "access",
			Path: "",
			Err:  err,
		}
	}
}

func fileIDToKey(info *winio.FileIDInfo) string {
	return fmt.Sprintf("%d_%x", info.VolumeSerialNumber, info.FileID)
}

// Define GUID structure (matches Windows' GUID)
type GUID struct {
	Data1 uint32
	Data2 uint16
	Data3 uint16
	Data4 [8]byte
}

var (
	ole32DLL         = syscall.NewLazyDLL("ole32.dll")
	procCoCreateGuid = ole32DLL.NewProc("CoCreateGuid")
)

func generateGUID() (string, error) {
	var guid GUID
	ret, _, _ := procCoCreateGuid.Call(uintptr(unsafe.Pointer(&guid)))
	if ret != 0 {
		return "", fmt.Errorf("CoCreateGuid failed: 0x%08X", ret)
	}

	// Format as standard GUID string
	return fmt.Sprintf("%08X-%04X-%04X-%04X-%012X",
		guid.Data1,
		guid.Data2,
		guid.Data3,
		guid.Data4[:2],
		guid.Data4[2:],
	), nil
}
