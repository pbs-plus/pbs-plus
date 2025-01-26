//go:build windows

package windows_utils

import (
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

var (
	kernel32        = syscall.NewLazyDLL("kernel32.dll")
	findFirstFileEx = kernel32.NewProc("FindFirstFileExW")
)

const (
	FindExInfoStandard        = 0
	FindExSearchNameMatch     = 0
	FIND_FIRST_EX_LARGE_FETCH = 0x00000002
)

func FindFirstFileEx(path string, findData *windows.Win32finddata) (windows.Handle, error) {
	pathPtr, _ := syscall.UTF16PtrFromString(path)
	ret, _, err := findFirstFileEx.Call(
		uintptr(unsafe.Pointer(pathPtr)),
		uintptr(FindExInfoStandard),
		uintptr(unsafe.Pointer(findData)),
		uintptr(FindExSearchNameMatch),
		0,
		uintptr(FIND_FIRST_EX_LARGE_FETCH),
	)
	if ret == uintptr(windows.InvalidHandle) {
		return windows.InvalidHandle, err
	}
	return windows.Handle(ret), nil
}
