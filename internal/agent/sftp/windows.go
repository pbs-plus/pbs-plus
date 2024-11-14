//go:build windows

package sftp

import (
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// FileStandardInfo contains extended information for the file.
// FILE_STANDARD_INFO in WinBase.h
// https://docs.microsoft.com/en-us/windows/win32/api/winbase/ns-winbase-file_standard_info
type FileStandardInfo struct {
	AllocationSize, EndOfFile int64
	NumberOfLinks             uint32
	DeletePending, Directory  bool
}

type FileAttributeTagInfo struct {
	FileAttributes uint32
	ReparseTag     uint32
}

func invalidAttributes(path string) (bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return true, err
	}
	defer file.Close()

	at := &FileAttributeTagInfo{}
	err = windows.GetFileInformationByHandleEx(windows.Handle(file.Fd()), windows.FileAttributeTagInfo, (*byte)(unsafe.Pointer(at)), uint32(unsafe.Sizeof(*at)))
	if err != nil {
		return true, err
	}

	if at.FileAttributes&windows.FILE_ATTRIBUTE_TEMPORARY != 0 {
		return true, nil
	}

	if at.FileAttributes&windows.FILE_ATTRIBUTE_RECALL_ON_OPEN != 0 {
		return true, nil
	}

	if at.FileAttributes&windows.FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS != 0 {
		return true, nil
	}

	if at.FileAttributes&windows.FILE_ATTRIBUTE_VIRTUAL != 0 {
		return true, nil
	}

	if at.FileAttributes&windows.FILE_ATTRIBUTE_OFFLINE != 0 {
		return true, nil
	}

	return false, nil
}

const ERROR_SHARING_VIOLATION syscall.Errno = 32

func isFileOpen(path string) bool {
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return false
	}

	// Use CreateFileW system call to open the file with read-write and exclusive access
	// FILE_SHARE_NONE ensures that the file cannot be opened by any other process
	h, err := syscall.CreateFile(
		p,
		syscall.GENERIC_READ|syscall.GENERIC_WRITE,
		0,
		nil,
		syscall.OPEN_EXISTING,
		syscall.FILE_ATTRIBUTE_NORMAL,
		0,
	)

	if err != nil {
		// ERROR_SHARING_VIOLATION means the file is already open by another process
		if errno, ok := err.(syscall.Errno); ok && errno == ERROR_SHARING_VIOLATION {
			return true
		}
		return false
	}

	syscall.CloseHandle(h)
	return false
}
