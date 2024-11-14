//go:build windows

package sftp

import (
	"os"
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

func isTemporary(path string) (bool, error) {
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

	if at.FileAttributes&windows.FILE_ATTRIBUTE_TEMPORARY == windows.FILE_ATTRIBUTE_TEMPORARY {
		return true, nil
	}

	return false, nil
}

func inconsistentSize(path string) (bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return true, err
	}
	defer file.Close()

	si := &FileStandardInfo{}
	err = windows.GetFileInformationByHandleEx(windows.Handle(file.Fd()), windows.FileStandardInfo, (*byte)(unsafe.Pointer(si)), uint32(unsafe.Sizeof(*si)))
	if err != nil {
		return true, err
	}

	stat, err := os.Lstat(path)
	if err != nil {
		return true, err
	}

	if si.EndOfFile == stat.Size() {
		return false, nil
	}

	return true, nil
}
