//go:build windows
// +build windows

package nfs

import (
	"syscall"

	nfsFile "github.com/willscott/go-nfs/file"
)

func Stat(path string) (stat *nfsFile.FileInfo, err error) {
	ptr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}

	handle, err := syscall.CreateFile(ptr,
		syscall.GENERIC_READ,
		syscall.FILE_SHARE_READ,
		nil,
		syscall.OPEN_EXISTING,
		syscall.FILE_FLAG_BACKUP_SEMANTICS,
		0)

	if err != nil {
		return nil, err
	}
	defer syscall.CloseHandle(handle)

	var fi syscall.ByHandleFileInformation
	err = syscall.GetFileInformationByHandle(handle, &fi)
	if err != nil {
		return nil, err
	}

	// Create and populate FileInfo
	info := &nfsFile.FileInfo{
		Nlink:  fi.NumberOfLinks,
		UID:    0, // Windows doesn't use Unix-style UID
		GID:    0, // Windows doesn't use Unix-style GID
		Major:  0, // Set based on your requirements
		Minor:  0, // Set based on your requirements
		Fileid: uint64(fi.FileIndexHigh)<<32 | uint64(fi.FileIndexLow),
	}

	return info, nil
}
