//go:build windows

package vssfs

import (
	"os"

	"github.com/willscott/go-nfs/file"
	"golang.org/x/sys/windows"
)

type VSSFileInfo struct {
	os.FileInfo
	path string
}

func (vi *VSSFileInfo) Sys() interface{} {
	pathPtr, err := windows.UTF16PtrFromString(vi.path)
	if err != nil {
		return nil
	}

	handle, err := windows.CreateFile(
		pathPtr,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return nil
	}
	defer windows.CloseHandle(handle)

	var fi windows.ByHandleFileInformation
	err = windows.GetFileInformationByHandle(handle, &fi)
	if err != nil {
		return nil
	}

	volumeID := uint64(fi.VolumeSerialNumber)
	fileIndex := uint64(fi.FileIndexHigh)<<32 | uint64(fi.FileIndexLow)
	stableID := (volumeID << 48) | (fileIndex & 0x0000FFFFFFFFFFFF)

	return file.FileInfo{
		Nlink:  fi.NumberOfLinks,
		UID:    1000,
		GID:    1000,
		Major:  0,
		Minor:  0,
		Fileid: stableID,
	}
}
