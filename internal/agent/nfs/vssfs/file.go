//go:build windows

package vssfs

import (
	"os"
	"time"

	"github.com/willscott/go-nfs/file"
	"golang.org/x/sys/windows"
)

type VSSFileInfo struct {
	os.FileInfo
	stableID uint64
	name     string
	size     int64
	modTime  time.Time
	isDir    bool
}

func (fi *VSSFileInfo) Name() string       { return fi.name }
func (fi *VSSFileInfo) Size() int64        { return fi.size }
func (fi *VSSFileInfo) Mode() os.FileMode  { return fi.FileInfo.Mode() }
func (fi *VSSFileInfo) ModTime() time.Time { return fi.modTime }
func (fi *VSSFileInfo) IsDir() bool        { return fi.FileInfo.IsDir() }
func (vi *VSSFileInfo) Sys() interface{} {
	nlink := uint32(1)
	if vi.IsDir() {
		nlink = 2 // Minimum links for directories (self + parent)
	}

	return file.FileInfo{
		Nlink:  nlink,
		UID:    1000,
		GID:    1000,
		Major:  0,
		Minor:  0,
		Fileid: vi.stableID,
	}
}

func createFileInfoFromFindData(name string, fd *windows.Win32finddata) os.FileInfo {
	size := int64(fd.FileSizeHigh)<<32 + int64(fd.FileSizeLow)
	modTime := time.Unix(0, fd.LastWriteTime.Nanoseconds())

	return &VSSFileInfo{
		name:    name,
		size:    size,
		modTime: modTime,
	}
}
