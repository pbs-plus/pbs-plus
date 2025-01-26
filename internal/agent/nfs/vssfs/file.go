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
	mode     os.FileMode
	modTime  time.Time
	isDir    bool
}

func (fi *VSSFileInfo) Name() string       { return fi.name }
func (fi *VSSFileInfo) Size() int64        { return fi.size }
func (fi *VSSFileInfo) Mode() os.FileMode  { return fi.mode }
func (fi *VSSFileInfo) ModTime() time.Time { return fi.modTime }
func (fi *VSSFileInfo) IsDir() bool        { return fi.isDir }
func (vi *VSSFileInfo) Sys() interface{} {
	return file.FileInfo{
		Nlink:  1,
		UID:    1000,
		GID:    1000,
		Major:  0,
		Minor:  0,
		Fileid: vi.stableID,
	}
}

func createFileInfoFromFindData(name string, fd *windows.Win32finddata) os.FileInfo {
	mode := os.FileMode(0)
	if fd.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
		mode |= os.ModeDir
	}
	if fd.FileAttributes&windows.FILE_ATTRIBUTE_READONLY != 0 {
		mode |= 0444
	} else {
		mode |= 0666
	}

	size := int64(fd.FileSizeHigh)<<32 + int64(fd.FileSizeLow)
	modTime := time.Unix(0, fd.LastWriteTime.Nanoseconds())

	return &VSSFileInfo{
		name:    name,
		size:    size,
		mode:    mode,
		modTime: modTime,
		isDir:   mode&os.ModeDir != 0,
	}
}
