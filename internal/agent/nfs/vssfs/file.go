//go:build windows

package vssfs

import (
	"io/fs"
	"os"
	"time"

	"github.com/willscott/go-nfs/file"
	"golang.org/x/sys/windows"
)

type VSSFileInfo struct {
	stableID uint64
	name     string
	size     int64
	mode     fs.FileMode
	modTime  time.Time
}

func (fi *VSSFileInfo) Name() string       { return fi.name }
func (fi *VSSFileInfo) Size() int64        { return fi.size }
func (fi *VSSFileInfo) Mode() fs.FileMode  { return fi.mode }
func (fi *VSSFileInfo) ModTime() time.Time { return fi.modTime }
func (fi *VSSFileInfo) IsDir() bool        { return fi.Mode().IsDir() }
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

func createFileInfoFromFindData(name string, path string, fd *windows.Win32finddata, vssfs *VSSFS) os.FileInfo {
	var mode fs.FileMode

	// Set base permissions
	if fd.FileAttributes&windows.FILE_ATTRIBUTE_READONLY != 0 {
		mode = 0444 // Read-only for everyone
	} else {
		mode = 0666 // Read-write for everyone
	}

	// Add directory flag and execute permissions
	if fd.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
		mode |= os.ModeDir | 0111 // Add execute bits for traversal
		// Set directory-specific permissions
		mode = (mode & 0666) | 0111 | os.ModeDir // Final mode: drwxr-xr-x
	}

	size := int64(fd.FileSizeHigh)<<32 + int64(fd.FileSizeLow)
	modTime := time.Unix(0, fd.LastWriteTime.Nanoseconds())

	stableID := getFileIDWindows(path)
	vssfs.PathToID.Store(path, stableID)
	vssfs.IDToPath.Store(stableID, path)

	return &VSSFileInfo{
		name:     name,
		size:     size,
		mode:     mode,
		modTime:  modTime,
		stableID: getFileIDWindows(path),
	}
}
