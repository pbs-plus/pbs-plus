//go:build windows

package vssfs

import (
	"io/fs"
	"os"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"github.com/willscott/go-nfs/file"
	"golang.org/x/sys/windows"
)

var (
	kernel32DLL    = windows.NewLazySystemDLL("kernel32.dll")
	lockFileExProc = kernel32DLL.NewProc("LockFileEx")
	unlockFileProc = kernel32DLL.NewProc("UnlockFile")
)

type vssfile struct {
	*os.File
	m sync.Mutex
}

const (
	lockfileExclusiveLock = 0x2
)

func (f *vssfile) Lock() error {
	f.m.Lock()
	defer f.m.Unlock()

	var overlapped windows.Overlapped
	// err is always non-nil as per sys/windows semantics.
	ret, _, err := lockFileExProc.Call(f.File.Fd(), lockfileExclusiveLock, 0, 0xFFFFFFFF, 0,
		uintptr(unsafe.Pointer(&overlapped)))
	runtime.KeepAlive(&overlapped)
	if ret == 0 {
		return err
	}
	return nil
}

func (f *vssfile) Unlock() error {
	f.m.Lock()
	defer f.m.Unlock()

	// err is always non-nil as per sys/windows semantics.
	ret, _, err := unlockFileProc.Call(f.File.Fd(), 0, 0, 0xFFFFFFFF, 0)
	if ret == 0 {
		return err
	}
	return nil
}

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

func createFileInfoFromFindData(name string, relativePath string, fd *windows.Win32finddata) os.FileInfo {
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

	stableID := generateFullPathID(relativePath)

	return &VSSFileInfo{
		name:     name,
		size:     size,
		mode:     mode,
		modTime:  modTime,
		stableID: stableID,
	}
}
