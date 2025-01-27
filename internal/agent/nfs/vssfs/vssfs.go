//go:build windows
// +build windows

package vssfs

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/nfs/windows_utils"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"golang.org/x/sys/windows"
)

// VSSFS extends osfs while enforcing read-only operations
type VSSFS struct {
	billy.Filesystem
	snapshot *snapshots.WinVSSSnapshot
	root     string

	PathToID *lru.Cache[string, uint64]
	IDToPath *lru.Cache[uint64, string]
}

var CacheLimit = 131072

var _ billy.Filesystem = (*VSSFS)(nil)

func NewVSSFS(snapshot *snapshots.WinVSSSnapshot, baseDir string) billy.Filesystem {
	pathToId, err := lru.New[string, uint64](CacheLimit)
	if err != nil {
		syslog.L.Errorf("LRU cache initialization error: %v", err)
		return nil
	}

	idToPath, err := lru.New[uint64, string](CacheLimit)
	if err != nil {
		syslog.L.Errorf("LRU cache initialization error: %v", err)
		return nil
	}

	fs := &VSSFS{
		Filesystem: osfs.New(filepath.Join(snapshot.SnapshotPath, baseDir), osfs.WithBoundOS()),
		snapshot:   snapshot,
		root:       filepath.Join(snapshot.SnapshotPath, baseDir),
		PathToID:   pathToId,
		IDToPath:   idToPath,
	}

	// Pre-cache root directory
	rootPath := filepath.Join(snapshot.SnapshotPath, baseDir)

	fs.PathToID.Add(rootPath, 0)
	fs.IDToPath.Add(0, rootPath)

	return fs
}

// Override write operations to return read-only errors
func (fs *VSSFS) Create(filename string) (billy.File, error) {
	return nil, fmt.Errorf("filesystem is read-only")
}

func (fs *VSSFS) Open(filename string) (billy.File, error) {
	return fs.OpenFile(filename, os.O_RDONLY, 0)
}

func (fs *VSSFS) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	if flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0 {
		return nil, fmt.Errorf("filesystem is read-only")
	}

	return fs.Filesystem.OpenFile(filename, flag, perm)
}

func (fs *VSSFS) Rename(oldpath, newpath string) error {
	return fmt.Errorf("filesystem is read-only")
}

func (fs *VSSFS) Remove(filename string) error {
	return fmt.Errorf("filesystem is read-only")
}

func (fs *VSSFS) MkdirAll(filename string, perm os.FileMode) error {
	return fmt.Errorf("filesystem is read-only")
}

func (fs *VSSFS) Symlink(target, link string) error {
	return fmt.Errorf("filesystem is read-only")
}

func (fs *VSSFS) TempFile(dir, prefix string) (billy.File, error) {
	return nil, fmt.Errorf("filesystem is read-only")
}

func (fs *VSSFS) Chmod(name string, mode os.FileMode) error {
	return fmt.Errorf("filesystem is read-only")
}

func (fs *VSSFS) Lchown(name string, uid, gid int) error {
	return fmt.Errorf("filesystem is read-only")
}

func (fs *VSSFS) Chown(name string, uid, gid int) error {
	return fmt.Errorf("filesystem is read-only")
}

func (fs *VSSFS) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return fmt.Errorf("filesystem is read-only")
}

func (fs *VSSFS) Lstat(filename string) (os.FileInfo, error) {
	return fs.Stat(filename)
}

func (fs *VSSFS) Stat(filename string) (os.FileInfo, error) {
	windowsPath := filepath.FromSlash(filename)
	fullPath := filepath.Join(fs.root, windowsPath)

	if filename == "." || filename == "" {
		fullPath = fs.root
		windowsPath = "."
	}

	pathPtr, err := windows.UTF16PtrFromString(fullPath)
	if err != nil {
		return nil, err
	}

	var findData windows.Win32finddata
	handle, err := windows.FindFirstFile(pathPtr, &findData)
	if err != nil {
		return nil, mapWinError(err, filename)
	}
	defer windows.FindClose(handle)

	foundName := windows.UTF16ToString(findData.FileName[:])
	expectedName := filepath.Base(fullPath)
	if filename == "." {
		expectedName = foundName
	}

	if !strings.EqualFold(foundName, expectedName) {
		return nil, os.ErrNotExist
	}

	// Use foundName as the file name for FileInfo
	name := foundName
	if filename == "." {
		name = "."
	}
	if filename == "/" {
		name = "/"
	}

	info := createFileInfoFromFindData(name, fullPath, windowsPath, &findData, fs)

	return info, nil
}

func (fs *VSSFS) ReadDir(dirname string) ([]os.FileInfo, error) {

	windowsDir := filepath.FromSlash(dirname)
	fullDirPath := filepath.Join(fs.root, windowsDir)

	if dirname == "." || dirname == "" {
		windowsDir = "."
		fullDirPath = fs.root
	}

	searchPath := filepath.Join(fullDirPath, "*")

	var findData windows.Win32finddata
	handle, err := windows_utils.FindFirstFileEx(searchPath, &findData)
	if err != nil {
		return nil, mapWinError(err, dirname)
	}
	defer windows.FindClose(handle)

	var entries []os.FileInfo
	for {
		name := windows.UTF16ToString(findData.FileName[:])

		if name == "." || name == ".." {
			if err := windows.FindNextFile(handle, &findData); err != nil {
				if err == windows.ERROR_NO_MORE_FILES {
					break
				}
				return nil, err
			}
			continue
		}

		winEntryPath := filepath.Join(windowsDir, name)
		fullPath := filepath.Join(fs.root, winEntryPath)

		if skipPathWithAttributes(findData.FileAttributes) {
			if err := windows.FindNextFile(handle, &findData); err != nil {
				if err == windows.ERROR_NO_MORE_FILES {
					break
				}
				return nil, err
			}
			continue
		}

		info := createFileInfoFromFindData(name, fullPath, winEntryPath, &findData, fs)
		entries = append(entries, info)
		if err := windows.FindNextFile(handle, &findData); err != nil {
			if err == windows.ERROR_NO_MORE_FILES {
				break
			}
			return nil, err
		}
	}

	return entries, nil
}

func mapWinError(err error, path string) error {
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
			Path: path,
			Err:  err,
		}
	}
}
