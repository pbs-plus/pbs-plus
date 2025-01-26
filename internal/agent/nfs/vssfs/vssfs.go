//go:build windows
// +build windows

package vssfs

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/pattern"
	"golang.org/x/sys/windows"
)

// VSSFS extends osfs while enforcing read-only operations
type VSSFS struct {
	billy.Filesystem
	snapshot      *snapshots.WinVSSSnapshot
	ExcludedPaths []*pattern.GlobPattern
	root          string

	PathToID      sync.Map // map[string]uint64
	IDToPath      sync.Map // map[uint64]string
	fileInfoCache sync.Map // map[string]*VSSFileInfo
}

var _ billy.Filesystem = (*VSSFS)(nil)

func NewVSSFS(snapshot *snapshots.WinVSSSnapshot, baseDir string, excludedPaths []*pattern.GlobPattern) billy.Filesystem {
	fs := &VSSFS{
		Filesystem:    osfs.New(filepath.Join(snapshot.SnapshotPath, baseDir), osfs.WithBoundOS()),
		snapshot:      snapshot,
		ExcludedPaths: excludedPaths,
		root:          filepath.Join(snapshot.SnapshotPath, baseDir),
	}

	// Pre-cache root directory
	rootPath := fs.normalizePath("/")

	rootID := getFileIDWindows(rootPath)
	fs.PathToID.Store(rootPath, rootID)
	fs.IDToPath.Store(rootID, rootPath)
	fs.IDToPath.Store(rootID, rootPath)

	return fs
}

// Override write operations to return read-only errors
func (fs *VSSFS) Create(filename string) (billy.File, error) {
	return nil, fmt.Errorf("filesystem is read-only")
}

func (fs *VSSFS) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	if flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0 {
		return nil, fmt.Errorf("filesystem is read-only")
	}

	normalizedPath := fs.normalizePath(filename)
	if _, err := fs.getStableID(normalizedPath); err != nil {
		return nil, os.ErrNotExist
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
	log.Printf("Stat: starting for filename: %s", filename)
	fullPath := filepath.Join(fs.root, filename)
	log.Printf("Stat: full path: %s", fullPath)

	pathPtr, err := windows.UTF16PtrFromString(fullPath)
	if err != nil {
		log.Printf("Stat: UTF16PtrFromString error: %v", err)
		return nil, err
	}

	var findData windows.Win32finddata
	handle, err := windows.FindFirstFile(pathPtr, &findData)
	if err != nil {
		log.Printf("Stat: FindFirstFile error: %v", err)
		return nil, err
	}
	windows.FindClose(handle)

	foundName := windows.UTF16ToString(findData.FileName[:])
	log.Printf("Stat: found name: %s, base filename: %s", foundName, filepath.Base(filename))
	if foundName != filepath.Base(filename) {
		log.Printf("Stat: name mismatch, returning ErrNotExist")
		return nil, os.ErrNotExist
	}

	info := createFileInfoFromFindData(foundName, &findData)
	vssInfo, err := fs.getVSSFileInfo(filename, info)
	log.Printf("Stat: getVSSFileInfo result: %v, error: %v", vssInfo, err)
	return vssInfo, err
}

func (fs *VSSFS) ReadDir(dirname string) ([]os.FileInfo, error) {
	log.Printf("ReadDir: starting for dirname: %s", dirname)
	fullDirPath := filepath.Join(fs.root, dirname)
	searchPath := filepath.Join(fullDirPath, "*")
	log.Printf("ReadDir: search path: %s", searchPath)

	searchPathPtr, err := windows.UTF16PtrFromString(searchPath)
	if err != nil {
		log.Printf("ReadDir: UTF16PtrFromString error: %v", err)
		return nil, err
	}

	var findData windows.Win32finddata
	handle, err := windows.FindFirstFile(searchPathPtr, &findData)
	if err != nil {
		if err == windows.ERROR_FILE_NOT_FOUND {
			log.Printf("ReadDir: directory not found")
			return nil, os.ErrNotExist
		}
		log.Printf("ReadDir: FindFirstFile error: %v", err)
		return nil, err
	}
	defer windows.FindClose(handle)

	var entries []os.FileInfo
	for {
		name := windows.UTF16ToString(findData.FileName[:])
		log.Printf("ReadDir: processing entry: %s", name)

		if name == "." || name == ".." {
			log.Printf("ReadDir: skipping special directory: %s", name)
			if err := windows.FindNextFile(handle, &findData); err != nil {
				if err == windows.ERROR_NO_MORE_FILES {
					break
				}
				log.Printf("ReadDir: FindNextFile error: %v", err)
				return nil, err
			}
			continue
		}

		entryPath := filepath.Join(dirname, name)
		fullPath := filepath.Join(fs.root, entryPath)
		log.Printf("ReadDir: full path for entry: %s", fullPath)

		if skipPathWithAttributes(fullPath, findData.FileAttributes, fs.snapshot, fs.ExcludedPaths) {
			log.Printf("ReadDir: skipping excluded path: %s", fullPath)
			if err := windows.FindNextFile(handle, &findData); err != nil {
				if err == windows.ERROR_NO_MORE_FILES {
					break
				}
				log.Printf("ReadDir: FindNextFile error: %v", err)
				return nil, err
			}
			continue
		}

		info := createFileInfoFromFindData(name, &findData)
		vssInfo, err := fs.getVSSFileInfo(entryPath, info)
		if err != nil {
			log.Printf("ReadDir: getVSSFileInfo error for %s: %v", entryPath, err)
			if err := windows.FindNextFile(handle, &findData); err != nil {
				if err == windows.ERROR_NO_MORE_FILES {
					break
				}
				log.Printf("ReadDir: FindNextFile error: %v", err)
				return nil, err
			}
			continue
		}

		entries = append(entries, vssInfo)
		if err := windows.FindNextFile(handle, &findData); err != nil {
			if err == windows.ERROR_NO_MORE_FILES {
				break
			}
			log.Printf("ReadDir: FindNextFile error: %v", err)
			return nil, err
		}
	}

	log.Printf("ReadDir: completed with %d entries", len(entries))
	return entries, nil
}

func (fs *VSSFS) Readlink(link string) (string, error) {
	return fs.Filesystem.Readlink(link)
}

func (fs *VSSFS) getStableID(path string) (uint64, error) {
	if id, ok := fs.PathToID.Load(path); ok {
		return id.(uint64), nil
	}

	stableID := getFileIDWindows(path)

	fs.PathToID.Store(path, stableID)
	fs.IDToPath.Store(stableID, path)
	return stableID, nil
}

func (fs *VSSFS) getVSSFileInfo(path string, baseInfo os.FileInfo) (*VSSFileInfo, error) {
	if cached, exists := fs.fileInfoCache.Load(path); exists {
		return cached.(*VSSFileInfo), nil
	}

	stableID, err := fs.getStableID(path)
	if err != nil {
		return nil, err
	}

	vssInfo := &VSSFileInfo{
		FileInfo: baseInfo,
		stableID: stableID,
	}

	fs.fileInfoCache.Store(path, vssInfo)
	return vssInfo, nil
}

func (fs *VSSFS) normalizePath(path string) string {
	// Convert to forward slashes and clean path
	cleanPath := filepath.ToSlash(filepath.Clean(path))

	// Normalize to uppercase for case insensitivity
	cleanPath = strings.ToUpper(cleanPath)

	// Ensure root is represented as "/"
	if cleanPath == "." || cleanPath == "" {
		return "/"
	}

	// Add leading slash if missing
	if !strings.HasPrefix(cleanPath, "/") {
		cleanPath = "/" + cleanPath
	}

	// Remove trailing slash (except for root)
	if len(cleanPath) > 1 && strings.HasSuffix(cleanPath, "/") {
		cleanPath = cleanPath[:len(cleanPath)-1]
	}

	return cleanPath
}
