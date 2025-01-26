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

	windowsFilename := filepath.FromSlash(filename)
	fullPath := filepath.Join(fs.root, windowsFilename)
	log.Printf("Stat: converted path: %s", fullPath)

	if filename == "." {
		fullPath = fs.root
		log.Printf("Stat: using root path for '.': %s", fullPath)
	}

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

	baseName := filepath.Base(fullPath)
	if filename == "." {
		baseName = "."
	}
	log.Printf("Stat: comparing base name: %s", baseName)

	foundName := windows.UTF16ToString(findData.FileName[:])
	log.Printf("Stat: found name: %s, isDir: %v", foundName, (findData.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY) != 0)

	if foundName != baseName && !(filename == "." && (findData.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY) != 0) && baseName != "\\" {
		log.Printf("Stat: name mismatch, returning ErrNotExist")
		return nil, os.ErrNotExist
	}

	info := createFileInfoFromFindData(baseName, &findData)
	normalizedPath := fs.normalizePath(filename)
	log.Printf("Stat: normalized path: %s", normalizedPath)

	vssInfo, err := fs.getVSSFileInfo(normalizedPath, info)
	log.Printf("Stat: getVSSFileInfo result: %v, error: %v", vssInfo, err)
	return vssInfo, err
}

func (fs *VSSFS) ReadDir(dirname string) ([]os.FileInfo, error) {
	log.Printf("ReadDir: starting for dirname: %s", dirname)

	windowsDir := filepath.FromSlash(dirname)
	normalizedDir := fs.normalizePath(windowsDir)
	log.Printf("ReadDir: normalized dir: %s", normalizedDir)

	if _, err := fs.getStableID(normalizedDir); err != nil {
		log.Printf("ReadDir: directory inaccessible: %v", err)
		return nil, fmt.Errorf("directory inaccessible: %w", err)
	}

	fullDirPath := filepath.Join(fs.root, windowsDir)
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

		unixEntryPath := filepath.ToSlash(filepath.Join(dirname, name))
		fullPath := filepath.Join(fs.root, windowsDir, name)
		log.Printf("ReadDir: entry paths - unix: %s, full: %s", unixEntryPath, fullPath)

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
		vssInfo, err := fs.getVSSFileInfo(unixEntryPath, info)
		if err != nil {
			log.Printf("ReadDir: getVSSFileInfo error for %s: %v", unixEntryPath, err)
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

	if dirname == "." {
		log.Printf("ReadDir: handling special case for '.'")
		selfInfo, err := fs.Stat(".")
		if err == nil {
			entries = append([]os.FileInfo{selfInfo}, entries...)
		} else {
			log.Printf("ReadDir: error getting self info: %v", err)
		}
	}

	log.Printf("ReadDir: completed with %d entries", len(entries))
	return entries, nil
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
	// Convert to forward slashes first
	cleanPath := filepath.ToSlash(path)

	// Remove any volume name (for Windows paths)
	cleanPath = strings.TrimPrefix(cleanPath, filepath.VolumeName(cleanPath))

	// Clean the path
	cleanPath = filepath.Clean(cleanPath)

	// Normalize to uppercase for case insensitivity
	cleanPath = strings.ToUpper(cleanPath)

	// Handle root path
	if cleanPath == "." || cleanPath == "" {
		return "/"
	}

	// Ensure leading slash
	if !strings.HasPrefix(cleanPath, "/") {
		cleanPath = "/" + cleanPath
	}

	// Remove trailing slash (except root)
	if len(cleanPath) > 1 && strings.HasSuffix(cleanPath, "/") {
		cleanPath = cleanPath[:len(cleanPath)-1]
	}

	return cleanPath
}
