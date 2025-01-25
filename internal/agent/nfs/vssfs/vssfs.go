//go:build windows
// +build windows

package vssfs

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/pattern"
	"golang.org/x/sys/windows"
)

// VSSFS extends osfs while enforcing read-only operations
type VSSFS struct {
	billy.Filesystem
	snapshot      *snapshots.WinVSSSnapshot
	ExcludedPaths *pattern.Matcher
	PartialFiles  *pattern.Matcher
	root          string

	mu       sync.RWMutex
	PathToID map[string]uint64
	IDToPath map[uint64]string
}

type cachedID struct {
	stableID uint64
	nLinks   uint32
}

var _ billy.Filesystem = (*VSSFS)(nil)

func NewVSSFS(snapshot *snapshots.WinVSSSnapshot, baseDir string, excludedPaths *pattern.Matcher, partialFiles *pattern.Matcher) billy.Filesystem {
	fs := &VSSFS{
		Filesystem:    osfs.New(filepath.Join(snapshot.SnapshotPath, baseDir)),
		snapshot:      snapshot,
		ExcludedPaths: excludedPaths,
		PartialFiles:  partialFiles,
		root:          filepath.Join(snapshot.SnapshotPath, baseDir),
		PathToID:      make(map[string]uint64),
		IDToPath:      make(map[uint64]string),
	}

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

	fullPath := filepath.Join(fs.Root(), filepath.Clean(filename))
	if skipPath(fullPath, fs.snapshot, fs.ExcludedPaths) {
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
	info, err := fs.Filesystem.Stat(filename)
	if err != nil {
		return nil, err
	}
	return fs.getVSSFileInfo(filename, info)
}

func (fs *VSSFS) ReadDir(dirname string) ([]os.FileInfo, error) {
	entries, err := fs.Filesystem.ReadDir(dirname)
	if err != nil {
		return nil, err
	}

	fullDirPath := filepath.Join(fs.Root(), dirname)
	if skipPath(fullDirPath, fs.snapshot, fs.ExcludedPaths) {
		return nil, os.ErrNotExist
	}

	results := make([]os.FileInfo, 0, len(entries)) // Single allocation with capacity
	for _, entry := range entries {
		fullPath := filepath.Clean(filepath.Join(fullDirPath, entry.Name()))
		if skipPath(fullPath, fs.snapshot, fs.ExcludedPaths) {
			continue
		}

		vssInfo, err := fs.getVSSFileInfo(filepath.Join(dirname, entry.Name()), entry)
		if err != nil {
			syslog.L.Infof("error: %s -> %v", fullPath, err)
			return nil, err
		}
		results = append(results, vssInfo)
	}
	return results, nil
}

func (fs *VSSFS) Readlink(link string) (string, error) {
	fullPath := filepath.Join(fs.Root(), filepath.Clean(link))
	if skipPath(fullPath, fs.snapshot, fs.ExcludedPaths) {
		return "", os.ErrNotExist
	}
	return fs.Filesystem.Readlink(link)
}

func (fs *VSSFS) getStableID(path string) (uint64, error) {
	// Check existing cache with read lock
	fs.mu.RLock()
	if id, exists := fs.PathToID[path]; exists {
		fs.mu.RUnlock()
		return id, nil
	}
	fs.mu.RUnlock()

	// Generate ID on demand
	fullPath := filepath.Join(fs.root, filepath.Clean(path))
	var fi windows.ByHandleFileInformation
	stableID, _, err := getFileIDWindows(fullPath, &fi)
	if err != nil {
		return 0, err
	}

	// Update cache with write lock
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Double-check in case another goroutine added it while we waited
	if existing, exists := fs.PathToID[path]; exists {
		return existing, nil
	}

	fs.PathToID[path] = stableID
	fs.IDToPath[stableID] = path
	return stableID, nil
}

func (fs *VSSFS) getVSSFileInfo(path string, info os.FileInfo) (*VSSFileInfo, error) {
	fs.mu.RLock()
	cachedID, exists := fs.PathToID[path]
	fs.mu.RUnlock()

	if exists {
		return &VSSFileInfo{
			FileInfo: info,
			stableID: cachedID,
			nLinks:   1, // Adjust based on actual data if available
		}, nil
	}

	// Fallback for uncached files (should rarely happen)
	fullPath := filepath.Join(fs.Root(), filepath.Clean(path))
	var fi windows.ByHandleFileInformation
	stableID, _, err := getFileIDWindows(fullPath, &fi)
	if err != nil {
		return nil, err
	}

	fs.mu.Lock()
	fs.PathToID[path] = stableID
	fs.IDToPath[stableID] = path
	fs.mu.Unlock()

	return &VSSFileInfo{
		FileInfo: info,
		stableID: stableID,
		nLinks:   fi.NumberOfLinks,
	}, nil
}
