//go:build windows
// +build windows

package vssfs

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"golang.org/x/sys/windows"
)

// VSSFS extends osfs while enforcing read-only operations
type VSSFS struct {
	billy.Filesystem
	snapshot      *snapshots.WinVSSSnapshot
	ExcludedPaths []*regexp.Regexp
	PartialFiles  []*regexp.Regexp
	idCache       sync.Map // Key: fullPath (string), Value: *cachedID
	root          string
}

type cachedID struct {
	stableID uint64
	nLinks   uint32
}

var _ billy.Filesystem = (*VSSFS)(nil)

func NewVSSFS(snapshot *snapshots.WinVSSSnapshot, baseDir string, excludedPaths []*regexp.Regexp, partialFiles []*regexp.Regexp) billy.Filesystem {
	return &VSSFS{
		Filesystem:    osfs.New(filepath.Join(snapshot.SnapshotPath, baseDir)),
		snapshot:      snapshot,
		ExcludedPaths: excludedPaths,
		PartialFiles:  partialFiles,
		root:          filepath.Join(snapshot.SnapshotPath, baseDir),
	}
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

func (fs *VSSFS) computeAndCacheID(fullPath string) (uint64, uint32, error) {
	// Use sync.Once or similar pattern to prevent concurrent recomputation
	var result struct {
		id    uint64
		links uint32
		err   error
	}

	actual, _ := fs.idCache.LoadOrStore(fullPath, &sync.Once{})
	once := actual.(*sync.Once)
	once.Do(func() {
		var fi windows.ByHandleFileInformation
		result.id, result.links, result.err = getFileIDWindows(fullPath, &fi)

		if result.err == nil {
			fs.idCache.Store(fullPath, &cachedID{
				stableID: result.id,
				nLinks:   result.links,
			})
		}
	})

	return result.id, result.links, result.err
}

func (fs *VSSFS) getVSSFileInfo(path string, info os.FileInfo) (*VSSFileInfo, error) {
	fullPath := filepath.Join(fs.Root(), filepath.Clean(path))

	if cached, exists := fs.idCache.Load(fullPath); exists {
		ci := cached.(*cachedID)
		return &VSSFileInfo{
			FileInfo: info,
			stableID: ci.stableID,
			nLinks:   ci.nLinks,
		}, nil
	}

	if fi, ok := info.Sys().(*windows.ByHandleFileInformation); ok {
		stableID, nLinks := computeIDFromExisting(fi)
		fs.idCache.Store(fullPath, &cachedID{
			stableID: stableID,
			nLinks:   nLinks,
		})
		return &VSSFileInfo{
			FileInfo: info,
			stableID: stableID,
			nLinks:   nLinks,
		}, nil
	}

	stableID, nLinks, err := fs.computeAndCacheID(fullPath)
	if err != nil {
		return nil, err
	}
	return &VSSFileInfo{
		FileInfo: info,
		stableID: stableID,
		nLinks:   nLinks,
	}, nil
}
