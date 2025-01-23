//go:build windows
// +build windows

package vssfs

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
)

// VSSFS extends osfs while enforcing read-only operations
type VSSFS struct {
	billy.Filesystem
	snapshot      *snapshots.WinVSSSnapshot
	ExcludedPaths []*regexp.Regexp
	PartialFiles  []*regexp.Regexp
}

func NewVSSFS(snapshot *snapshots.WinVSSSnapshot, excludedPaths []*regexp.Regexp, partialFiles []*regexp.Regexp) billy.Filesystem {
	return &VSSFS{
		Filesystem:    osfs.New(snapshot.SnapshotPath),
		snapshot:      snapshot,
		ExcludedPaths: excludedPaths,
		PartialFiles:  partialFiles,
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
	if skipFile(fullPath, fs.snapshot, fs.ExcludedPaths) {
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

// Override read operations to check if file should be skipped
func (fs *VSSFS) Stat(filename string) (os.FileInfo, error) {
	fullPath := filepath.Join(fs.Root(), filepath.Clean(filename))
	if skipFile(fullPath, fs.snapshot, fs.ExcludedPaths) {
		return nil, os.ErrNotExist
	}

	info, err := fs.Filesystem.Stat(filename)
	if err != nil {
		return nil, err
	}

	return info, nil
}

func (fs *VSSFS) Lstat(filename string) (os.FileInfo, error) {
	return fs.Stat(filename)
}

func (fs *VSSFS) ReadDir(path string) ([]os.FileInfo, error) {
	entries, err := fs.Filesystem.ReadDir(path)
	if err != nil {
		return nil, err
	}

	var fileInfos []os.FileInfo
	for _, entry := range entries {
		entryPath := filepath.Join(fs.Root(), path, entry.Name())
		if skipFile(entryPath, fs.snapshot, fs.ExcludedPaths) {
			continue
		}

		info := entry
		fileInfos = append(fileInfos, info)
	}

	return fileInfos, nil
}

func (fs *VSSFS) Readlink(link string) (string, error) {
	fullPath := filepath.Join(fs.Root(), filepath.Clean(link))
	if skipFile(fullPath, fs.snapshot, fs.ExcludedPaths) {
		return "", os.ErrNotExist
	}
	return fs.Filesystem.Readlink(link)
}

// Preserve Chroot functionality while maintaining read-only nature
func (fs *VSSFS) Chroot(path string) (billy.Filesystem, error) {
	return NewVSSFS(fs.snapshot, fs.ExcludedPaths, fs.PartialFiles), nil
}
