//go:build windows
// +build windows

package vssfs

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

	mu            sync.RWMutex
	PathToID      map[string]uint64
	IDToPath      map[uint64]string
	fileInfoCache map[string]*VSSFileInfo
	CacheMu       sync.RWMutex // Protects the caches
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
		fileInfoCache: make(map[string]*VSSFileInfo),
	}

	// Initialize root directory
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Pre-cache root directory
	rootPath := fs.normalizePath("/")
	fullRootPath := filepath.Join(fs.root, filepath.Clean("/"))

	var fi windows.ByHandleFileInformation
	rootID, _, err := getFileIDWindows(fullRootPath, &fi)
	if err == nil {
		fs.PathToID[rootPath] = rootID
		fs.IDToPath[rootID] = rootPath
	} else {
		syslog.L.Errorf("Failed to initialize root directory: %v", err)
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
	normalizedDir := fs.normalizePath(dirname)
	syslog.L.Infof("Reading directory: %s (normalized: %s)", dirname, normalizedDir)

	// Verify directory exists in cache
	if _, err := fs.getStableID(normalizedDir); err != nil {
		return nil, fmt.Errorf("directory inaccessible: %w", err)
	}

	entries, err := fs.Filesystem.ReadDir(dirname)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	results := make([]os.FileInfo, 0, len(entries))
	for _, entry := range entries {
		entryName := entry.Name()
		entryPath := filepath.Join(dirname, entryName)
		normalizedEntry := fs.normalizePath(entryPath)
		fullPath := filepath.Join(fs.root, filepath.Clean(normalizedEntry))

		if skipPath(fullPath, fs.snapshot, fs.ExcludedPaths) {
			syslog.L.Infof("Skipping excluded entry: %s", normalizedEntry)
			continue
		}

		// Convert to VSSFileInfo
		vssInfo, err := fs.getVSSFileInfo(normalizedEntry, entry)
		if err != nil {
			syslog.L.Warnf("Skipping entry due to ID error: %s - %v", normalizedEntry, err)
			continue
		}

		syslog.L.Infof("Adding entry: %s (ID: %d)", normalizedEntry, vssInfo.stableID)
		results = append(results, vssInfo)
	}

	syslog.L.Infof("Returning %d entries for %s", len(results), normalizedDir)
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
	fs.mu.RLock()
	if id, exists := fs.PathToID[path]; exists {
		fs.mu.RUnlock()
		return id, nil
	}
	fs.mu.RUnlock()

	fullPath := filepath.Join(fs.root, filepath.Clean(path))
	var fi windows.ByHandleFileInformation
	stableID, _, err := getFileIDWindows(fullPath, &fi)
	if err != nil {
		return 0, err
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Double-check after lock
	if existing, exists := fs.PathToID[path]; exists {
		return existing, nil
	}

	fs.PathToID[path] = stableID
	fs.IDToPath[stableID] = path
	return stableID, nil
}

func (fs *VSSFS) getVSSFileInfo(path string, baseInfo os.FileInfo) (*VSSFileInfo, error) {
	// Check cache first
	fs.CacheMu.RLock()
	if cached, exists := fs.fileInfoCache[path]; exists {
		fs.CacheMu.RUnlock()
		return cached, nil
	}s.CacheMu.RUnlock()

	// Generate stable ID if needed
	stableID, err := fs.getStableID(path)
	if err != nil {
		return nil, err
	}

	// Get number of links from Windows attributes
	fullPath := filepath.Join(fs.root, filepath.Clean(path))
	var fi windows.ByHandleFileInformation
	if _, _, err := getFileIDWindows(fullPath, &fi); err != nil {
		return nil, err
	}

	// Create and cache the VSSFileInfo
	vssInfo := &VSSFileInfo{
		FileInfo: baseInfo,
		stableID: stableID,
		nLinks:   fi.NumberOfLinks,
	}

	fs.CacheMu.Lock()
	fs.fileInfoCache[path] = vssInfo
	fs.CacheMu.Unlock()

	return vssInfo, nil
}

func (fs *VSSFS) normalizePath(path string) string {
	// Convert to forward slashes and clean path
	cleanPath := filepath.ToSlash(filepath.Clean(path))

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

