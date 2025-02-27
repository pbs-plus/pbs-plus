//go:build linux

package arpcfs

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/go-git/go-billy/v5"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/backend/arpc/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

var _ billy.Filesystem = (*ARPCFS)(nil)

func NewARPCFS(ctx context.Context, session *arpc.Session, hostname string, jobId string) *ARPCFS {
	statC, err := lru.New[string, statCacheEntry](1024)
	if err != nil {
		panic(err)
	}
	readDirC, err := lru.New[string, readDirCacheEntry](1024)
	if err != nil {
		panic(err)
	}
	statFSC, err := lru.New[string, statFSCacheEntry](1)
	if err != nil {
		panic(err)
	}

	fs := &ARPCFS{
		basePath:       "/",
		ctx:            ctx,
		session:        session,
		JobId:          jobId,
		Hostname:       hostname,
		statCache:      statC,
		readDirCache:   readDirC,
		statFSCache:    statFSC,
		statCacheMu:    NewShardedRWMutex(16),
		readDirCacheMu: NewShardedRWMutex(16),
		statFSCacheMu:  NewShardedRWMutex(4),
	}

	return fs
}

func hashPath(path string) uint64 {
	return xxhash.Sum64String(path)
}

// trackAccess records that a path has been accessed using its xxHash
func (fs *ARPCFS) trackAccess(path string, isDir bool) {
	h := hashPath(path)
	if _, loaded := fs.accessedPaths.LoadOrStore(h, isDir); !loaded {
		if isDir {
			atomic.AddInt64(&fs.folderCount, 1)
		} else {
			atomic.AddInt64(&fs.fileCount, 1)
		}
	}
}

// GetStats returns a unified snapshot of all access and byte-read stats.
func (fs *ARPCFS) GetStats() Stats {
	currentTime := time.Now()

	// Obtain the current unique counts atomically.
	currentFileCount := atomic.LoadInt64(&fs.fileCount)
	currentFolderCount := atomic.LoadInt64(&fs.folderCount)
	totalAccessed := currentFileCount + currentFolderCount

	// Calculate access speed (unique accesses per second).
	var accessSpeed float64
	fs.lastAccessMu.Lock()
	elapsed := currentTime.Sub(fs.lastAccessTime).Seconds()
	if elapsed > 0 {
		accessSpeed = float64((currentFileCount+currentFolderCount)-
			(fs.lastFileCount+fs.lastFolderCount)) / elapsed
	}
	// Update last state for subsequent speed calculation.
	fs.lastAccessTime = currentTime
	fs.lastFileCount = currentFileCount
	fs.lastFolderCount = currentFolderCount
	fs.lastAccessMu.Unlock()

	// Calculate byte read speed.
	var bytesSpeed float64
	fs.totalBytesMu.Lock()
	bytesDiff := fs.totalBytes - fs.lastTotalBytes
	secDiff := currentTime.Sub(fs.lastBytesTime).Seconds()
	if secDiff > 0 {
		bytesSpeed = float64(bytesDiff) / secDiff
	}
	fs.lastTotalBytes = fs.totalBytes
	fs.lastBytesTime = currentTime
	fs.totalBytesMu.Unlock()

	return Stats{
		FilesAccessed:   currentFileCount,
		FoldersAccessed: currentFolderCount,
		TotalAccessed:   totalAccessed,
		FileAccessSpeed: accessSpeed,
		TotalBytes:      fs.totalBytes,
		ByteReadSpeed:   bytesSpeed,
	}
}

func (f *ARPCFS) Unmount() {
	if f.Mount != nil {
		_ = f.Mount.Unmount()
	}
}

var _ billy.File = (*ARPCFile)(nil)

func (fs *ARPCFS) Create(filename string) (billy.File, error) {
	return nil, os.ErrInvalid
}

func (fs *ARPCFS) Open(filename string) (billy.File, error) {
	filename = filepath.Join(fs.basePath, filename)
	return fs.OpenFile(filename, os.O_RDONLY, 0)
}

func (fs *ARPCFS) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	filename = filepath.Join(fs.basePath, filename)

	if fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
		return nil, os.ErrInvalid
	}
	var resp struct {
		HandleID uint64 `msgpack:"handleID"`
	}

	ctx, cancel := TimeoutCtx()
	defer cancel()

	req := OpenRequest{
		Path: filename,
		Flag: flag,
		Perm: int(perm),
	}

	// Use the CPU efficient CallMsgDirect helper.
	err := fs.session.CallMsg(ctx, fs.JobId+"/OpenFile", req, &resp)
	if err != nil {
		return nil, err
	}

	return &ARPCFile{
		fs:       fs,
		name:     filename,
		handleID: resp.HandleID,
		jobId:    fs.JobId,
	}, nil
}

// Stat first tries the LRU cache before performing an RPC call.
func (fs *ARPCFS) Stat(filename string) (os.FileInfo, error) {
	filename = filepath.Join(fs.basePath, filename)

	// Attempt to fetch from the LRU cache.
	fs.statCacheMu.RLock(filename)
	if entry, ok := fs.statCache.Get(filename); ok {
		fs.statCacheMu.RUnlock(filename)

		fs.trackAccess(filename, entry.info.IsDir())
		return entry.info, nil
	}
	fs.statCacheMu.RUnlock(filename)

	// Cache miss or expired; perform RPC.
	var fi FileInfoResponse
	if fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
		return nil, os.ErrInvalid
	}

	ctx, cancel := TimeoutCtx()
	defer cancel()

	req := StatRequest{Path: filename}

	// Use the new CallMsgDirect helper:
	err := fs.session.CallMsg(ctx, fs.JobId+"/Stat", req, &fi)
	if err != nil {
		return nil, err
	}

	modTime := time.Unix(fi.ModTimeUnix, 0)
	info := &fileInfo{
		name:    filepath.Base(filename),
		size:    fi.Size,
		mode:    fi.Mode,
		modTime: modTime,
		isDir:   fi.IsDir,
	}

	// Update the LRU cache.
	fs.statCacheMu.Lock(filename)
	fs.statCache.Add(filename, statCacheEntry{
		info: info,
	})
	fs.statCacheMu.Unlock(filename)

	fs.trackAccess(filename, info.IsDir())

	return info, nil
}

// StatFS tries the LRU cache before making the RPC call.
func (fs *ARPCFS) StatFS() (types.StatFS, error) {
	const statFSKey = "statFS"

	fs.statFSCacheMu.RLock(statFSKey)
	if entry, ok := fs.statFSCache.Get(statFSKey); ok {
		stat := entry.stat
		fs.statFSCacheMu.RUnlock(statFSKey)
		return stat, nil
	}
	fs.statFSCacheMu.RUnlock(statFSKey)

	if fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
		return types.StatFS{}, os.ErrInvalid
	}

	var fsStat utils.FSStat
	ctx, cancel := TimeoutCtx()
	defer cancel()

	err := fs.session.CallMsg(ctx, fs.JobId+"/FSstat", struct{}{}, &fsStat)
	if err != nil {
		syslog.L.Errorf("StatFS RPC failed: %v", err)
		return types.StatFS{}, err
	}

	stat := types.StatFS{
		Bsize:   uint64(4096), // Standard block size.
		Blocks:  uint64(fsStat.TotalSize / 4096),
		Bfree:   uint64(fsStat.FreeSize / 4096),
		Bavail:  uint64(fsStat.AvailableSize / 4096),
		Files:   uint64(fsStat.TotalFiles),
		Ffree:   uint64(fsStat.FreeFiles),
		NameLen: 255, // Typically supports long filenames.
	}

	fs.statFSCacheMu.Lock(statFSKey)
	fs.statFSCache.Add(statFSKey, statFSCacheEntry{
		stat: stat,
	})
	fs.statFSCacheMu.Unlock(statFSKey)

	return stat, nil
}

// ReadDir first tries the LRU cache before performing an RPC call.
func (fs *ARPCFS) ReadDir(path string) ([]os.FileInfo, error) {
	path = filepath.Join(fs.basePath, path)

	fs.readDirCacheMu.RLock(path)
	if entry, ok := fs.readDirCache.Get(path); ok {
		fs.readDirCacheMu.RUnlock(path)
		fs.trackAccess(path, true)
		return entry.entries, nil
	}
	fs.readDirCacheMu.RUnlock(path)

	if fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
		return nil, os.ErrInvalid
	}

	var resp ReadDirResponse
	ctx, cancel := TimeoutCtx()
	defer cancel()

	req := ReadDirRequest{Path: path}
	err := fs.session.CallMsg(ctx, fs.JobId+"/ReadDir", req, &resp)
	if err != nil {
		return nil, err
	}

	entries := make([]os.FileInfo, len(resp.Entries))
	for i, e := range resp.Entries {
		modTime := time.Unix(e.ModTimeUnix, 0)
		entries[i] = &fileInfo{
			name:    e.Name,
			size:    e.Size,
			mode:    e.Mode,
			modTime: modTime,
			isDir:   e.IsDir,
		}

		childPath := filepath.Join(path, e.Name)
		fs.statCacheMu.Lock(childPath)
		fs.statCache.Add(childPath, statCacheEntry{
			info: entries[i],
		})
		fs.statCacheMu.Unlock(childPath)
		fs.trackAccess(childPath, e.IsDir)
	}

	fs.trackAccess(path, true)

	// Cache the directory listing itself.
	fs.readDirCacheMu.Lock(path)
	fs.readDirCache.Add(path, readDirCacheEntry{
		entries: entries,
	})
	fs.readDirCacheMu.Unlock(path)

	return entries, nil
}

func (fs *ARPCFS) Rename(oldpath, newpath string) error {
	return os.ErrInvalid
}

func (fs *ARPCFS) Remove(filename string) error {
	return os.ErrInvalid
}

func (fs *ARPCFS) MkdirAll(path string, perm os.FileMode) error {
	return os.ErrInvalid
}

func (fs *ARPCFS) Symlink(target, link string) error {
	return os.ErrInvalid
}

func (fs *ARPCFS) Readlink(link string) (string, error) {
	return "", os.ErrInvalid
}

func (fs *ARPCFS) TempFile(dir, prefix string) (billy.File, error) {
	return nil, os.ErrInvalid
}

func (fs *ARPCFS) Join(elem ...string) string {
	return filepath.Join(elem...)
}

func (fs *ARPCFS) Chroot(path string) (billy.Filesystem, error) {
	arpcfs := NewARPCFS(fs.ctx, fs.session, fs.Hostname, fs.JobId)
	arpcfs.basePath = filepath.Join(fs.basePath, path)

	return arpcfs, nil
}

func (fs *ARPCFS) Root() string {
	return fs.basePath
}

func (fs *ARPCFS) Lstat(filename string) (os.FileInfo, error) {
	return fs.Stat(filename)
}

func (fs *ARPCFS) Chmod(name string, mode os.FileMode) error {
	return os.ErrInvalid
}

func (fs *ARPCFS) Lchown(name string, uid, gid int) error {
	return os.ErrInvalid
}

func (fs *ARPCFS) Chown(name string, uid, gid int) error {
	return os.ErrInvalid
}

func (fs *ARPCFS) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return os.ErrInvalid
}
