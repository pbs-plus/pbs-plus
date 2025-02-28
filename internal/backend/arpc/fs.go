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
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/vssfs"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/backend/arpc/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

var _ billy.Filesystem = (*ARPCFS)(nil)

func NewARPCFS(ctx context.Context, session *arpc.Session, hostname string, jobId string) *ARPCFS {
	fs := &ARPCFS{
		basePath: "/",
		ctx:      ctx,
		session:  session,
		JobId:    jobId,
		Hostname: hostname,
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
	return fs.OpenFile(filename, os.O_RDONLY, 0)
}

func (fs *ARPCFS) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	if fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
		return nil, os.ErrInvalid
	}

	var resp vssfs.FileHandleId

	ctx, cancel := TimeoutCtx()
	defer cancel()

	req := vssfs.OpenFileReq{
		Path: filename,
		Flag: flag,
		Perm: int(perm),
	}
	reqBytes, err := req.MarshalMsg(nil)
	if err != nil {
		return nil, os.ErrInvalid
	}

	// Use the CPU efficient CallMsgDirect helper.
	raw, err := fs.session.CallMsg(ctx, fs.JobId+"/OpenFile", reqBytes)
	if err != nil {
		return nil, err
	}

	_, err = resp.UnmarshalMsg(raw)
	if err != nil {
		return nil, os.ErrInvalid
	}

	return &ARPCFile{
		fs:       fs,
		name:     filename,
		handleID: int(resp),
		jobId:    fs.JobId,
	}, nil
}

// Stat first tries the LRU cache before performing an RPC call.
func (fs *ARPCFS) Stat(filename string) (os.FileInfo, error) {
	var fi vssfs.VSSFileInfo
	if fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
		return nil, os.ErrInvalid
	}

	ctx, cancel := TimeoutCtx()
	defer cancel()

	req := vssfs.StatReq{Path: filename}
	reqBytes, err := req.MarshalMsg(nil)
	if err != nil {
		return nil, os.ErrInvalid
	}

	// Use the new CallMsgDirect helper:
	raw, err := fs.session.CallMsg(ctx, fs.JobId+"/Stat", reqBytes)
	if err != nil {
		return nil, err
	}

	_, err = fi.UnmarshalMsg(raw)
	if err != nil {
		return nil, os.ErrInvalid
	}

	info := &fileInfo{
		name:    filepath.Base(filename),
		size:    fi.Size,
		mode:    os.FileMode(fi.Mode),
		modTime: fi.ModTime,
		isDir:   fi.IsDir,
	}

	fs.trackAccess(filename, info.IsDir())

	return info, nil
}

// StatFS tries the LRU cache before making the RPC call.
func (fs *ARPCFS) StatFS() (types.StatFS, error) {
	const statFSKey = "statFS"

	if fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
		return types.StatFS{}, os.ErrInvalid
	}

	var fsStat vssfs.FSStat
	ctx, cancel := TimeoutCtx()
	defer cancel()

	raw, err := fs.session.CallMsg(ctx, fs.JobId+"/FSstat", nil)
	if err != nil {
		syslog.L.Errorf("StatFS RPC failed: %v", err)
		return types.StatFS{}, err
	}

	_, err = fsStat.UnmarshalMsg(raw)
	if err != nil {
		syslog.L.Errorf("StatFS RPC failed: %v", err)
		return types.StatFS{}, os.ErrInvalid
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

	return stat, nil
}

// ReadDir first tries the LRU cache before performing an RPC call.
func (fs *ARPCFS) ReadDir(path string) ([]os.FileInfo, error) {
	if fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
		return nil, os.ErrInvalid
	}

	var resp vssfs.ReadDirEntries
	ctx, cancel := TimeoutCtx()
	defer cancel()

	req := vssfs.ReadDirReq{Path: path}
	reqBytes, err := req.MarshalMsg(nil)
	if err != nil {
		return nil, os.ErrInvalid
	}

	raw, err := fs.session.CallMsg(ctx, fs.JobId+"/ReadDir", reqBytes)
	if err != nil {
		return nil, err
	}

	_, err = resp.UnmarshalMsg(raw)
	if err != nil {
		return nil, os.ErrInvalid
	}

	entries := make([]os.FileInfo, len(resp))
	for i, e := range resp {
		entries[i] = &fileInfo{
			name:    e.Name,
			size:    e.Size,
			mode:    os.FileMode(e.Mode),
			modTime: e.ModTime,
			isDir:   e.IsDir,
		}

		childPath := filepath.Join(path, e.Name)
		fs.trackAccess(childPath, e.IsDir)
	}

	fs.trackAccess(path, true)

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
