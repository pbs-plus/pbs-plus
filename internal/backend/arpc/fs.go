//go:build linux

package arpcfs

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/agentfs/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/zeebo/xxh3"
)

func NewARPCFS(ctx context.Context, session *arpc.Session, hostname string, jobId string, backupMode string) *ARPCFS {
	fs := &ARPCFS{
		basePath:      "/",
		ctx:           ctx,
		session:       session,
		JobId:         jobId,
		Hostname:      hostname,
		accessedPaths: roaring.NewBitmap(),
		backupMode:    backupMode,
	}

	return fs
}

func (fs *ARPCFS) GetBackupMode() string {
	return fs.backupMode
}

func hashPath(path string) uint32 {
	return uint32(xxh3.HashString(path))
}

// trackAccess records that a path has been accessed using its xxHash
func (fs *ARPCFS) trackAccess(path string, isDir bool) {
	hashedPath := hashPath(path)

	fs.accessMutex.RLock()
	alreadyAccessed := fs.accessedPaths.Contains(hashedPath)
	fs.accessMutex.RUnlock()

	if !alreadyAccessed {
		fs.accessMutex.Lock()
		if !fs.accessedPaths.Contains(hashedPath) {
			fs.accessedPaths.Add(hashedPath)
			if isDir {
				atomic.AddInt64(&fs.folderCount, 1)
			} else {
				atomic.AddInt64(&fs.fileCount, 1)
			}
		}
		fs.accessMutex.Unlock()
	}
}

// GetStats returns a unified snapshot of all access and byte-read stats.
func (fs *ARPCFS) GetStats() Stats {
	// Get the current time as UnixNano.
	currentTime := time.Now().UnixNano()

	// Get the current counts atomically.
	currentFileCount := atomic.LoadInt64(&fs.fileCount)
	currentFolderCount := atomic.LoadInt64(&fs.folderCount)
	totalAccessed := currentFileCount + currentFolderCount

	// Atomically swap out the previous access state.
	// SwapInt64 returns the previous value.
	lastATime := atomic.SwapInt64(&fs.lastAccessTime, currentTime)
	lastFileCount := atomic.SwapInt64(&fs.lastFileCount, currentFileCount)
	lastFolderCount := atomic.SwapInt64(&fs.lastFolderCount, currentFolderCount)

	// Calculate elapsed time in seconds.
	elapsed := float64(currentTime-lastATime) / 1e9
	var accessSpeed float64
	if elapsed > 0 {
		accessDelta := (currentFileCount + currentFolderCount) -
			(lastFileCount + lastFolderCount)
		accessSpeed = float64(accessDelta) / elapsed
	}

	// Get the totalBytes count (if updated atomically elsewhere).
	currentTotalBytes := atomic.LoadInt64(&fs.totalBytes)
	// Atomically swap the previous byte state.
	lastBTime := atomic.SwapInt64(&fs.lastBytesTime, currentTime)
	lastTotalBytes := atomic.SwapInt64(&fs.lastTotalBytes, currentTotalBytes)

	secDiff := float64(currentTime-lastBTime) / 1e9
	var bytesSpeed float64
	if secDiff > 0 {
		bytesSpeed = float64(currentTotalBytes-lastTotalBytes) / secDiff
	}

	return Stats{
		FilesAccessed:   currentFileCount,
		FoldersAccessed: currentFolderCount,
		TotalAccessed:   totalAccessed,
		FileAccessSpeed: accessSpeed,
		TotalBytes:      uint64(currentTotalBytes),
		ByteReadSpeed:   bytesSpeed,
	}
}

func (f *ARPCFS) Unmount() {
	f.accessedPaths.Clear()

	if f.Mount != nil {
		_ = f.Mount.Unmount()
	}
}

func (fs *ARPCFS) Open(filename string) (ARPCFile, error) {
	return fs.OpenFile(filename, os.O_RDONLY, 0)
}

func (fs *ARPCFS) OpenFile(filename string, flag int, perm os.FileMode) (ARPCFile, error) {
	if fs.session == nil {
		syslog.L.Error(os.ErrInvalid).WithMessage("arpc session is nil").Write()
		return ARPCFile{}, syscall.EIO
	}

	var resp types.FileHandleId
	req := types.OpenFileReq{
		Path: filename,
		Flag: flag,
		Perm: int(perm),
	}

	// Use the CPU efficient CallMsgDirect helper.
	raw, err := fs.session.CallMsgWithTimeout(10*time.Second, fs.JobId+"/OpenFile", &req)
	if err != nil {
		if arpc.IsOSError(err) {
			return ARPCFile{}, err
		}
		return ARPCFile{}, syscall.EIO
	}

	err = resp.Decode(raw)
	if err != nil {
		return ARPCFile{}, syscall.EIO
	}

	return ARPCFile{
		fs:       fs,
		name:     filename,
		handleID: resp,
		jobId:    fs.JobId,
	}, nil
}

// Stat first tries the LRU cache before performing an RPC call.
func (fs *ARPCFS) Attr(filename string) (types.AgentFileInfo, error) {
	var fi types.AgentFileInfo
	if fs.session == nil {
		syslog.L.Error(os.ErrInvalid).WithMessage("arpc session is nil").Write()
		return types.AgentFileInfo{}, syscall.EIO
	}

	req := types.StatReq{Path: filename}
	raw, err := fs.session.CallMsgWithTimeout(time.Second*10, fs.JobId+"/Attr", &req)
	if err != nil {
		if arpc.IsOSError(err) {
			return types.AgentFileInfo{}, err
		}
		return types.AgentFileInfo{}, syscall.EIO
	}

	err = fi.Decode(raw)
	if err != nil {
		return types.AgentFileInfo{}, syscall.EIO
	}

	fs.trackAccess(filename, fi.IsDir)

	return fi, nil
}

func (fs *ARPCFS) Xattr(filename string) (types.AgentFileInfo, error) {
	var fi types.AgentFileInfo
	if fs.session == nil {
		syslog.L.Error(os.ErrInvalid).WithMessage("arpc session is nil").Write()
		return types.AgentFileInfo{}, syscall.EIO
	}

	req := types.StatReq{Path: filename}
	raw, err := fs.session.CallMsgWithTimeout(time.Second*10, fs.JobId+"/Xattr", &req)
	if err != nil {
		if arpc.IsOSError(err) {
			return types.AgentFileInfo{}, err
		}
		return types.AgentFileInfo{}, syscall.EIO
	}

	err = fi.Decode(raw)
	if err != nil {
		return types.AgentFileInfo{}, syscall.EIO
	}

	fs.trackAccess(filename, fi.IsDir)

	return fi, nil
}

// StatFS tries the LRU cache before making the RPC call.
func (fs *ARPCFS) StatFS() (types.StatFS, error) {
	const statFSKey = "statFS"

	if fs.session == nil {
		syslog.L.Error(os.ErrInvalid).WithMessage("arpc session is nil").Write()
		return types.StatFS{}, syscall.EIO
	}

	var fsStat types.StatFS
	raw, err := fs.session.CallMsgWithTimeout(10*time.Second, fs.JobId+"/StatFS", nil)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to handle statfs").Write()
		if arpc.IsOSError(err) {
			return types.StatFS{}, err
		}
		return types.StatFS{}, syscall.EIO
	}

	err = fsStat.Decode(raw)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to handle statfs decode").Write()
		return types.StatFS{}, syscall.EIO
	}

	return fsStat, nil
}

// ReadDir first tries the LRU cache before performing an RPC call.
func (fs *ARPCFS) ReadDir(path string) (types.ReadDirEntries, error) {
	if fs.session == nil {
		syslog.L.Error(os.ErrInvalid).WithMessage("arpc session is nil").Write()
		return nil, syscall.EIO
	}

	var resp types.ReadDirEntries
	req := types.ReadDirReq{Path: path}
	raw, err := fs.session.CallMsgWithTimeout(10*time.Second, fs.JobId+"/ReadDir", &req)
	if err != nil {
		if arpc.IsOSError(err) {
			return nil, err
		}
		return nil, syscall.EIO
	}

	err = resp.Decode(raw)
	if err != nil {
		return nil, syscall.EIO
	}

	if resp == nil {
		return types.ReadDirEntries{}, nil // Return an empty slice instead of nil
	}

	fs.trackAccess(path, true)

	return resp, nil
}

func (fs *ARPCFS) Join(elem ...string) string {
	return filepath.Join(elem...)
}

func (fs *ARPCFS) Root() string {
	return fs.basePath
}
