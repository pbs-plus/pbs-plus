//go:build linux

package arpcfs

import (
	"context"
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/agentfs/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/zeebo/xxh3"
)

// accessMsg represents one file (or directory) access event.
type accessMsg struct {
	hash  uint64
	isDir bool
}

// hashPath uses xxHash to obtain an unsigned 64-bit hash of the given path.
func hashPath(path string) uint64 {
	return xxh3.HashString(path)
}

// NewARPCFS creates an instance of ARPCFS and opens the bbolt DB.
// It also starts a background worker to batch and flush file-access events.
func NewARPCFS(ctx context.Context, session *arpc.Session, hostname string, jobId string, backupMode string) *ARPCFS {
	fs := &ARPCFS{
		basePath:   "/",
		ctx:        ctx,
		session:    session,
		JobId:      jobId,
		Hostname:   hostname,
		backupMode: backupMode,
	}
	return fs
}

// GetStats returns a snapshot of all access and byte-read statistics.
func (fs *ARPCFS) GetStats() Stats {
	// Get the current time in nanoseconds.
	currentTime := time.Now().UnixNano()

	// Atomically load the current counters.
	currentFileCount := atomic.LoadInt64(&fs.fileCount)
	currentFolderCount := atomic.LoadInt64(&fs.folderCount)
	totalAccessed := currentFileCount + currentFolderCount

	// Swap out the previous access statistics.
	lastATime := atomic.SwapInt64(&fs.lastAccessTime, currentTime)
	lastFileCount := atomic.SwapInt64(&fs.lastFileCount, currentFileCount)
	lastFolderCount := atomic.SwapInt64(&fs.lastFolderCount, currentFolderCount)

	// Calculate the elapsed time in seconds.
	elapsed := float64(currentTime-lastATime) / 1e9
	var accessSpeed float64
	if elapsed > 0 {
		accessDelta := (currentFileCount + currentFolderCount) - (lastFileCount + lastFolderCount)
		accessSpeed = float64(accessDelta) / elapsed
	}

	// Similarly, for byte counters (if you're tracking totalBytes elsewhere).
	currentTotalBytes := atomic.LoadInt64(&fs.totalBytes)
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

func (fs *ARPCFS) Unmount() {
	if fs.Mount != nil {
		_ = fs.Mount.Unmount()
	}
	if fs.session != nil {
		_ = fs.session.Close()
	}
}

func (fs *ARPCFS) GetBackupMode() string {
	return fs.backupMode
}

func (fs *ARPCFS) Open(filename string) (ARPCFile, error) {
	return fs.OpenFile(filename, os.O_RDONLY, 0)
}

func (fs *ARPCFS) OpenFile(filename string, flag int, perm os.FileMode) (ARPCFile, error) {
	if fs.session == nil {
		syslog.L.Error(os.ErrInvalid).
			WithMessage("arpc session is nil").
			Write()
		return ARPCFile{}, syscall.EIO
	}

	var resp types.FileHandleId
	req := types.OpenFileReq{
		Path: filename,
		Flag: flag,
		Perm: int(perm),
	}

	raw, err := fs.session.CallMsgWithTimeout(1*time.Minute, fs.JobId+"/OpenFile", &req)
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

// Attr retrieves file attributes via RPC and then tracks the access.
func (fs *ARPCFS) Attr(filename string) (types.AgentFileInfo, error) {
	var fi types.AgentFileInfo
	if fs.session == nil {
		syslog.L.Error(os.ErrInvalid).
			WithMessage("arpc session is nil").
			Write()
		return types.AgentFileInfo{}, syscall.EIO
	}

	req := types.StatReq{Path: filename}
	raw, err := fs.session.CallMsgWithTimeout(1*time.Minute, fs.JobId+"/Attr", &req)
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

	if fi.IsDir {
		atomic.AddInt64(&fs.folderCount, 1)
	} else {
		atomic.AddInt64(&fs.fileCount, 1)
	}

	return fi, nil
}

// Xattr retrieves extended attributes and logs the access similarly.
func (fs *ARPCFS) Xattr(filename string) (types.AgentFileInfo, error) {
	var fi types.AgentFileInfo
	if fs.session == nil {
		syslog.L.Error(os.ErrInvalid).
			WithMessage("arpc session is nil").
			Write()
		return types.AgentFileInfo{}, syscall.EIO
	}

	req := types.StatReq{Path: filename}
	raw, err := fs.session.CallMsgWithTimeout(1*time.Minute, fs.JobId+"/Xattr", &req)
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

	return fi, nil
}

// StatFS calls StatFS via RPC.
func (fs *ARPCFS) StatFS() (types.StatFS, error) {
	if fs.session == nil {
		syslog.L.Error(os.ErrInvalid).
			WithMessage("arpc session is nil").
			Write()
		return types.StatFS{}, syscall.EIO
	}

	var fsStat types.StatFS
	raw, err := fs.session.CallMsgWithTimeout(1*time.Minute,
		fs.JobId+"/StatFS", nil)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to handle statfs").Write()
		if arpc.IsOSError(err) {
			return types.StatFS{}, err
		}
		return types.StatFS{}, syscall.EIO
	}

	err = fsStat.Decode(raw)
	if err != nil {
		syslog.L.Error(err).
			WithMessage("failed to handle statfs decode").
			Write()
		return types.StatFS{}, syscall.EIO
	}

	return fsStat, nil
}

// ReadDir calls ReadDir via RPC and logs directory accesses.
func (fs *ARPCFS) ReadDir(path string) (types.ReadDirEntries, error) {
	if fs.session == nil {
		syslog.L.Error(os.ErrInvalid).
			WithMessage("arpc session is nil").
			Write()
		return nil, syscall.EIO
	}

	var resp types.ReadDirEntries
	req := types.ReadDirReq{Path: path}
	raw, err := fs.session.CallMsgWithTimeout(1*time.Minute, fs.JobId+"/ReadDir", &req)
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

	return resp, nil
}

func (fs *ARPCFS) Root() string {
	return fs.basePath
}
