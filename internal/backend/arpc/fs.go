//go:build linux

package arpcfs

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/vssfs/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/safemap"
)

func NewARPCFS(ctx context.Context, session *arpc.Session, hostname string, jobId string) *ARPCFS {
	fs := &ARPCFS{
		basePath:      "/",
		ctx:           ctx,
		session:       session,
		JobId:         jobId,
		Hostname:      hostname,
		accessedPaths: safemap.New[string, bool](),
	}

	return fs
}

// trackAccess records that a path has been accessed using its xxHash
func (fs *ARPCFS) trackAccess(path string, isDir bool) {
	if _, loaded := fs.accessedPaths.Get(path); !loaded {
		fs.accessedPaths.Set(path, isDir)
		if isDir {
			atomic.AddInt64(&fs.folderCount, 1)
		} else {
			atomic.AddInt64(&fs.fileCount, 1)
		}
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
	if f.Mount != nil {
		_ = f.Mount.Unmount()
	}
}

func (fs *ARPCFS) Open(filename string) (ARPCFile, error) {
	return fs.OpenFile(filename, os.O_RDONLY, 0)
}

func (fs *ARPCFS) OpenFile(filename string, flag int, perm os.FileMode) (ARPCFile, error) {
	if fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
		return ARPCFile{}, os.ErrInvalid
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
		return ARPCFile{}, err
	}

	err = resp.Decode(raw)
	if err != nil {
		return ARPCFile{}, os.ErrInvalid
	}

	return ARPCFile{
		fs:       fs,
		name:     filename,
		handleID: resp,
		jobId:    fs.JobId,
	}, nil
}

// Stat first tries the LRU cache before performing an RPC call.
func (fs *ARPCFS) Stat(filename string) (types.VSSFileInfo, error) {
	var fi types.VSSFileInfo
	if fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
		return types.VSSFileInfo{}, os.ErrInvalid
	}

	req := types.StatReq{Path: filename}
	raw, err := fs.session.CallMsgWithTimeout(time.Second*10, fs.JobId+"/Stat", &req)
	if err != nil {
		return types.VSSFileInfo{}, err
	}

	err = fi.Decode(raw)
	if err != nil {
		return types.VSSFileInfo{}, os.ErrInvalid
	}

	fs.trackAccess(filename, fi.IsDir)

	return fi, nil
}

// StatFS tries the LRU cache before making the RPC call.
func (fs *ARPCFS) StatFS() (types.StatFS, error) {
	const statFSKey = "statFS"

	if fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
		return types.StatFS{}, os.ErrInvalid
	}

	var fsStat types.StatFS
	raw, err := fs.session.CallMsgWithTimeout(10*time.Second, fs.JobId+"/StatFS", nil)
	if err != nil {
		syslog.L.Errorf("StatFS RPC failed: %v", err)
		return types.StatFS{}, err
	}

	err = fsStat.Decode(raw)
	if err != nil {
		syslog.L.Errorf("StatFS RPC failed: %v", err)
		return types.StatFS{}, os.ErrInvalid
	}

	return fsStat, nil
}

// ReadDir first tries the LRU cache before performing an RPC call.
func (fs *ARPCFS) ReadDir(path string) (types.ReadDirEntries, error) {
	if fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
		return nil, os.ErrInvalid
	}

	var resp types.ReadDirEntries
	req := types.ReadDirReq{Path: path}
	raw, err := fs.session.CallMsgWithTimeout(10*time.Second, fs.JobId+"/ReadDir", &req)
	if err != nil {
		return nil, err
	}

	err = resp.Decode(raw)
	if err != nil {
		return nil, os.ErrInvalid
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
