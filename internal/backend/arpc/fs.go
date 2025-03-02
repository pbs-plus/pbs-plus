//go:build linux

package arpcfs

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/vssfs"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/hashmap"
)

func NewARPCFS(ctx context.Context, session *arpc.Session, hostname string, jobId string) *ARPCFS {
	fs := &ARPCFS{
		basePath:      "/",
		ctx:           ctx,
		session:       session,
		JobId:         jobId,
		Hostname:      hostname,
		accessedPaths: hashmap.New[bool](),
	}

	return fs
}

// trackAccess records that a path has been accessed using its xxHash
func (fs *ARPCFS) trackAccess(path string, isDir bool) {
	if _, loaded := fs.accessedPaths.GetOrSet(path, isDir); !loaded {
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

func (fs *ARPCFS) Open(filename string) (*ARPCFile, error) {
	return fs.OpenFile(filename, os.O_RDONLY, 0)
}

func (fs *ARPCFS) OpenFile(filename string, flag int, perm os.FileMode) (*ARPCFile, error) {
	if fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
		return nil, os.ErrInvalid
	}

	var resp vssfs.FileHandleId
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
	raw, err := fs.session.CallMsgWithTimeout(10*time.Second, fs.JobId+"/OpenFile", reqBytes)
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
		handleID: resp,
		jobId:    fs.JobId,
	}, nil
}

// Stat first tries the LRU cache before performing an RPC call.
func (fs *ARPCFS) Stat(filename string) (*vssfs.VSSFileInfo, error) {
	var fi vssfs.VSSFileInfo
	if fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
		return nil, os.ErrInvalid
	}

	req := vssfs.StatReq{Path: filename}
	reqBytes, err := req.MarshalMsg(nil)
	if err != nil {
		return nil, os.ErrInvalid
	}

	// Use the new CallMsgDirect helper:
	raw, err := fs.session.CallMsgWithTimeout(time.Second*10, fs.JobId+"/Stat", reqBytes)
	if err != nil {
		return nil, err
	}

	_, err = fi.UnmarshalMsg(raw)
	if err != nil {
		return nil, os.ErrInvalid
	}

	fi.Name = filepath.Base(fi.Name)

	go fs.trackAccess(filename, fi.IsDir)

	return &fi, nil
}

// StatFS tries the LRU cache before making the RPC call.
func (fs *ARPCFS) StatFS() (*vssfs.StatFS, error) {
	const statFSKey = "statFS"

	if fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
		return nil, os.ErrInvalid
	}

	var fsStat vssfs.StatFS
	raw, err := fs.session.CallMsgWithTimeout(10*time.Second, fs.JobId+"/FSstat", nil)
	if err != nil {
		syslog.L.Errorf("StatFS RPC failed: %v", err)
		return nil, err
	}

	_, err = fsStat.UnmarshalMsg(raw)
	if err != nil {
		syslog.L.Errorf("StatFS RPC failed: %v", err)
		return nil, os.ErrInvalid
	}

	return &fsStat, nil
}

// ReadDir first tries the LRU cache before performing an RPC call.
func (fs *ARPCFS) ReadDir(path string) (*vssfs.ReadDirEntries, error) {
	if fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
		return nil, os.ErrInvalid
	}

	var resp vssfs.ReadDirEntries
	req := vssfs.ReadDirReq{Path: path}
	reqBytes, err := req.MarshalMsg(nil)
	if err != nil {
		return nil, os.ErrInvalid
	}

	raw, err := fs.session.CallMsgWithTimeout(10*time.Second, fs.JobId+"/ReadDir", reqBytes)
	if err != nil {
		return nil, err
	}

	_, err = resp.UnmarshalMsg(raw)
	if err != nil {
		return nil, os.ErrInvalid
	}

	go fs.trackAccess(path, true)

	return &resp, nil
}

func (fs *ARPCFS) Join(elem ...string) string {
	return filepath.Join(elem...)
}

func (fs *ARPCFS) Chroot(path string) (*ARPCFS, error) {
	arpcfs := NewARPCFS(fs.ctx, fs.session, fs.Hostname, fs.JobId)
	arpcfs.basePath = filepath.Join(fs.basePath, path)

	return arpcfs, nil
}

func (fs *ARPCFS) Root() string {
	return fs.basePath
}
