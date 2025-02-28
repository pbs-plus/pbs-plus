//go:build linux

package arpcfs

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/vssfs"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

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

func (fs *ARPCFS) Open(filename string) (*ARPCFile, error) {
	return fs.OpenFile(filename, os.O_RDONLY, 0)
}

func (fs *ARPCFS) OpenFile(filename string, flag int, perm os.FileMode) (*ARPCFile, error) {
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
func (fs *ARPCFS) Stat(filename string) (*vssfs.VSSFileInfo, error) {
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

	fi.Name = filepath.Base(fi.Name)

	fs.trackAccess(filename, fi.IsDir)

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
	ctx, cancel := TimeoutCtx()
	defer cancel()

	raw, err := fs.session.CallMsg(ctx, fs.JobId+"/FSstat", nil)
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

	fs.trackAccess(path, true)

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
