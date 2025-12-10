//go:build linux

package arpcfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/backend/vfs"
	"github.com/pbs-plus/pbs-plus/internal/memlocal"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	storeTypes "github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

func (fs *ARPCFS) logError(fpath string, err error) {
	if !strings.HasSuffix(fpath, ".pxarexclude") {
		syslog.L.Error(err).
			WithField("path", fpath).
			WithJob(fs.Job.ID).
			Write()
	}
}

var _ vfs.FS = (*ARPCFS)(nil)

func NewARPCFS(ctx context.Context, session *arpc.Session, hostname string, job storeTypes.Job, backupMode string) *ARPCFS {
	ctxFs, cancel := context.WithCancel(ctx)

	memcachePath := filepath.Join(constants.MemcachedSocketPath, fmt.Sprintf("%s.sock", job.ID))

	stopMemLocal, err := memlocal.StartMemcachedOnUnixSocket(ctxFs, memlocal.MemcachedConfig{
		SocketPath:     memcachePath,
		MemoryMB:       1024,
		MaxConnections: 0,
	})
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to run memcached server").Write()
		cancel()
		return nil
	}

	fs := &ARPCFS{
		basePath:   "/",
		ctx:        ctxFs,
		cancel:     cancel,
		session:    session,
		Job:        job,
		Hostname:   hostname,
		backupMode: backupMode,
		memcache:   memcache.New(memcachePath),
	}

	go func() {
		<-ctxFs.Done()
		fs.memcache.DeleteAll()
		fs.memcache.Close()
		stopMemLocal()
	}()

	return fs
}

func (fs *ARPCFS) Context() context.Context { return fs.ctx }

// GetStats returns a snapshot of all access and byte-read statistics.
func (fs *ARPCFS) GetStats() vfs.Stats {
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

	return vfs.Stats{
		FilesAccessed:   currentFileCount,
		FoldersAccessed: currentFolderCount,
		TotalAccessed:   totalAccessed,
		FileAccessSpeed: accessSpeed,
		TotalBytes:      uint64(currentTotalBytes),
		ByteReadSpeed:   bytesSpeed,
		StatCacheHits:   atomic.LoadInt64(&fs.statCacheHits),
	}
}

func (fs *ARPCFS) GetBackupMode() string {
	return fs.backupMode
}

func (fs *ARPCFS) Open(filename string) (vfs.FileHandle, error) {
	return fs.OpenFile(filename, os.O_RDONLY, 0)
}

func (fs *ARPCFS) OpenFile(filename string, flag int, perm os.FileMode) (vfs.FileHandle, error) {
	if fs.session == nil {
		syslog.L.Error(os.ErrInvalid).
			WithMessage("arpc session is nil").
			WithJob(fs.Job.ID).
			Write()
		return nil, syscall.ENOENT
	}

	var resp types.FileHandleId
	req := types.OpenFileReq{
		Path: filename,
		Flag: flag,
		Perm: int(perm),
	}

	raw, err := fs.session.CallMsgWithTimeout(1*time.Minute, fs.Job.ID+"/OpenFile", &req)
	if err != nil {
		fs.logError(req.Path, err)
		return nil, syscall.ENOENT
	}

	err = resp.Decode(raw)
	if err != nil {
		fs.logError(req.Path, err)
		return nil, syscall.ENOENT
	}

	return &ARPCFile{
		fs:       fs,
		name:     filename,
		handleID: resp,
		jobId:    fs.Job.ID,
	}, nil
}

// Attr retrieves file attributes via RPC and then tracks the access.
func (fs *ARPCFS) Attr(filename string, isLookup bool) (types.AgentFileInfo, error) {
	var fi types.AgentFileInfo
	if fs.session == nil {
		syslog.L.Error(os.ErrInvalid).
			WithMessage("arpc session is nil").
			WithJob(fs.Job.ID).
			Write()
		return types.AgentFileInfo{}, syscall.ENOENT
	}

	req := types.StatReq{Path: filename}

	var raw []byte
	cached, err := fs.memcache.Get("attr:" + filename)
	if err == nil {
		atomic.AddInt64(&fs.statCacheHits, 1)
		raw = cached.Value
	} else {
		raw, err = fs.session.CallMsgWithTimeout(1*time.Minute, fs.Job.ID+"/Attr", &req)
		if err != nil {
			fs.logError(req.Path, err)
			return types.AgentFileInfo{}, syscall.ENOENT
		}
		if isLookup {
			_ = fs.memcache.Set(&memcache.Item{Key: "attr:" + filename, Value: raw, Expiration: 0})
		}
	}

	err = fi.Decode(raw)
	if err != nil {
		fs.logError(req.Path, err)
		return types.AgentFileInfo{}, syscall.ENOENT
	}

	if !isLookup {
		if fi.IsDir {
			atomic.AddInt64(&fs.folderCount, 1)
		} else {
			fs.memcache.Delete("attr:" + filename)
			atomic.AddInt64(&fs.fileCount, 1)
		}
	}

	return fi, nil
}

// Xattr retrieves extended attributes and logs the access similarly.
func (fs *ARPCFS) Xattr(filename string) (types.AgentFileInfo, error) {
	var fi types.AgentFileInfo
	if fs.session == nil {
		syslog.L.Error(os.ErrInvalid).
			WithMessage("arpc session is nil").
			WithJob(fs.Job.ID).
			Write()
		return types.AgentFileInfo{}, syscall.ENODATA
	}

	var fiCached types.AgentFileInfo
	req := types.StatReq{Path: filename}

	rawCached, err := fs.memcache.Get("xattr:" + filename)
	if err == nil {
		req.AclOnly = true
		_ = fiCached.Decode(rawCached.Value)
		fs.memcache.Delete("xattr:" + filename)
	}

	raw, err := fs.session.CallMsgWithTimeout(1*time.Minute, fs.Job.ID+"/Xattr", &req)
	if err != nil {
		fs.logError(req.Path, err)
		return types.AgentFileInfo{}, syscall.ENODATA
	}

	err = fi.Decode(raw)
	if err != nil {
		fs.logError(req.Path, err)
		return types.AgentFileInfo{}, syscall.ENODATA
	}

	if req.AclOnly {
		fi.CreationTime = fiCached.CreationTime
		fi.LastWriteTime = fiCached.LastWriteTime
		fi.LastAccessTime = fiCached.LastAccessTime
		fi.FileAttributes = fiCached.FileAttributes
	}

	return fi, nil
}

// StatFS calls StatFS via RPC.
func (fs *ARPCFS) StatFS() (types.StatFS, error) {
	if fs.session == nil {
		syslog.L.Error(os.ErrInvalid).
			WithMessage("arpc session is nil").
			WithJob(fs.Job.ID).
			Write()
		return types.StatFS{}, syscall.ENOENT
	}

	var fsStat types.StatFS
	raw, err := fs.session.CallMsgWithTimeout(1*time.Minute,
		fs.Job.ID+"/StatFS", nil)
	if err != nil {
		syslog.L.Error(err).
			WithJob(fs.Job.ID).
			Write()
		return types.StatFS{}, syscall.ENOENT
	}

	err = fsStat.Decode(raw)
	if err != nil {
		syslog.L.Error(err).
			WithMessage("failed to handle statfs decode").
			WithJob(fs.Job.ID).
			Write()
		return types.StatFS{}, syscall.ENOENT
	}

	return fsStat, nil
}

// ReadDir calls ReadDir via RPC and logs directory accesses.
func (fs *ARPCFS) ReadDir(path string) (vfs.DirStream, error) {
	if fs.session == nil {
		syslog.L.Error(os.ErrInvalid).
			WithMessage("arpc session is nil").
			WithJob(fs.Job.ID).
			Write()
		return nil, syscall.ENOENT
	}

	var handleId types.FileHandleId
	openReq := types.OpenFileReq{Path: path}
	raw, err := fs.session.CallMsgWithTimeout(1*time.Minute, fs.Job.ID+"/OpenFile", &openReq)
	if err != nil {
		return nil, syscall.ENOENT
	}
	err = handleId.Decode(raw)
	if err != nil {
		return nil, syscall.ENOENT
	}

	fs.memcache.Delete("attr:" + path)

	return &DirStream{
		fs:       fs,
		path:     path,
		handleId: handleId,
		lastResp: types.ReadDirEntries{},
	}, nil
}

func (fs *ARPCFS) Root() string {
	return fs.basePath
}
