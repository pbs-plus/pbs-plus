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

func NewARPCFS(ctx context.Context, session *arpc.StreamPipe, hostname string, job storeTypes.Job, backupMode string) *ARPCFS {
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
		VFSBase: &vfs.VFSBase{
			BasePath: "/",
			Ctx:      ctxFs,
			Cancel:   cancel,
			Job:      job,
			Memcache: memcache.New(memcachePath),
		},
		session:    session,
		Hostname:   hostname,
		backupMode: backupMode,
	}

	go func() {
		<-ctxFs.Done()
		fs.Memcache.DeleteAll()
		fs.Memcache.Close()
		stopMemLocal()
	}()

	return fs
}

func (fs *ARPCFS) Context() context.Context { return fs.Ctx }

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
			WithJob(fs.Job.ID).
			Write()
		return ARPCFile{}, syscall.ENOENT
	}

	var resp types.FileHandleId
	req := types.OpenFileReq{
		Path: filename,
		Flag: flag,
		Perm: int(perm),
	}

	raw, err := fs.session.CallDataWithTimeout(1*time.Minute, fs.Job.ID+"/OpenFile", &req)
	if err != nil {
		fs.logError(req.Path, err)
		return ARPCFile{}, syscall.ENOENT
	}

	err = resp.Decode(raw)
	if err != nil {
		fs.logError(req.Path, err)
		return ARPCFile{}, syscall.ENOENT
	}

	return ARPCFile{
		fs:       fs,
		name:     filename,
		handleID: resp,
		jobId:    fs.Job.ID,
	}, nil
}

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
	cached, err := fs.Memcache.Get("attr:" + filename)
	if err == nil {
		atomic.AddInt64(&fs.StatCacheHits, 1)
		raw = cached.Value
	} else {
		raw, err = fs.session.CallDataWithTimeout(1*time.Minute, fs.Job.ID+"/Attr", &req)
		if err != nil {
			fs.logError(req.Path, err)
			return types.AgentFileInfo{}, syscall.ENOENT
		}
		if isLookup {
			_ = fs.Memcache.Set(&memcache.Item{Key: "attr:" + filename, Value: raw, Expiration: 0})
		}
	}

	err = fi.Decode(raw)
	if err != nil {
		fs.logError(req.Path, err)
		return types.AgentFileInfo{}, syscall.ENOENT
	}

	if !isLookup {
		if fi.IsDir {
			atomic.AddInt64(&fs.FolderCount, 1)
		} else {
			fs.Memcache.Delete("attr:" + filename)
			atomic.AddInt64(&fs.FileCount, 1)
		}
	}

	return fi, nil
}

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

	rawCached, err := fs.Memcache.Get("xattr:" + filename)
	if err == nil {
		req.AclOnly = true
		_ = fiCached.Decode(rawCached.Value)
		fs.Memcache.Delete("xattr:" + filename)
	}

	raw, err := fs.session.CallDataWithTimeout(1*time.Minute, fs.Job.ID+"/Xattr", &req)
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

func (fs *ARPCFS) StatFS() (types.StatFS, error) {
	if fs.session == nil {
		syslog.L.Error(os.ErrInvalid).
			WithMessage("arpc session is nil").
			WithJob(fs.Job.ID).
			Write()
		return types.StatFS{}, syscall.ENOENT
	}

	var fsStat types.StatFS
	raw, err := fs.session.CallDataWithTimeout(1*time.Minute,
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

func (fs *ARPCFS) ReadDir(path string) (DirStream, error) {
	if fs.session == nil {
		syslog.L.Error(os.ErrInvalid).
			WithMessage("arpc session is nil").
			WithJob(fs.Job.ID).
			Write()
		return DirStream{}, syscall.ENOENT
	}

	var handleId types.FileHandleId
	openReq := types.OpenFileReq{Path: path}
	raw, err := fs.session.CallDataWithTimeout(1*time.Minute, fs.Job.ID+"/OpenFile", &openReq)
	if err != nil {
		return DirStream{}, syscall.ENOENT
	}
	err = handleId.Decode(raw)
	if err != nil {
		return DirStream{}, syscall.ENOENT
	}

	fs.Memcache.Delete("attr:" + path)

	return DirStream{
		fs:       fs,
		path:     path,
		handleId: handleId,
		lastResp: types.ReadDirEntries{},
	}, nil
}

func (fs *ARPCFS) Root() string {
	return fs.BasePath
}

func (fs *ARPCFS) Unmount() {
	if fs.Fuse != nil {
		_ = fs.Fuse.Unmount()
	}
	if fs.session != nil {
		_ = fs.session.Close()
	}
	fs.Cancel()
}
