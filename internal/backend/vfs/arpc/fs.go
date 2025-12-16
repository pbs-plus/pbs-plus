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
	"github.com/fxamacker/cbor/v2"
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
	syslog.L.Debug().
		WithMessage("NewARPCFS called").
		WithField("hostname", hostname).
		WithField("jobId", job.ID).
		WithField("backupMode", backupMode).
		Write()

	ctxFs, cancel := context.WithCancel(ctx)

	memcachePath := filepath.Join(constants.MemcachedSocketPath, fmt.Sprintf("%s.sock", job.ID))

	syslog.L.Debug().
		WithMessage("Starting local memcached").
		WithField("socketPath", memcachePath).
		WithField("jobId", job.ID).
		Write()

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

	syslog.L.Debug().
		WithMessage("ARPCFS initialized").
		WithField("jobId", fs.Job.ID).
		WithField("hostname", fs.Hostname).
		WithField("basePath", fs.BasePath).
		Write()

	go func() {
		<-ctxFs.Done()
		syslog.L.Debug().
			WithMessage("Context done, cleaning up memcache and memlocal").
			WithField("jobId", fs.Job.ID).
			Write()
		fs.Memcache.DeleteAll()
		fs.Memcache.Close()
		stopMemLocal()
	}()

	return fs
}

func (fs *ARPCFS) Context() context.Context { return fs.Ctx }

func (fs *ARPCFS) GetBackupMode() string {
	syslog.L.Debug().
		WithMessage("GetBackupMode called").
		WithField("jobId", fs.Job.ID).
		WithField("backupMode", fs.backupMode).
		Write()
	return fs.backupMode
}

func (fs *ARPCFS) Open(ctx context.Context, filename string) (ARPCFile, error) {
	syslog.L.Debug().
		WithMessage("Open called").
		WithField("path", filename).
		WithField("jobId", fs.Job.ID).
		Write()
	return fs.OpenFile(ctx, filename, os.O_RDONLY, 0)
}

func (fs *ARPCFS) OpenFile(ctx context.Context, filename string, flag int, perm os.FileMode) (ARPCFile, error) {
	if fs.session == nil {
		syslog.L.Error(os.ErrInvalid).
			WithMessage("arpc session is nil").
			WithJob(fs.Job.ID).
			Write()
		return ARPCFile{}, syscall.ENOENT
	}

	syslog.L.Debug().
		WithMessage("OpenFile called").
		WithField("path", filename).
		WithField("flag", flag).
		WithField("perm", perm).
		WithField("jobId", fs.Job.ID).
		Write()

	var resp types.FileHandleId
	req := types.OpenFileReq{
		Path: filename,
		Flag: flag,
		Perm: int(perm),
	}

	ctxN, cancelN := context.WithTimeout(ctx, 1*time.Minute)
	defer cancelN()

	raw, err := fs.session.CallData(ctxN, fs.Job.ID+"/OpenFile", &req)
	if err != nil {
		fs.logError(req.Path, err)
		return ARPCFile{}, syscall.ENOENT
	}

	err = cbor.Unmarshal(raw, &resp)
	if err != nil {
		fs.logError(req.Path, err)
		return ARPCFile{}, syscall.ENOENT
	}

	syslog.L.Debug().
		WithMessage("OpenFile succeeded").
		WithField("path", filename).
		WithField("handleID", resp).
		WithField("jobId", fs.Job.ID).
		Write()

	return ARPCFile{
		fs:       fs,
		name:     filename,
		handleID: resp,
		jobId:    fs.Job.ID,
	}, nil
}

func (fs *ARPCFS) Attr(ctx context.Context, filename string, isLookup bool) (types.AgentFileInfo, error) {
	syslog.L.Debug().
		WithMessage("Attr called").
		WithField("path", filename).
		WithField("isLookup", isLookup).
		WithField("jobId", fs.Job.ID).
		Write()

	var fi types.AgentFileInfo
	if fs.session == nil {
		syslog.L.Error(os.ErrInvalid).
			WithMessage("arpc session is nil").
			WithJob(fs.Job.ID).
			Write()
		return types.AgentFileInfo{}, syscall.ENOENT
	}

	ctxN, cancelN := context.WithTimeout(ctx, 1*time.Minute)
	defer cancelN()

	req := types.StatReq{Path: filename}

	var raw []byte
	cached, err := fs.Memcache.Get("attr:" + memlocal.Key(filename))
	if err == nil {
		atomic.AddInt64(&fs.StatCacheHits, 1)
		raw = cached.Value
		syslog.L.Debug().
			WithMessage("Attr cache hit").
			WithField("path", filename).
			WithField("jobId", fs.Job.ID).
			Write()
	} else {
		syslog.L.Debug().
			WithMessage("Attr cache miss, issuing RPC").
			WithField("path", filename).
			WithField("jobId", fs.Job.ID).
			Write()
		raw, err = fs.session.CallData(ctxN, fs.Job.ID+"/Attr", &req)
		if err != nil {
			fs.logError(req.Path, err)
			return types.AgentFileInfo{}, syscall.ENOENT
		}
		if isLookup {
			if mcErr := fs.Memcache.Set(&memcache.Item{Key: "attr:" + memlocal.Key(filename), Value: raw, Expiration: 0}); mcErr != nil {
				syslog.L.Debug().
					WithMessage("Attr cache set failed").
					WithField("path", filename).
					WithField("error", mcErr.Error()).
					WithJob(fs.Job.ID).
					Write()
			}
		}
	}

	err = cbor.Unmarshal(raw, &fi)
	if err != nil {
		fs.logError(req.Path, err)
		return types.AgentFileInfo{}, syscall.ENOENT
	}

	if !isLookup {
		if !fi.IsDir {
			fs.Memcache.Delete("attr:" + memlocal.Key(filename))
			syslog.L.Debug().
				WithMessage("Attr counted file and cleared cache").
				WithField("path", filename).
				WithField("fileCount", atomic.LoadInt64(&fs.FileCount)).
				WithJob(fs.Job.ID).
				Write()
		}
	}

	return fi, nil
}

func (fs *ARPCFS) Xattr(ctx context.Context, filename string) (types.AgentFileInfo, error) {
	syslog.L.Debug().
		WithMessage("Xattr called").
		WithField("path", filename).
		WithField("jobId", fs.Job.ID).
		Write()

	if !fs.Job.IncludeXattr {
		syslog.L.Debug().
			WithMessage("Xattr disabled by job").
			WithField("path", filename).
			WithField("jobId", fs.Job.ID).
			Write()
		return types.AgentFileInfo{}, syscall.ENOTSUP
	}

	ctxN, cancelN := context.WithTimeout(ctx, 1*time.Minute)
	defer cancelN()

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

	rawCached, err := fs.Memcache.Get("xattr:" + memlocal.Key(filename))
	if err == nil {
		req.AclOnly = true
		_ = cbor.Unmarshal(rawCached.Value, &fiCached)
		fs.Memcache.Delete("xattr:" + memlocal.Key(filename))
		syslog.L.Debug().
			WithMessage("Xattr cache hit for metadata").
			WithField("path", filename).
			WithField("jobId", fs.Job.ID).
			Write()
	}

	raw, err := fs.session.CallData(ctxN, fs.Job.ID+"/Xattr", &req)
	if err != nil {
		fs.logError(req.Path, err)
		return types.AgentFileInfo{}, syscall.ENODATA
	}

	err = cbor.Unmarshal(raw, &fi)
	if err != nil {
		fs.logError(req.Path, err)
		return types.AgentFileInfo{}, syscall.ENODATA
	}

	if req.AclOnly {
		fi.CreationTime = fiCached.CreationTime
		fi.LastWriteTime = fiCached.LastWriteTime
		fi.LastAccessTime = fiCached.LastAccessTime
		fi.FileAttributes = fiCached.FileAttributes
		syslog.L.Debug().
			WithMessage("Xattr merged cached timestamps/attributes").
			WithField("path", filename).
			WithField("jobId", fs.Job.ID).
			Write()
	}

	return fi, nil
}

func (fs *ARPCFS) StatFS(ctx context.Context) (types.StatFS, error) {
	syslog.L.Debug().
		WithMessage("StatFS called").
		WithField("jobId", fs.Job.ID).
		Write()

	ctxN, cancelN := context.WithTimeout(ctx, 1*time.Minute)
	defer cancelN()

	if fs.session == nil {
		syslog.L.Error(os.ErrInvalid).
			WithMessage("arpc session is nil").
			WithJob(fs.Job.ID).
			Write()
		return types.StatFS{}, syscall.ENOENT
	}

	var fsStat types.StatFS
	raw, err := fs.session.CallData(ctxN,
		fs.Job.ID+"/StatFS", nil)
	if err != nil {
		syslog.L.Error(err).
			WithJob(fs.Job.ID).
			Write()
		return types.StatFS{}, syscall.ENOENT
	}

	err = cbor.Unmarshal(raw, &fsStat)
	if err != nil {
		syslog.L.Error(err).
			WithMessage("failed to handle statfs decode").
			WithJob(fs.Job.ID).
			Write()
		return types.StatFS{}, syscall.ENOENT
	}

	syslog.L.Debug().
		WithMessage("StatFS completed").
		WithField("jobId", fs.Job.ID).
		Write()

	return fsStat, nil
}

func (fs *ARPCFS) ReadDir(ctx context.Context, path string) (DirStream, error) {
	syslog.L.Debug().
		WithMessage("ReadDir called").
		WithField("path", path).
		WithField("jobId", fs.Job.ID).
		Write()

	ctxN, cancelN := context.WithTimeout(ctx, 1*time.Minute)
	defer cancelN()

	if fs.session == nil {
		syslog.L.Error(os.ErrInvalid).
			WithMessage("arpc session is nil").
			WithJob(fs.Job.ID).
			Write()
		return DirStream{}, syscall.ENOENT
	}

	var handleId types.FileHandleId
	openReq := types.OpenFileReq{Path: path}
	raw, err := fs.session.CallData(ctxN, fs.Job.ID+"/OpenFile", &openReq)
	if err != nil {
		syslog.L.Error(err).
			WithMessage("ReadDir open failed").
			WithField("path", path).
			WithJob(fs.Job.ID).
			Write()
		return DirStream{}, syscall.ENOENT
	}
	err = cbor.Unmarshal(raw, &handleId)
	if err != nil {
		syslog.L.Error(err).
			WithMessage("ReadDir handle decode failed").
			WithField("path", path).
			WithJob(fs.Job.ID).
			Write()
		return DirStream{}, syscall.ENOENT
	}

	fs.Memcache.Delete("attr:" + memlocal.Key(path))

	syslog.L.Debug().
		WithMessage("ReadDir opened directory").
		WithField("path", path).
		WithField("handleId", handleId).
		WithJob(fs.Job.ID).
		Write()

	return DirStream{
		fs:       fs,
		path:     path,
		handleId: handleId,
		lastResp: types.ReadDirEntries{},
	}, nil
}

func (fs *ARPCFS) Root() string {
	syslog.L.Debug().
		WithMessage("Root called").
		WithField("basePath", fs.BasePath).
		WithField("jobId", fs.Job.ID).
		Write()
	return fs.BasePath
}

func (fs *ARPCFS) Unmount(ctx context.Context) {
	syslog.L.Debug().
		WithMessage("Unmount called").
		WithField("jobId", fs.Job.ID).
		Write()

	if fs.Fuse != nil {
		_ = fs.Fuse.Unmount()
		syslog.L.Debug().
			WithMessage("Fuse unmounted").
			WithField("jobId", fs.Job.ID).
			Write()
	}
	if fs.session != nil {
		_ = fs.session.Close()
		syslog.L.Debug().
			WithMessage("ARPC session closed").
			WithField("jobId", fs.Job.ID).
			Write()
	}
	fs.Cancel()
	syslog.L.Debug().
		WithMessage("Context canceled").
		WithField("jobId", fs.Job.ID).
		Write()
}
