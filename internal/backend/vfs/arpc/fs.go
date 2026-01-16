//go:build linux

package arpcfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

const attrPrefix = "attr:"
const xattrPrefix = "xattr:"

func (fs *ARPCFS) logError(fpath string, err error) {
	if !strings.HasSuffix(fpath, ".pxarexclude") {
		syslog.L.Error(err).
			WithField("path", fpath).
			WithJob(fs.Backup.ID).
			Write()
	}
}

func NewARPCFS(ctx context.Context, agentManager *arpc.AgentsManager, sessionId string, hostname string, backup storeTypes.Backup, backupMode string) *ARPCFS {
	syslog.L.Debug().
		WithMessage("NewARPCFS called").
		WithField("hostname", hostname).
		WithField("backupId", backup.ID).
		WithField("backupMode", backupMode).
		Write()

	ctxFs, cancel := context.WithCancel(ctx)

	memcachePath := filepath.Join(constants.MemcachedSocketPath, fmt.Sprintf("%s.sock", backup.ID))

	syslog.L.Debug().
		WithMessage("Starting local memcached").
		WithField("socketPath", memcachePath).
		WithField("backupId", backup.ID).
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
		VFSBase: vfs.InjectBase(vfs.VFSBase{
			BasePath: "/",
			Ctx:      ctxFs,
			Cancel:   cancel,
			Backup:   backup,
			Memcache: memcache.New(memcachePath),
		}),
		Hostname:     hostname,
		backupMode:   backupMode,
		agentManager: agentManager,
		sessionId:    sessionId,
	}

	syslog.L.Debug().
		WithMessage("ARPCFS initialized").
		WithField("backupId", fs.Backup.ID).
		WithField("hostname", fs.Hostname).
		WithField("basePath", fs.BasePath).
		Write()

	go func() {
		<-ctxFs.Done()
		syslog.L.Debug().
			WithMessage("Context done, cleaning up memcache and memlocal").
			WithField("backupId", fs.Backup.ID).
			Write()
		fs.Memcache.DeleteAll()
		fs.Memcache.Close()

		fs.TotalBytes.Reset()
		fs.FolderCount.Reset()
		fs.FileCount.Reset()
		fs.StatCacheHits.Reset()
		stopMemLocal()
	}()

	return fs
}

func (fs *ARPCFS) getPipe(ctx context.Context) (*arpc.StreamPipe, error) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	pipeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	for {
		session, exists := fs.agentManager.GetStreamPipe(fs.sessionId)
		if exists {
			return session, nil
		}

		if _, exists := fs.agentManager.GetStreamPipe(fs.Hostname); !exists {
			return nil, fmt.Errorf("primary agent for %s is unreachable", fs.Hostname)
		}

		select {
		case <-pipeCtx.Done():
			return nil, pipeCtx.Err()
		case <-ticker.C:
			continue
		}
	}
}

func (fs *ARPCFS) Context() context.Context { return fs.Ctx }

func (fs *ARPCFS) GetBackupMode() string {
	syslog.L.Debug().
		WithMessage("GetBackupMode called").
		WithField("backupId", fs.Backup.ID).
		WithField("backupMode", fs.backupMode).
		Write()
	return fs.backupMode
}

func (fs *ARPCFS) Open(ctx context.Context, filename string) (ARPCFile, error) {
	syslog.L.Debug().
		WithMessage("Open called").
		WithField("path", filename).
		WithField("backupId", fs.Backup.ID).
		Write()
	return fs.OpenFile(ctx, filename, os.O_RDONLY, 0)
}

func (fs *ARPCFS) OpenFile(ctx context.Context, filename string, flag int, perm os.FileMode) (ARPCFile, error) {
	pipe, err := fs.getPipe(ctx)
	if err != nil {
		syslog.L.Error(err).
			WithMessage("arpc session is nil").
			WithJob(fs.Backup.ID).
			Write()
		return ARPCFile{}, syscall.ENOENT
	}

	syslog.L.Debug().
		WithMessage("OpenFile called").
		WithField("path", filename).
		WithField("flag", flag).
		WithField("perm", perm).
		WithField("backupId", fs.Backup.ID).
		Write()

	var resp types.FileHandleId
	req := types.OpenFileReq{
		Path: filename,
		Flag: flag,
		Perm: int(perm),
	}

	ctxN, cancelN := context.WithTimeout(ctx, 1*time.Minute)
	defer cancelN()

	raw, err := pipe.CallData(ctxN, "OpenFile", &req)
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
		WithField("backupId", fs.Backup.ID).
		Write()

	return ARPCFile{
		fs:       fs,
		name:     filename,
		handleID: resp,
		backupId: fs.Backup.ID,
	}, nil
}

func (fs *ARPCFS) Attr(ctx context.Context, filename string, isLookup bool) (types.AgentFileInfo, error) {
	syslog.L.Debug().
		WithMessage("Attr called").
		WithField("path", filename).
		WithField("isLookup", isLookup).
		WithField("backupId", fs.Backup.ID).
		Write()

	var fi types.AgentFileInfo
	pipe, err := fs.getPipe(ctx)
	if err != nil {
		syslog.L.Error(err).
			WithMessage("arpc session is nil").
			WithJob(fs.Backup.ID).
			Write()
		return types.AgentFileInfo{}, syscall.ENOENT
	}

	cacheKey := fs.GetCacheKey(attrPrefix, filename)

	ctxN, cancelN := context.WithTimeout(ctx, 1*time.Minute)
	defer cancelN()

	req := types.StatReq{Path: filename}

	var raw []byte
	cached, err := fs.Memcache.Get(cacheKey)
	if err == nil {
		fs.StatCacheHits.Add(1)
		raw = cached.Value
		syslog.L.Debug().
			WithMessage("Attr cache hit").
			WithField("path", filename).
			WithField("backupId", fs.Backup.ID).
			Write()
	} else {
		syslog.L.Debug().
			WithMessage("Attr cache miss, issuing RPC").
			WithField("path", filename).
			WithField("backupId", fs.Backup.ID).
			Write()
		raw, err = pipe.CallData(ctxN, "Attr", &req)
		if err != nil {
			fs.logError(req.Path, err)
			return types.AgentFileInfo{}, syscall.ENOENT
		}
		if isLookup {
			if mcErr := fs.Memcache.Set(&memcache.Item{Key: cacheKey, Value: raw, Expiration: 0}); mcErr != nil {
				syslog.L.Debug().
					WithMessage("Attr cache set failed").
					WithField("path", filename).
					WithField("error", mcErr.Error()).
					WithJob(fs.Backup.ID).
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
			fs.Memcache.Delete(cacheKey)
			syslog.L.Debug().
				WithMessage("Attr counted file and cleared cache").
				WithField("path", filename).
				WithField("fileCount", fs.FileCount.Value()).
				WithJob(fs.Backup.ID).
				Write()
		}
	}

	return fi, nil
}

func (fs *ARPCFS) ListXattr(ctx context.Context, filename string) (types.AgentFileInfo, error) {
	syslog.L.Debug().
		WithMessage("ListXattr called").
		WithField("path", filename).
		WithField("backupId", fs.Backup.ID).
		Write()

	if !fs.Backup.IncludeXattr {
		syslog.L.Debug().
			WithMessage("Xattr disabled by backup").
			WithField("path", filename).
			WithField("backupId", fs.Backup.ID).
			Write()
		return types.AgentFileInfo{}, syscall.ENOTSUP
	}

	ctxN, cancelN := context.WithTimeout(ctx, 1*time.Minute)
	defer cancelN()

	cacheKey := fs.GetCacheKey(xattrPrefix, filename)

	var fi types.AgentFileInfo
	pipe, err := fs.getPipe(ctx)
	if err != nil {
		syslog.L.Error(err).
			WithMessage("arpc session is nil").
			WithJob(fs.Backup.ID).
			Write()
		return types.AgentFileInfo{}, syscall.ENOTSUP
	}

	var fiCached types.AgentFileInfo
	req := types.StatReq{Path: filename}

	rawCached, err := fs.Memcache.Get(cacheKey)
	if err == nil {
		req.AclOnly = true
		_ = cbor.Unmarshal(rawCached.Value, &fiCached)
		syslog.L.Debug().
			WithMessage("Xattr cache hit for metadata").
			WithField("path", filename).
			WithField("backupId", fs.Backup.ID).
			Write()
	}

	raw, err := pipe.CallData(ctxN, "Xattr", &req)
	if err != nil {
		fs.logError(req.Path, err)
		return types.AgentFileInfo{}, syscall.ENOTSUP
	}

	err = cbor.Unmarshal(raw, &fi)
	if err != nil {
		fs.logError(req.Path, err)
		return types.AgentFileInfo{}, syscall.ENOTSUP
	}

	if req.AclOnly {
		fi.CreationTime = fiCached.CreationTime
		fi.LastWriteTime = fiCached.LastWriteTime
		fi.LastAccessTime = fiCached.LastAccessTime
		fi.FileAttributes = fiCached.FileAttributes
		syslog.L.Debug().
			WithMessage("Xattr merged cached timestamps/attributes").
			WithField("path", filename).
			WithField("backupId", fs.Backup.ID).
			Write()
	}

	xattrBytes, err := cbor.Marshal(fi)
	if err != nil {
		return types.AgentFileInfo{}, syscall.ENOTSUP
	}

	fs.Memcache.Set(&memcache.Item{Key: cacheKey, Value: xattrBytes, Expiration: 5})

	return fi, nil
}

func (fs *ARPCFS) Xattr(ctx context.Context, filename string, attr string) (types.AgentFileInfo, error) {
	syslog.L.Debug().
		WithMessage("Xattr called").
		WithField("path", filename).
		WithField("attr", attr).
		WithField("backupId", fs.Backup.ID).
		Write()

	if !fs.Backup.IncludeXattr {
		syslog.L.Debug().
			WithMessage("Xattr disabled by backup").
			WithField("path", filename).
			WithField("backupId", fs.Backup.ID).
			Write()
		return types.AgentFileInfo{}, syscall.ENOTSUP
	}

	cacheKey := fs.GetCacheKey(xattrPrefix, filename)

	var fiCached types.AgentFileInfo
	rawCached, err := fs.Memcache.Get(cacheKey)
	if err != nil {
		return fs.ListXattr(ctx, filename)
	}

	err = cbor.Unmarshal(rawCached.Value, &fiCached)
	if err != nil {
		fs.logError(filename, err)
		return types.AgentFileInfo{}, syscall.ENODATA
	}

	return fiCached, nil
}

func (fs *ARPCFS) StatFS(ctx context.Context) (types.StatFS, error) {
	syslog.L.Debug().
		WithMessage("StatFS called").
		WithField("backupId", fs.Backup.ID).
		Write()

	ctxN, cancelN := context.WithTimeout(ctx, 1*time.Minute)
	defer cancelN()

	pipe, err := fs.getPipe(ctx)
	if err != nil {
		syslog.L.Error(err).
			WithMessage("arpc session is nil").
			WithJob(fs.Backup.ID).
			Write()
		return types.StatFS{}, syscall.ENOENT
	}

	var fsStat types.StatFS
	raw, err := pipe.CallData(ctxN,
		fs.Backup.ID+"/StatFS", nil)
	if err != nil {
		syslog.L.Error(err).
			WithJob(fs.Backup.ID).
			Write()
		return types.StatFS{}, syscall.ENOENT
	}

	err = cbor.Unmarshal(raw, &fsStat)
	if err != nil {
		syslog.L.Error(err).
			WithMessage("failed to handle statfs decode").
			WithJob(fs.Backup.ID).
			Write()
		return types.StatFS{}, syscall.ENOENT
	}

	syslog.L.Debug().
		WithMessage("StatFS completed").
		WithField("backupId", fs.Backup.ID).
		Write()

	return fsStat, nil
}

func (fs *ARPCFS) ReadDir(ctx context.Context, path string) (DirStream, error) {
	syslog.L.Debug().
		WithMessage("ReadDir called").
		WithField("path", path).
		WithField("backupId", fs.Backup.ID).
		Write()

	ctxN, cancelN := context.WithTimeout(ctx, 1*time.Minute)
	defer cancelN()

	cacheKey := fs.GetCacheKey(attrPrefix, path)

	pipe, err := fs.getPipe(ctx)
	if err != nil {
		syslog.L.Error(err).
			WithMessage("arpc session is nil").
			WithJob(fs.Backup.ID).
			Write()
		return DirStream{}, syscall.ENOENT
	}

	var handleId types.FileHandleId
	openReq := types.OpenFileReq{Path: path}
	raw, err := pipe.CallData(ctxN, "OpenFile", &openReq)
	if err != nil {
		syslog.L.Error(err).
			WithMessage("ReadDir open failed").
			WithField("path", path).
			WithJob(fs.Backup.ID).
			Write()
		return DirStream{}, syscall.ENOENT
	}
	err = cbor.Unmarshal(raw, &handleId)
	if err != nil {
		syslog.L.Error(err).
			WithMessage("ReadDir handle decode failed").
			WithField("path", path).
			WithJob(fs.Backup.ID).
			Write()
		return DirStream{}, syscall.ENOENT
	}

	fs.Memcache.Delete(cacheKey)

	syslog.L.Debug().
		WithMessage("ReadDir opened directory").
		WithField("path", path).
		WithField("handleId", handleId).
		WithJob(fs.Backup.ID).
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
		WithField("backupId", fs.Backup.ID).
		Write()
	return fs.BasePath
}

func (fs *ARPCFS) Unmount(ctx context.Context) {
	syslog.L.Debug().
		WithMessage("Unmount called").
		WithField("backupId", fs.Backup.ID).
		Write()

	if fs.Fuse != nil {
		_ = fs.Fuse.Unmount()
		syslog.L.Debug().
			WithMessage("Fuse unmounted").
			WithField("backupId", fs.Backup.ID).
			Write()
	}

	pipe, _ := fs.getPipe(ctx)
	if pipe != nil {
		pipe.Close()
		syslog.L.Debug().
			WithMessage("ARPC session closed").
			WithField("backupId", fs.Backup.ID).
			Write()
	}
	fs.Cancel()
	syslog.L.Debug().
		WithMessage("Context canceled").
		WithField("backupId", fs.Backup.ID).
		Write()
}
