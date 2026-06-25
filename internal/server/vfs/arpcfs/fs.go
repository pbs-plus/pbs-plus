//go:build linux

package arpcfs

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs"
)

const attrPrefix = "attr:"
const xattrPrefix = "xattr:"

// path are suppressed to avoid noisy duplicate entries in task logs.
func (fs *ARPCFS) logOnce(path string, err error, op string) {
	if isIgnoredPath(path) {
		return
	}
	if _, loaded := fs.loggedPaths.LoadOrStore(path, struct{}{}); loaded {
		return
	}
	log.Error(err,
		"FUSE "+op+" failed",
		"path", path)

}

// suppressed  -  these are files probed by proxmox-backup-client on every
func isIgnoredPath(p string) bool {
	base := p
	if idx := strings.LastIndexAny(p, "/\\"); idx >= 0 {
		base = p[idx+1:]
	}
	switch base {
	case ".pxarexclude", ".pxarexclude-cli":
		return true
	}
	return false
}

func NewARPCFS(ctx context.Context, agentManager *arpc.AgentsManager, sessionId string, hostname string, backup database.Backup, backupMode string) *ARPCFS {


	ctxFs, cancel := context.WithCancel(ctx)

	memcachePath := filepath.Join(conf.MemcachedSocketPath, fmt.Sprintf("%s.sock", backup.ID))


	stopMemLocal, err := vfs.StartMemcachedOnUnixSocket(ctxFs, vfs.MemcachedConfig{
		SocketPath:     memcachePath,
		MemoryMB:       1024,
		MaxConnections: 0,
	})
	if err != nil {
		log.Error(err, "failed to run memcached server")
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


	go func() {
		<-ctxFs.Done()


		if err := fs.Memcache.DeleteAll(); err != nil {
			log.Error(err, "")
		}
		if err := fs.Memcache.Close(); err != nil {
			log.Error(err, "")
		}

		fs.TotalBytes.Reset()
		fs.FolderCount.Reset()
		fs.FileCount.Reset()
		fs.StatCacheHits.Reset()
		if err := stopMemLocal(); err != nil {
			log.Error(err, "")
		}
	}()

	return fs
}

func (fs *ARPCFS) getPipe(ctx context.Context) (*arpc.StreamPipe, error) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	pipeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	primaryUpCount := 0

	for {
		session, exists := fs.agentManager.GetStreamPipe(fs.sessionId)
		if exists {
			return session, nil
		}

		if _, exists := fs.agentManager.GetStreamPipe(fs.Hostname); exists {
			primaryUpCount++
			if primaryUpCount > 4 {
				return nil, fmt.Errorf("primary agent for %s is reachable but the backup session is severed; agent crashed without graceful exit", fs.Hostname)
			}
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


	return fs.backupMode
}

func (fs *ARPCFS) Open(ctx context.Context, filename string) (ARPCFile, error) {


	return fs.OpenFile(ctx, filename, os.O_RDONLY, 0)
}

func (fs *ARPCFS) OpenFile(ctx context.Context, filename string, flag int, perm os.FileMode) (ARPCFile, error) {
	pipe, err := fs.getPipe(ctx)
	if err != nil {
		log.Error(err,
			"arpc session is nil")

		return ARPCFile{}, syscall.ENOENT
	}


	var resp types.FileHandleID
	req := types.OpenFileReq{
		Path: filename,
		Flag: flag,
		Perm: int(perm),
	}

	ctxN, cancelN := context.WithTimeout(ctx, 1*time.Minute)
	defer cancelN()

	raw, err := pipe.CallData(ctxN, "OpenFile", &req)
	if err != nil {
		return ARPCFile{}, fmt.Errorf("open: %w", err)
	}

	err = cbor.Unmarshal(raw, &resp)
	if err != nil {
		return ARPCFile{}, fmt.Errorf("open decode: %w", err)
	}


	return ARPCFile{
		fs:       fs,
		name:     filename,
		handleID: resp,
		backupID: fs.Backup.ID,
	}, nil
}

func (fs *ARPCFS) Attr(ctx context.Context, filename string, isLookup bool) (types.AgentFileInfo, error) {


	var fi types.AgentFileInfo
	pipe, err := fs.getPipe(ctx)
	if err != nil {
		log.Error(err,
			"arpc session is nil")

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


	} else {


		raw, err = pipe.CallData(ctxN, "Attr", &req)
		if err != nil {
			return types.AgentFileInfo{}, fmt.Errorf("stat: %w", err)
		}
		if isLookup {
			if mcErr := fs.Memcache.Set(&memcache.Item{Key: cacheKey, Value: raw, Expiration: 0}); mcErr != nil {


			}
		}
	}

	err = cbor.Unmarshal(raw, &fi)
	if err != nil {
		return types.AgentFileInfo{}, fmt.Errorf("stat decode: %w", err)
	}

	if !isLookup {
		if !fi.IsDir {
			if err := fs.Memcache.Delete(cacheKey); err != nil {
				log.Error(err, "")
			}


		}
	}

	return fi, nil
}

func (fs *ARPCFS) ListXattr(ctx context.Context, filename string) (types.AgentFileInfo, error) {


	if !fs.Backup.IncludeXattr {


		return types.AgentFileInfo{}, syscall.ENOTSUP
	}

	ctxN, cancelN := context.WithTimeout(ctx, 1*time.Minute)
	defer cancelN()

	cacheKey := fs.GetCacheKey(xattrPrefix, filename)

	var fi types.AgentFileInfo
	pipe, err := fs.getPipe(ctx)
	if err != nil {
		log.Error(err,
			"arpc session is nil")

		return types.AgentFileInfo{}, syscall.ENOTSUP
	}

	var fiCached types.AgentFileInfo
	req := types.StatReq{Path: filename}

	rawCached, err := fs.Memcache.Get(cacheKey)
	if err == nil {
		req.AclOnly = true
		if err := cbor.Unmarshal(rawCached.Value, &fiCached); err != nil {
			log.Error(err, "")
		}


	}

	raw, err := pipe.CallData(ctxN, "Xattr", &req)
	if err != nil {
		fs.logOnce(req.Path, err, "Xattr")
		return types.AgentFileInfo{}, syscall.ENOTSUP
	}

	err = cbor.Unmarshal(raw, &fi)
	if err != nil {
		fs.logOnce(req.Path, err, "Xattr")
		return types.AgentFileInfo{}, syscall.ENOTSUP
	}

	if req.AclOnly {
		fi.CreationTime = fiCached.CreationTime
		fi.LastWriteTime = fiCached.LastWriteTime
		fi.LastAccessTime = fiCached.LastAccessTime
		fi.FileAttributes = fiCached.FileAttributes


	}

	xattrBytes, err := cbor.Marshal(fi)
	if err != nil {
		return types.AgentFileInfo{}, syscall.ENOTSUP
	}

	if err := fs.Memcache.Set(&memcache.Item{Key: cacheKey, Value: xattrBytes, Expiration: 5}); err != nil {
		log.Error(err, "")
	}

	return fi, nil
}

func (fs *ARPCFS) Xattr(ctx context.Context, filename string, attr string) (types.AgentFileInfo, error) {


	if !fs.Backup.IncludeXattr {


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
		fs.logOnce(filename, err, "Xattr")
		return types.AgentFileInfo{}, syscall.ENODATA
	}

	return fiCached, nil
}

func (fs *ARPCFS) StatFS(ctx context.Context) (types.StatFS, error) {


	ctxN, cancelN := context.WithTimeout(ctx, 1*time.Minute)
	defer cancelN()

	pipe, err := fs.getPipe(ctx)
	if err != nil {
		log.Error(err,
			"arpc session is nil")

		return types.StatFS{}, syscall.ENOENT
	}

	var fsStat types.StatFS
	raw, err := pipe.CallData(ctxN, "StatFS", nil)
	if err != nil {
		log.Error(err, "")

		return types.StatFS{}, syscall.ENOENT
	}

	err = cbor.Unmarshal(raw, &fsStat)
	if err != nil {
		log.Error(err,
			"failed to handle statfs decode")

		return types.StatFS{}, syscall.ENOENT
	}


	return fsStat, nil
}

func (fs *ARPCFS) ReadDir(ctx context.Context, path string) (DirStream, error) {


	ctxN, cancelN := context.WithTimeout(ctx, 1*time.Minute)
	defer cancelN()

	cacheKey := fs.GetCacheKey(attrPrefix, path)

	pipe, err := fs.getPipe(ctx)
	if err != nil {
		log.Error(err,
			"arpc session is nil")

		return DirStream{}, syscall.ENOENT
	}

	var handleId types.FileHandleID
	openReq := types.OpenFileReq{Path: path}
	raw, err := pipe.CallData(ctxN, "OpenFile", &openReq)
	if err != nil {
		return DirStream{}, fmt.Errorf("readdir open: %w", err)
	}
	err = cbor.Unmarshal(raw, &handleId)
	if err != nil {
		return DirStream{}, fmt.Errorf("readdir decode: %w", err)
	}

	if err := fs.Memcache.Delete(cacheKey); err != nil {
		log.Error(err, "")
	}


	decOpts := cbor.DecOptions{
		MaxArrayElements: math.MaxInt32,
	}
	defaultDec, err := decOpts.DecMode()
	if err != nil {
		log.Error(err,
			"ReadDir decoder failed",
			"path", path)

		return DirStream{}, syscall.ENOENT
	}

	return DirStream{
		fs:       fs,
		path:     path,
		handleId: handleId,
		lastResp: types.ReadDirEntries{},
		cborDec:  defaultDec,
	}, nil
}

func (fs *ARPCFS) Root() string {


	return fs.BasePath
}

func (fs *ARPCFS) Unmount(ctx context.Context) {


	if fs.Fuse != nil {
		if err := fs.Fuse.Unmount(); err != nil {
			log.Error(err, "")
		}


	}

	pipe, err := fs.getPipe(ctx)
	if err != nil {
		log.Error(err, "")
	}
	if pipe != nil {
		pipe.Close()


	}
	fs.Cancel()


}
