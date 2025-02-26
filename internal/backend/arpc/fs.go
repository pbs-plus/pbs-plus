//go:build linux

package arpcfs

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-git/go-billy/v5"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/backend/arpc/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

var _ billy.Filesystem = (*ARPCFS)(nil)

func NewARPCFS(ctx context.Context, session *arpc.Session, hostname string, drive string) *ARPCFS {
	statC, err := lru.New[string, statCacheEntry](1024)
	if err != nil {
		panic(err)
	}
	readDirC, err := lru.New[string, readDirCacheEntry](1024)
	if err != nil {
		panic(err)
	}
	statFSC, err := lru.New[string, statFSCacheEntry](1)
	if err != nil {
		panic(err)
	}

	return &ARPCFS{
		ctx:          ctx,
		session:      session,
		Drive:        drive,
		Hostname:     hostname,
		statCache:    statC,
		readDirCache: readDirC,
		statFSCache:  statFSC,
	}
}

func (f *ARPCFS) Unmount() {
	if f.Mount != nil {
		_ = f.Mount.Unmount()
	}
}

var _ billy.File = (*ARPCFile)(nil)

func (fs *ARPCFS) Create(filename string) (billy.File, error) {
	return nil, os.ErrInvalid
}

func (fs *ARPCFS) Open(filename string) (billy.File, error) {
	return fs.OpenFile(filename, os.O_RDONLY, 0)
}

func (fs *ARPCFS) OpenFile(filename string, flag int,
	perm os.FileMode) (billy.File, error) {
	if flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0 {
		return nil, os.ErrInvalid
	}

	var resp struct {
		HandleID uint64 `json:"handleID"`
	}

	if fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
		return nil, os.ErrInvalid
	}

	ctx, cancel := TimeoutCtx()
	defer cancel()

	err := fs.session.CallJSON(ctx, fs.Drive+"/OpenFile", OpenRequest{
		Path: filename,
		Flag: flag,
		Perm: int(perm),
	}, &resp)
	if err != nil {
		syslog.L.Errorf("OpenFile RPC failed (%s): %v", filename, err)
		if strings.Contains(err.Error(), "not found") {
			return nil, os.ErrNotExist
		}
		return nil, os.ErrInvalid
	}

	return &ARPCFile{
		fs:       fs,
		name:     filename,
		handleID: resp.HandleID,
		drive:    fs.Drive,
	}, nil
}

// Stat first tries the LRU cache before performing an RPC call.
func (fs *ARPCFS) Stat(filename string) (os.FileInfo, error) {
	// Attempt to fetch from the LRU cache.
	fs.statCacheMu.Lock()
	if entry, ok := fs.statCache.Get(filename); ok {
		fs.statCacheMu.Unlock()
		return entry.info, nil
	}
	fs.statCacheMu.Unlock()

	// Cache miss or expired; perform RPC.
	var fi FileInfoResponse
	if fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
		return nil, os.ErrInvalid
	}

	ctx, cancel := TimeoutCtx()
	defer cancel()

	err := fs.session.CallJSON(ctx, fs.Drive+"/Stat",
		struct {
			Path string `json:"path"`
		}{Path: filename}, &fi)
	if err != nil {
		syslog.L.Errorf("Stat RPC failed (%s): %v", filename, err)
		if strings.Contains(err.Error(), "file not found") {
			return nil, os.ErrNotExist
		}
		return nil, err
	}

	modTime := time.Unix(fi.ModTimeUnix, 0)
	info := &fileInfo{
		name:    filepath.Base(filename),
		size:    fi.Size,
		mode:    fi.Mode,
		modTime: modTime,
		isDir:   fi.IsDir,
	}

	// Update the LRU cache.
	fs.statCacheMu.Lock()
	fs.statCache.Add(filename, statCacheEntry{
		info: info,
	})
	fs.statCacheMu.Unlock()

	return info, nil
}

// StatFS tries the LRU cache before making the RPC call.
func (fs *ARPCFS) StatFS() (types.StatFS, error) {
	const statFSKey = "statFS"

	fs.statFSCacheMu.Lock()
	if entry, ok := fs.statFSCache.Get(statFSKey); ok {
		stat := entry.stat
		fs.statFSCacheMu.Unlock()
		return stat, nil
	}
	fs.statFSCacheMu.Unlock()

	if fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
		return types.StatFS{}, os.ErrInvalid
	}

	var fsStat utils.FSStat
	ctx, cancel := TimeoutCtx()
	defer cancel()

	err := fs.session.CallJSON(ctx, fs.Drive+"/FSstat", struct{}{}, &fsStat)
	if err != nil {
		syslog.L.Errorf("StatFS RPC failed: %v", err)
		return types.StatFS{}, os.ErrInvalid
	}

	stat := types.StatFS{
		Bsize:   uint64(4096), // Standard block size.
		Blocks:  uint64(fsStat.TotalSize / 4096),
		Bfree:   uint64(fsStat.FreeSize / 4096),
		Bavail:  uint64(fsStat.AvailableSize / 4096),
		Files:   uint64(fsStat.TotalFiles),
		Ffree:   uint64(fsStat.FreeFiles),
		NameLen: 255, // Typically supports long filenames.
	}

	fs.statFSCacheMu.Lock()
	fs.statFSCache.Add(statFSKey, statFSCacheEntry{
		stat: stat,
	})
	fs.statFSCacheMu.Unlock()

	return stat, nil
}

// ReadDir first tries the LRU cache before performing an RPC call.
func (fs *ARPCFS) ReadDir(path string) ([]os.FileInfo, error) {
	fs.readDirCacheMu.Lock()
	if entry, ok := fs.readDirCache.Get(path); ok {
		fs.readDirCacheMu.Unlock()
		return entry.entries, nil
	}
	fs.readDirCacheMu.Unlock()

	if fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
		return nil, os.ErrInvalid
	}

	var resp ReadDirResponse
	ctx, cancel := TimeoutCtx()
	defer cancel()

	err := fs.session.CallJSON(ctx, fs.Drive+"/ReadDir", struct {
		Path string `json:"path"`
	}{Path: path}, &resp)
	if err != nil {
		syslog.L.Errorf("ReadDir RPC failed: %v", err)
		if strings.Contains(err.Error(), "not found") {
			return nil, os.ErrNotExist
		}
		return nil, os.ErrInvalid
	}

	entries := make([]os.FileInfo, len(resp.Entries))
	for i, e := range resp.Entries {
		modTime := time.Unix(e.ModTimeUnix, 0)
		entries[i] = &fileInfo{
			name:    e.Name,
			size:    e.Size,
			mode:    e.Mode,
			modTime: modTime,
			isDir:   e.IsDir,
		}

		fs.statCacheMu.Lock()
		childPath := filepath.Join(path, e.Name)
		fs.statCache.Add(childPath, statCacheEntry{
			info: entries[i],
		})
		fs.statCacheMu.Unlock()
	}

	// Cache the directory listing itself.
	fs.readDirCacheMu.Lock()
	fs.readDirCache.Add(path, readDirCacheEntry{
		entries: entries,
	})
	fs.readDirCacheMu.Unlock()

	return entries, nil
}

func (fs *ARPCFS) Rename(oldpath, newpath string) error {
	return os.ErrInvalid
}

func (fs *ARPCFS) Remove(filename string) error {
	return os.ErrInvalid
}

func (fs *ARPCFS) MkdirAll(path string, perm os.FileMode) error {
	return os.ErrInvalid
}

func (fs *ARPCFS) Symlink(target, link string) error {
	return os.ErrInvalid
}

func (fs *ARPCFS) Readlink(link string) (string, error) {
	return "", os.ErrInvalid
}

func (fs *ARPCFS) TempFile(dir, prefix string) (billy.File, error) {
	return nil, os.ErrInvalid
}

func (fs *ARPCFS) Join(elem ...string) string {
	return filepath.Join(elem...)
}

func (fs *ARPCFS) Chroot(path string) (billy.Filesystem, error) {
	return NewARPCFS(fs.ctx, fs.session, fs.Hostname, fs.Drive), nil
}

func (fs *ARPCFS) Root() string {
	return "/"
}

func (fs *ARPCFS) Lstat(filename string) (os.FileInfo, error) {
	return fs.Stat(filename)
}

func (fs *ARPCFS) Chmod(name string, mode os.FileMode) error {
	return os.ErrInvalid
}

func (fs *ARPCFS) Lchown(name string, uid, gid int) error {
	return os.ErrInvalid
}

func (fs *ARPCFS) Chown(name string, uid, gid int) error {
	return os.ErrInvalid
}

func (fs *ARPCFS) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return os.ErrInvalid
}
