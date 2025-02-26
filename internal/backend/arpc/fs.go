//go:build linux

package arpcfs

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/backend/arpc/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

var _ billy.Filesystem = (*ARPCFS)(nil)

// NewARPCFS creates a new filesystem backed by aRPC calls
func NewARPCFS(ctx context.Context, session *arpc.Session, hostname string, drive string) *ARPCFS {
	return &ARPCFS{
		ctx:      ctx,
		session:  session,
		Drive:    drive,
		Hostname: hostname,
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

func (fs *ARPCFS) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	if flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0 {
		return nil, os.ErrInvalid
	}

	var resp struct {
		HandleID string `json:"handleID"`
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

func (fs *ARPCFS) Stat(filename string) (os.FileInfo, error) {
	var fi FileInfoResponse
	if fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
		return nil, os.ErrInvalid
	}

	ctx, cancel := TimeoutCtx()
	defer cancel()

	err := fs.session.CallJSON(ctx, fs.Drive+"/Stat", struct {
		Path string `json:"path"`
	}{
		Path: filename,
	}, &fi)
	if err != nil {
		syslog.L.Errorf("Stat RPC failed (%s): %v", filename, err)
		if strings.Contains(err.Error(), "file not found") {
			return nil, os.ErrNotExist
		}
	}

	encJson, _ := json.Marshal(fi)
	syslog.L.Infof("Entry - %s: %v", filename, string(encJson))

	modTime := time.Unix(fi.ModTimeUnix, 0)
	return &fileInfo{
		name:    filepath.Base(filename),
		size:    fi.Size,
		mode:    fi.Mode,
		modTime: modTime,
		isDir:   fi.IsDir,
	}, nil
}

func (fs *ARPCFS) StatFS() (types.StatFS, error) {
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

	return types.StatFS{
		Bsize:   uint64(4096), // Standard block size
		Blocks:  uint64(fsStat.TotalSize / 4096),
		Bfree:   uint64(fsStat.FreeSize / 4096),
		Bavail:  uint64(fsStat.AvailableSize / 4096),
		Files:   uint64(fsStat.TotalFiles),
		Ffree:   uint64(fsStat.FreeFiles),
		NameLen: 255, // Windows typically supports long filenames
	}, nil
}

func (fs *ARPCFS) ReadDir(path string) ([]os.FileInfo, error) {
	var resp ReadDirResponse
	if fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
		return nil, os.ErrInvalid
	}

	ctx, cancel := TimeoutCtx()
	defer cancel()

	err := fs.session.CallJSON(ctx, fs.Drive+"/ReadDir", struct {
		Path string `json:"path"`
	}{
		Path: path,
	}, &resp)
	if err != nil {
		syslog.L.Errorf("ReadDir RPC failed: %v", err)
		if strings.Contains(err.Error(), "not found") {
			return nil, os.ErrNotExist
		}
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
		encJson, _ := json.Marshal(e)
		syslog.L.Infof("Entry - %s: %v", path, string(encJson))
	}
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
