//go:build linux

package arpcfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"bazil.org/fuse/fs"
	"github.com/go-git/go-billy/v5"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/backend/arpc/fuse"
)

var _ billy.Filesystem = (*ARPCFS)(nil)

// NewARPCFS creates a new filesystem backed by aRPC calls
func NewARPCFS(ctx context.Context, session *arpc.Session, drive string) *ARPCFS {
	return &ARPCFS{
		ctx:     ctx,
		session: session,
		drive:   drive,
	}
}

func (fs *ARPCFS) GetFUSE() fs.FS {
	return fuse.New(fs, nil)
}

var _ billy.File = (*ARPCFile)(nil)

func (fs *ARPCFS) Create(filename string) (billy.File, error) {
	return nil, fmt.Errorf("read-only filesystem")
}

func (fs *ARPCFS) Open(filename string) (billy.File, error) {
	return fs.OpenFile(filename, os.O_RDONLY, 0)
}

func (fs *ARPCFS) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	if flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0 {
		return nil, fmt.Errorf("read-only filesystem")
	}

	var resp struct {
		HandleID string `json:"handleID"`
	}

	if fs.session == nil {
		return nil, fmt.Errorf("RPC failed: aRPC session is nil")
	}

	ctx, cancel := TimeoutCtx()
	defer cancel()

	err := fs.session.CallJSON(ctx, fs.drive+"/OpenFile", OpenRequest{
		Path: filename,
		Flag: flag,
		Perm: int(perm),
	}, &resp)
	if err != nil {
		return nil, fmt.Errorf("OpenFile RPC failed: %w", err)
	}

	return &ARPCFile{
		fs:       fs,
		name:     filename,
		handleID: resp.HandleID,
		drive:    fs.drive,
	}, nil
}

func (fs *ARPCFS) Stat(filename string) (os.FileInfo, error) {
	var fi FileInfoResponse
	if fs.session == nil {
		return nil, fmt.Errorf("RPC failed: aRPC session is nil")
	}

	ctx, cancel := TimeoutCtx()
	defer cancel()

	err := fs.session.CallJSON(ctx, fs.drive+"/Stat", struct {
		Path string `json:"path"`
	}{
		Path: filename,
	}, &fi)
	if err != nil {
		return nil, fmt.Errorf("Stat RPC failed: %w", err)
	}

	return &fileInfo{
		name:    filepath.Base(filename),
		size:    fi.Size,
		mode:    fi.Mode,
		modTime: fi.ModTime,
		isDir:   fi.IsDir,
	}, nil
}

func (fs *ARPCFS) ReadDir(path string) ([]os.FileInfo, error) {
	var resp ReadDirResponse
	if fs.session == nil {
		return nil, fmt.Errorf("RPC failed: aRPC session is nil")
	}

	ctx, cancel := TimeoutCtx()
	defer cancel()

	err := fs.session.CallJSON(ctx, fs.drive+"/ReadDir", struct {
		Path string `json:"path"`
	}{
		Path: path,
	}, &resp)
	if err != nil {
		return nil, fmt.Errorf("ReadDir RPC failed: %w", err)
	}

	entries := make([]os.FileInfo, len(resp.Entries))
	for i, e := range resp.Entries {
		entries[i] = &fileInfo{
			name:    e.Name,
			size:    e.Size,
			mode:    e.Mode,
			modTime: e.ModTime,
			isDir:   e.IsDir,
		}
	}
	return entries, nil
}

func (fs *ARPCFS) Rename(oldpath, newpath string) error {
	return fmt.Errorf("read-only filesystem")
}

func (fs *ARPCFS) Remove(filename string) error {
	return fmt.Errorf("read-only filesystem")
}

func (fs *ARPCFS) MkdirAll(path string, perm os.FileMode) error {
	return fmt.Errorf("read-only filesystem")
}

func (fs *ARPCFS) Symlink(target, link string) error {
	return fmt.Errorf("read-only filesystem")
}

func (fs *ARPCFS) Readlink(link string) (string, error) {
	return "", fmt.Errorf("read link unsupported")
}

func (fs *ARPCFS) TempFile(dir, prefix string) (billy.File, error) {
	return nil, fmt.Errorf("read-only filesystem")
}

func (fs *ARPCFS) Join(elem ...string) string {
	return filepath.Join(elem...)
}

func (fs *ARPCFS) Chroot(path string) (billy.Filesystem, error) {
	return NewARPCFS(fs.ctx, fs.session, path), nil
}

func (fs *ARPCFS) Root() string {
	return "/"
}

func (fs *ARPCFS) Lstat(filename string) (os.FileInfo, error) {
	return fs.Stat(filename)
}

func (fs *ARPCFS) Chmod(name string, mode os.FileMode) error {
	return fmt.Errorf("read-only filesystem")
}

func (fs *ARPCFS) Lchown(name string, uid, gid int) error {
	return fmt.Errorf("read-only filesystem")
}

func (fs *ARPCFS) Chown(name string, uid, gid int) error {
	return fmt.Errorf("read-only filesystem")
}

func (fs *ARPCFS) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return fmt.Errorf("read-only filesystem")
}
