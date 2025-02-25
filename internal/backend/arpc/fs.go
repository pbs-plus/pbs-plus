//go:build linux

package arpcfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/backend/arpc/fuse"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

var _ billy.Filesystem = (*ARPCFS)(nil)

// NewARPCFS creates a new filesystem backed by aRPC calls
func NewARPCFS(ctx context.Context, session *arpc.Session, hostname string, drive string) *ARPCFS {
	return &ARPCFS{
		ctx:      ctx,
		session:  session,
		drive:    drive,
		hostname: hostname,
	}
}

func (f *ARPCFS) Mount(mountpoint string) error {
	timeout := 5 * time.Second

	options := &fs.Options{
		MountOptions: gofuse.MountOptions{
			Debug:      false,
			FsName:     utils.Slugify(f.hostname) + "/" + f.drive,
			Name:       "pbsagent",
			AllowOther: true,
		},
		// Use sensible cache timeouts
		EntryTimeout:    &timeout,
		AttrTimeout:     &timeout,
		NegativeTimeout: &timeout,
	}

	root := fuse.New(f, nil)

	server, err := fs.Mount(mountpoint, root, options)
	if err != nil {
		return err
	}

	f.mount = server

	f.mount.WaitMount()
	return nil
}

func (f *ARPCFS) Unmount() {
	if f.mount != nil {
		_ = f.mount.Unmount()
	}
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
		syslog.L.Error("RPC failed: aRPC session is nil")
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
		syslog.L.Errorf("OpenFile RPC failed: %v", err)
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
		syslog.L.Error("RPC failed: aRPC session is nil")
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
		syslog.L.Errorf("Stat RPC failed: %v", err)
		return nil, fmt.Errorf("Stat RPC failed: %w", err)
	}

	modTime := time.Unix(fi.ModTimeUnix, 0)
	return &fileInfo{
		name:    filepath.Base(filename),
		size:    fi.Size,
		mode:    fi.Mode,
		modTime: modTime,
		isDir:   fi.IsDir,
	}, nil
}

func (fs *ARPCFS) ReadDir(path string) ([]os.FileInfo, error) {
	var resp ReadDirResponse
	if fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
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
		syslog.L.Errorf("ReadDir RPC failed: %v", err)
		return nil, fmt.Errorf("ReadDir RPC failed: %w", err)
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
	return NewARPCFS(fs.ctx, fs.session, fs.hostname, fs.drive), nil
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
