//go:build linux

package fuse

import (
	"context"
	"errors"
	"io"
	"os"
	"path"
	"sync"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/go-git/go-billy/v5"
)

// CallHook is the callback you can get before every call from FUSE, before it's passed to Billy.
type CallHook func(ctx context.Context, req fuse.Request) error

// New creates a fuse/fs.FS that passes all calls through to the given filesystem.
// callHook is called before every call from FUSE, and can be nil.
func New(underlying billy.Basic, callHook CallHook) fs.FS {
	if callHook == nil {
		callHook = func(ctx context.Context, req fuse.Request) error {
			return nil
		}
	}
	return &root{
		underlying: underlying,
		callHook:   callHook,
	}
}

type root struct {
	underlying billy.Basic
	callHook   CallHook
}

func (r *root) Root() (fs.Node, error) {
	return &node{r, ""}, nil
}

type node struct {
	root *root
	path string
}

var _ fs.Node = &node{}
var _ fs.NodeCreater = &node{}
var _ fs.NodeMkdirer = &node{}
var _ fs.NodeOpener = &node{}
var _ fs.NodeReadlinker = &node{}
var _ fs.NodeRemover = &node{}
var _ fs.NodeRenamer = &node{}
var _ fs.NodeRequestLookuper = &node{}
var _ fs.NodeSymlinker = &node{}

func (n *node) Attr(ctx context.Context, attr *fuse.Attr) error {
	fi, err := n.root.underlying.Stat(n.path)
	if err != nil {
		return convertError(err)
	}
	fileInfoToAttr(fi, attr)
	return nil
}

func fileInfoToAttr(fi os.FileInfo, out *fuse.Attr) {
	out.Mode = fi.Mode()
	out.Size = uint64(fi.Size())
	out.Mtime = fi.ModTime()
}

func (n *node) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	if err := n.root.callHook(ctx, req); err != nil {
		return nil, convertError(err)
	}
	return &node{n.root, path.Join(n.path, req.Name)}, nil
}

func (n *node) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	return nil, syscall.EROFS
}

// Unlink removes a file.
func (n *node) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	return syscall.EROFS
}

// Symlink creates a symbolic link.
func (n *node) Symlink(ctx context.Context, req *fuse.SymlinkRequest) (fs.Node, error) {
	return nil, syscall.EROFS
}

// Readlink reads the target of a symbolic link.
func (n *node) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	if err := n.root.callHook(ctx, req); err != nil {
		return "", convertError(err)
	}
	if sfs, ok := n.root.underlying.(billy.Symlink); ok {
		fn, err := sfs.Readlink(n.path)
		if err != nil {
			return "", convertError(err)
		}
		return fn, nil
	}
	return "", syscall.ENOSYS
}

// Rename renames a file.
func (n *node) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	return syscall.EROFS
}

func (n *node) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	return syscall.EROFS
}

func (n *node) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	return nil, nil, syscall.EROFS
}

func (n *node) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	if err := n.root.callHook(ctx, req); err != nil {
		return nil, convertError(err)
	}
	if req.Dir {
		return &dirHandle{root: n.root, path: n.path}, nil
	}
	fh, err := n.root.underlying.OpenFile(n.path, int(req.Flags), 0777)
	if err != nil {
		return nil, convertError(err)
	}
	return &handle{root: n.root, fh: fh}, nil
}

type handle struct {
	root      *root
	fh        billy.File
	writeLock sync.Mutex
}

var _ fs.HandleReader = &handle{}
var _ fs.HandleReleaser = &handle{}
var _ fs.HandleWriter = &handle{}

func (h *handle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	if err := h.root.callHook(ctx, req); err != nil {
		return convertError(err)
	}
	resp.Data = make([]byte, req.Size)
	n, err := h.fh.ReadAt(resp.Data, req.Offset)
	if err == io.EOF {
		err = nil
	}
	resp.Data = resp.Data[:n]
	return convertError(err)
}

func (h *handle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	return syscall.EROFS
}

func (h *handle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	if err := h.root.callHook(ctx, req); err != nil {
		return convertError(err)
	}
	return convertError(h.fh.Close())
}

type dirHandle struct {
	root *root
	path string
}

var _ fs.HandleReadDirAller = &dirHandle{}

func (h *dirHandle) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	if dfs, ok := h.root.underlying.(billy.Dir); ok {
		entries, err := dfs.ReadDir(h.path)
		if err != nil {
			return nil, convertError(err)
		}
		ret := make([]fuse.Dirent, len(entries))
		for i, e := range entries {
			t := fuse.DT_File
			if e.IsDir() {
				t = fuse.DT_Dir
			} else if e.Mode()&os.ModeSymlink > 0 {
				t = fuse.DT_Link
			}
			ret[i] = fuse.Dirent{
				Name: e.Name(),
				Type: t,
			}
		}
		return ret, nil
	}
	return nil, syscall.ENOSYS
}

func convertError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(fuse.ErrorNumber); ok {
		return err
	}
	if os.IsExist(err) {
		return syscall.EEXIST
	}
	if os.IsNotExist(err) {
		return syscall.ENOENT
	}
	if os.IsPermission(err) {
		return syscall.EPERM
	}
	if errors.Is(err, os.ErrInvalid) || errors.Is(err, os.ErrClosed) || errors.Is(err, billy.ErrCrossedBoundary) {
		return syscall.EINVAL
	}
	if errors.Is(err, billy.ErrNotSupported) {
		return syscall.ENOTSUP
	}
	return syscall.EIO
}
