package nfs

import (
	"os"

	"github.com/go-git/go-billy/v5"
)

var errReadOnly = billy.ErrReadOnly

type ReadOnlyFS struct {
	fs billy.Filesystem
}

func NewROFS(fs billy.Filesystem) billy.Filesystem {
	return &ReadOnlyFS{fs: fs}
}

func (ro *ReadOnlyFS) Create(filename string) (billy.File, error) {
	return nil, errReadOnly
}

func (ro *ReadOnlyFS) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	// Disallow any write flags
	if flag != os.O_RDONLY {
		return nil, errReadOnly
	}

	if skipPath(filename) {
		return nil, billy.ErrNotSupported
	}

	return ro.fs.OpenFile(filename, flag, perm)
}

func (ro *ReadOnlyFS) Rename(oldpath, newpath string) error {
	return errReadOnly
}

func (ro *ReadOnlyFS) Remove(filename string) error {
	return errReadOnly
}

// Delegate read-only operations to the underlying fs

func (ro *ReadOnlyFS) Open(filename string) (billy.File, error) {
	if skipPath(filename) {
		return nil, billy.ErrNotSupported
	}

	return ro.fs.Open(filename)
}

func (ro *ReadOnlyFS) Stat(filename string) (os.FileInfo, error) {
	if skipPath(filename) {
		return nil, billy.ErrNotSupported
	}

	return ro.fs.Stat(filename)
}

func (ro *ReadOnlyFS) Join(elem ...string) string {
	return ro.fs.Join(elem...)
}

func (ro *ReadOnlyFS) TempFile(dir, prefix string) (billy.File, error) {
	return nil, errReadOnly
}

func (ro *ReadOnlyFS) ReadDir(path string) ([]os.FileInfo, error) {
	if skipPath(path) {
		return nil, billy.ErrNotSupported
	}

	return ro.fs.ReadDir(path)
}

func (ro *ReadOnlyFS) MkdirAll(filename string, perm os.FileMode) error {
	return errReadOnly
}

func (ro *ReadOnlyFS) Lstat(filename string) (os.FileInfo, error) {
	return ro.fs.Lstat(filename)
}

func (ro *ReadOnlyFS) Symlink(target, link string) error {
	return errReadOnly
}

func (ro *ReadOnlyFS) Readlink(link string) (string, error) {
	if skipPath(link) {
		return "", billy.ErrNotSupported
	}

	return ro.fs.Readlink(link)
}

func (ro *ReadOnlyFS) Chroot(path string) (billy.Filesystem, error) {
	if skipPath(path) {
		return nil, billy.ErrNotSupported
	}

	fs, err := ro.fs.Chroot(path)
	if err != nil {
		return nil, err
	}
	return NewROFS(fs), nil
}

func (ro *ReadOnlyFS) Root() string {
	return ro.fs.Root()
}

func (ro *ReadOnlyFS) Capabilities() billy.Capability {
	return billy.DefaultCapabilities &^ billy.WriteCapability
}
