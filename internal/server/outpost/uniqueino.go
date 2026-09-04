package outpost

import (
	"os"
	"sync/atomic"
	"syscall"

	"github.com/go-git/go-billy/v5"
)

// uniqueInoFS remaps reported inodes per share so two shares of one outpost never collide on NFS fileid.
type uniqueInoFS struct {
	billy.Filesystem
	billy.Change
	prefix uint64
}

const uniqueInoBits = 48

func newUniqueInoFS(fs billy.Filesystem, seq uint64) billy.Filesystem {
	return &uniqueInoFS{Filesystem: fs, Change: asChange(fs), prefix: (seq + 1) << uniqueInoBits}
}

func asChange(fs billy.Filesystem) billy.Change {
	if c, ok := fs.(billy.Change); ok {
		return c
	}
	return nil
}

type remappedInfo struct {
	os.FileInfo
	ino uint64
}

func statOf(fi os.FileInfo) *syscall.Stat_t {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return st
	}
	st := &syscall.Stat_t{Size: fi.Size()}
	if fi.Mode()&os.ModeDir != 0 {
		st.Mode = syscall.S_IFDIR | uint32(fi.Mode().Perm())
	} else if fi.Mode()&os.ModeSymlink != 0 {
		st.Mode = syscall.S_IFLNK | uint32(fi.Mode().Perm())
	} else {
		st.Mode = syscall.S_IFREG | uint32(fi.Mode().Perm())
	}
	return st
}

func (r *remappedInfo) Sys() any {
	st := statOf(r.FileInfo)
	st.Ino = r.ino
	return st
}

func (u *uniqueInoFS) remap(fi os.FileInfo) os.FileInfo {
	if fi == nil {
		return nil
	}
	return &remappedInfo{FileInfo: fi, ino: u.prefix | (statOf(fi).Ino & ((1 << uniqueInoBits) - 1))}
}

func (u *uniqueInoFS) Lstat(p string) (os.FileInfo, error) {
	fi, err := u.Filesystem.Lstat(p)
	if err != nil {
		return nil, err
	}
	return u.remap(fi), nil
}

func (u *uniqueInoFS) Stat(p string) (os.FileInfo, error) {
	fi, err := u.Filesystem.Stat(p)
	if err != nil {
		return nil, err
	}
	return u.remap(fi), nil
}

func (u *uniqueInoFS) ReadDir(p string) ([]os.FileInfo, error) {
	entries, err := u.Filesystem.ReadDir(p)
	if err != nil {
		return nil, err
	}
	out := make([]os.FileInfo, len(entries))
	for i, e := range entries {
		out[i] = u.remap(e)
	}
	return out, nil
}

var uniqueInoSeq atomic.Uint64

func nextUniqueInoSeq() uint64 {
	return uniqueInoSeq.Add(1)
}
