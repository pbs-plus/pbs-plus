//go:build linux

package fuse

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	arpcfs "github.com/sonroyaalmerol/pbs-plus/internal/backend/arpc"
)

func newRoot(fs *arpcfs.ARPCFS) fs.InodeEmbedder {
	return &BillyNode{
		fs: fs,
	}
}

// Mount mounts the billy filesystem at the specified mountpoint
func Mount(mountpoint string, fsName string, afs *arpcfs.ARPCFS) (*fuse.Server, error) {
	root := newRoot(afs)

	timeout := time.Second

	options := &fs.Options{
		MountOptions: fuse.MountOptions{
			Debug:              true,
			FsName:             fsName,
			Name:               "pbsagent",
			AllowOther:         true,
			DisableXAttrs:      true,
			DisableReadDirPlus: true,
			Options: []string{
				"ro",
				"allow_other",
			},
		},
		// Use sensible cache timeouts
		EntryTimeout: &timeout,
		AttrTimeout:  &timeout,
	}

	server, err := fs.Mount(mountpoint, root, options)
	if err != nil {
		return nil, err
	}
	return server, nil
}

// BillyNode represents a file or directory in the filesystem
type BillyNode struct {
	fs.Inode
	fs   *arpcfs.ARPCFS
	path string
}

var _ = (fs.NodeGetattrer)((*BillyNode)(nil))
var _ = (fs.NodeLookuper)((*BillyNode)(nil))
var _ = (fs.NodeReaddirer)((*BillyNode)(nil))
var _ = (fs.NodeOpener)((*BillyNode)(nil))
var _ = (fs.NodeReadlinker)((*BillyNode)(nil))
var _ = (fs.NodeStatfser)((*BillyNode)(nil))
var _ = (fs.NodeAccesser)((*BillyNode)(nil))
var _ = (fs.NodeOpendirer)((*BillyNode)(nil))
var _ = (fs.NodeReleaser)((*BillyNode)(nil))
var _ = (fs.NodeStatxer)((*BillyNode)(nil))

func (n *BillyNode) Access(ctx context.Context, mask uint32) syscall.Errno {
	// For read-only filesystem, deny write access (bit 1)
	if mask&2 != 0 { // 2 = write bit (traditional W_OK)
		return syscall.EROFS
	}

	// Check if the file exists (that's sufficient for read-only fs)
	_, err := n.fs.Stat(n.path)
	if err != nil {
		return fs.ToErrno(err)
	}

	return 0
}

func (n *BillyNode) Opendir(ctx context.Context) syscall.Errno {
	info, err := n.fs.Stat(n.path)
	if err != nil {
		return fs.ToErrno(err)
	}

	if !info.IsDir() {
		return syscall.ENOTDIR
	}

	return 0
}

func (n *BillyNode) Release(ctx context.Context, f fs.FileHandle) syscall.Errno {
	if fh, ok := f.(fs.FileReleaser); ok {
		return fh.Release(ctx)
	}

	return 0
}

func (n *BillyNode) Statx(ctx context.Context, f fs.FileHandle, flags uint32, mask uint32, out *fuse.StatxOut) syscall.Errno {
	// Get file stats the regular way, then populate StatxOut
	var attrOut fuse.AttrOut
	errno := n.Getattr(ctx, f, &attrOut)
	if errno != 0 {
		return errno
	}

	// Use actual STATX mask values
	// These values come from Linux's statx flags in <linux/stat.h>
	const (
		STATX_TYPE  = 0x00000001 // Want stx_mode & S_IFMT
		STATX_MODE  = 0x00000002 // Want stx_mode & ~S_IFMT
		STATX_NLINK = 0x00000004 // Want stx_nlink
		STATX_SIZE  = 0x00000200 // Want stx_size
		STATX_MTIME = 0x00000020 // Want stx_mtime
	)

	// Set basic attributes
	out.Mask = STATX_TYPE | STATX_MODE | STATX_NLINK | STATX_SIZE
	out.Mode = uint16(attrOut.Mode)
	out.Size = attrOut.Size
	out.Nlink = attrOut.Nlink

	// Add timestamps if requested
	if mask&STATX_MTIME != 0 {
		out.Mask |= STATX_MTIME
		out.Mtime.Sec = attrOut.Mtime
		out.Mtime.Nsec = attrOut.Mtimensec
	}

	return 0
}

// Getattr implements NodeGetattrer
func (n *BillyNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	fi, err := n.fs.Stat(n.path)
	if err != nil {
		return fs.ToErrno(err)
	}

	mode := uint32(fi.Mode().Perm())
	if fi.IsDir() {
		mode |= syscall.S_IFDIR
	} else if fi.Mode()&os.ModeSymlink != 0 {
		mode |= syscall.S_IFLNK
	} else {
		mode |= syscall.S_IFREG
	}

	out.Mode = mode
	out.Size = uint64(fi.Size())
	mtime := fi.ModTime()
	out.SetTimes(nil, &mtime, nil)

	return 0
}

// Lookup implements NodeLookuper
func (n *BillyNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childPath := filepath.Join(n.path, name)
	fi, err := n.fs.Stat(childPath)
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	childNode := &BillyNode{
		fs:   n.fs,
		path: childPath,
	}

	mode := uint32(fi.Mode().Perm())
	if fi.IsDir() {
		mode |= syscall.S_IFDIR
	} else if fi.Mode()&os.ModeSymlink != 0 {
		mode |= syscall.S_IFLNK
	} else {
		mode |= syscall.S_IFREG
	}

	stable := fs.StableAttr{
		Mode: mode,
	}

	child := n.NewInode(ctx, childNode, stable)

	out.Mode = mode
	out.Size = uint64(fi.Size())
	mtime := fi.ModTime()
	out.SetTimes(nil, &mtime, nil)

	return child, 0
}

// Readdir implements NodeReaddirer
func (n *BillyNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, err := n.fs.ReadDir(n.path)
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	result := make([]fuse.DirEntry, 0, len(entries))
	for _, e := range entries {
		entryType := uint32(0) // DT_Unknown
		if e.IsDir() {
			entryType = syscall.DT_DIR
		} else if e.Mode()&os.ModeSymlink != 0 {
			entryType = syscall.DT_LNK
		} else {
			entryType = syscall.DT_REG
		}

		result = append(result, fuse.DirEntry{
			Name: e.Name(),
			Mode: entryType << 12, // Convert to type bits
		})
	}

	return fs.NewListDirStream(result), 0
}

// Open implements NodeOpener
func (n *BillyNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	file, err := n.fs.OpenFile(n.path, int(flags), 0)
	if err != nil {
		return nil, 0, fs.ToErrno(err)
	}

	return &BillyFileHandle{
		fs:   n.fs,
		file: file,
	}, 0, 0
}

// Readlink implements NodeReadlinker
func (n *BillyNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	target, err := n.fs.Readlink(n.path)
	if err != nil {
		return nil, fs.ToErrno(err)
	}
	return []byte(target), 0
}

func (n *BillyNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	stat, err := n.fs.StatFS()
	if err != nil {
		return fs.ToErrno(err)
	}

	out.Blocks = stat.Blocks
	out.Bfree = stat.Bfree
	out.Bavail = stat.Bavail
	out.Files = stat.Files
	out.Ffree = stat.Ffree
	out.Bsize = uint32(stat.Bsize)
	out.NameLen = uint32(stat.NameLen)
	out.Frsize = uint32(stat.Bsize)

	return 0
}

// BillyFileHandle handles file operations
type BillyFileHandle struct {
	fs   *arpcfs.ARPCFS
	file billy.File
}

var _ = (fs.FileReader)((*BillyFileHandle)(nil))
var _ = (fs.FileReleaser)((*BillyFileHandle)(nil))

// Read implements FileReader
func (fh *BillyFileHandle) Read(ctx context.Context, dest []byte, offset int64) (fuse.ReadResult, syscall.Errno) {
	n, err := fh.file.ReadAt(dest, offset)
	if err != nil && err != io.EOF {
		return nil, fs.ToErrno(err)
	}

	return fuse.ReadResultData(dest[:n]), 0
}

// Release implements FileReleaser
func (fh *BillyFileHandle) Release(ctx context.Context) syscall.Errno {
	err := fh.file.Close()
	return fs.ToErrno(err)
}
