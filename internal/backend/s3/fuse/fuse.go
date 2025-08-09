//go:build linux

package fuse

import (
	"context"
	"io"
	"os"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	s3fs "github.com/pbs-plus/pbs-plus/internal/backend/s3"
)

var nodePool = &sync.Pool{
	New: func() any {
		return &Node{}
	},
}

var pathBuilderPool = &sync.Pool{
	New: func() interface{} {
		return make([]string, 128)
	},
}

func newRoot(fs *s3fs.S3FS) fs.InodeEmbedder {
	rootNode := nodePool.Get().(*Node)
	rootNode.fs = fs
	rootNode.fullPathCache = ""
	rootNode.name = ""
	rootNode.parent = nil
	return rootNode
}

// Mount mounts the billy filesystem at the specified mountpoint
func Mount(
	mountpoint string,
	fsName string,
	afs *s3fs.S3FS,
) (*fuse.Server, error) {
	root := newRoot(afs)

	timeout := 2 * time.Second

	options := &fs.Options{
		MountOptions: fuse.MountOptions{
			Debug:              false,
			FsName:             fsName,
			Name:               "pbsagent",
			AllowOther:         true,
			DisableXAttrs:      true,
			DisableReadDirPlus: true,
			Options: []string{
				"ro",
				"allow_other",
				"noatime",
			},
		},
		EntryTimeout: &timeout,
		AttrTimeout:  &timeout,
	}

	server, err := fs.Mount(mountpoint, root, options)
	if err != nil {
		return nil, err
	}
	return server, nil
}

// Node represents a file or directory in the filesystem
type Node struct {
	fs.Inode
	fs            *s3fs.S3FS
	name          string
	fullPathCache string
	parent        *Node
}

func (n *Node) getPath() string {
	if n.fullPathCache != "" || n.parent == nil {
		return n.fullPathCache
	}

	// Check if parent's path is cached to build the current path directly
	if parentPath := n.parent.fullPathCache; parentPath != "" {
		const maxStackBuffer = 256
		totalLen := len(parentPath) + 1 + len(n.name)
		if totalLen <= maxStackBuffer {
			var buffer [maxStackBuffer]byte
			b := buffer[:0]
			b = append(b, parentPath...)
			b = append(b, '/')
			b = append(b, n.name...)
			n.fullPathCache = string(b)
		} else {
			b := make([]byte, 0, totalLen)
			b = append(b, parentPath...)
			b = append(b, '/')
			b = append(b, n.name...)
			n.fullPathCache = string(b)
		}
		return n.fullPathCache
	}

	// Collect path components by traversing to the root
	var partsStorage [128]string
	parts := partsStorage[:0]
	for current := n; current != nil; current = current.parent {
		if current.parent != nil {
			parts = append(parts, current.name)
		}
	}

	// Calculate total length needed for the path
	totalLen := 0
	for i := len(parts) - 1; i >= 0; i-- {
		totalLen += len(parts[i])
		if i > 0 {
			totalLen++
		}
	}

	// Use stack-allocated buffer for common path lengths
	const maxPathBuffer = 4096
	var buffer []byte
	if totalLen <= maxPathBuffer {
		var stackBuffer [maxPathBuffer]byte
		buffer = stackBuffer[:0]
	} else {
		buffer = make([]byte, 0, totalLen)
	}

	// Build the path from collected parts
	for i := len(parts) - 1; i >= 0; i-- {
		part := parts[i]
		buffer = append(buffer, part...)
		if i > 0 {
			buffer = append(buffer, '/')
		}
	}

	n.fullPathCache = unsafe.String(
		unsafe.SliceData(buffer[:totalLen]),
		totalLen,
	)
	return n.fullPathCache
}

var _ = (fs.NodeGetattrer)((*Node)(nil))
var _ = (fs.NodeLookuper)((*Node)(nil))
var _ = (fs.NodeReaddirer)((*Node)(nil))
var _ = (fs.NodeOpener)((*Node)(nil))
var _ = (fs.NodeStatfser)((*Node)(nil))
var _ = (fs.NodeAccesser)((*Node)(nil))
var _ = (fs.NodeOpendirer)((*Node)(nil))
var _ = (fs.NodeReleaser)((*Node)(nil))
var _ = (fs.NodeStatxer)((*Node)(nil))

func (n *Node) Access(ctx context.Context, mask uint32) syscall.Errno {
	// For read-only filesystem, deny write access (bit 1)
	if mask&2 != 0 { // 2 = write bit (traditional W_OK)
		return syscall.EROFS
	}

	return 0
}

func (n *Node) Opendir(ctx context.Context) syscall.Errno {
	if !n.IsDir() {
		return syscall.ENOTDIR
	}

	return 0
}

func (n *Node) Release(
	ctx context.Context,
	f fs.FileHandle,
) syscall.Errno {
	if fh, ok := f.(fs.FileReleaser); ok {
		return fh.Release(ctx)
	}

	nodePool.Put(n)

	return 0
}

func (n *Node) Statx(
	ctx context.Context,
	f fs.FileHandle,
	flags uint32,
	mask uint32,
	out *fuse.StatxOut,
) syscall.Errno {
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
func (n *Node) Getattr(
	ctx context.Context,
	fh fs.FileHandle,
	out *fuse.AttrOut,
) syscall.Errno {
	fi, err := n.fs.Attr(n.getPath())
	if err != nil {
		return s3ErrorToErrno(err)
	}

	mode := fi.Mode
	if fi.IsDir {
		mode |= syscall.S_IFDIR
	} else if os.FileMode(fi.Mode)&os.ModeSymlink != 0 {
		mode |= syscall.S_IFLNK
	} else {
		mode |= syscall.S_IFREG
	}

	out.Mode = mode
	out.Size = uint64(fi.Size)
	out.Blocks = fi.Blocks

	atime := time.Unix(fi.LastAccessTime, 0)
	mtime := time.Unix(fi.LastWriteTime, 0)
	ctime := time.Unix(fi.CreationTime, 0)
	out.SetTimes(&atime, &mtime, &ctime)

	return 0
}

// Lookup implements NodeLookuper
func (n *Node) Lookup(
	ctx context.Context,
	name string,
	out *fuse.EntryOut,
) (*fs.Inode, syscall.Errno) {
	childNode := nodePool.Get().(*Node)
	childNode.fs = n.fs
	childNode.parent = n
	childNode.name = name
	childNode.fullPathCache = ""

	path := childNode.getPath()
	fi, err := childNode.fs.Attr(path)
	if err != nil {
		nodePool.Put(childNode) // Return to pool on error
		return nil, s3ErrorToErrno(err)
	}

	mode := fi.Mode
	if fi.IsDir {
		mode |= syscall.S_IFDIR
	} else if os.FileMode(fi.Mode)&os.ModeSymlink != 0 {
		mode |= syscall.S_IFLNK
	} else {
		mode |= syscall.S_IFREG
	}

	stable := fs.StableAttr{
		Mode: mode,
	}

	child := n.NewInode(ctx, childNode, stable)

	out.Mode = mode
	out.Size = uint64(fi.Size)
	out.Blocks = fi.Blocks

	atime := time.Unix(fi.LastAccessTime, 0)
	mtime := time.Unix(fi.LastWriteTime, 0)
	ctime := time.Unix(fi.CreationTime, 0)
	out.SetTimes(&atime, &mtime, &ctime)

	return child, 0
}

// Readdir implements NodeReaddirer
func (n *Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, err := n.fs.ReadDir(n.getPath())
	if err != nil {
		return nil, s3ErrorToErrno(err)
	}

	return entries, 0
}

// Open implements NodeOpener
func (n *Node) Open(
	ctx context.Context,
	flags uint32,
) (fs.FileHandle, uint32, syscall.Errno) {
	file, err := n.fs.OpenFile(n.getPath(), int(flags), 0)
	if err != nil {
		return nil, 0, s3ErrorToErrno(err)
	}

	return &FileHandle{
		fs:   n.fs,
		file: file,
	}, 0, 0
}

func (n *Node) Statfs(
	ctx context.Context,
	out *fuse.StatfsOut,
) syscall.Errno {
	stat, err := n.fs.StatFS()
	if err != nil {
		return s3ErrorToErrno(err)
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

// FileHandle handles file operations
type FileHandle struct {
	fs   *s3fs.S3FS
	file *s3fs.S3File
}

var _ = (fs.FileReader)((*FileHandle)(nil))
var _ = (fs.FileReleaser)((*FileHandle)(nil))
var _ = (fs.FileLseeker)((*FileHandle)(nil))

// Read implements FileReader
func (fh *FileHandle) Read(
	ctx context.Context,
	dest []byte,
	offset int64,
) (fuse.ReadResult, syscall.Errno) {
	n, err := fh.file.ReadAt(dest, offset)
	if err != nil && err != io.EOF {
		return nil, s3ErrorToErrno(err)
	}

	return fuse.ReadResultData(dest[:n]), 0
}

func (fh *FileHandle) Lseek(
	ctx context.Context,
	off uint64,
	whence uint32,
) (uint64, syscall.Errno) {
	n, err := fh.file.Lseek(int64(off), int(whence))
	if err != nil && err != io.EOF {
		return 0, s3ErrorToErrno(err)
	}

	return n, 0
}

func (fh *FileHandle) Release(ctx context.Context) syscall.Errno {
	err := fh.file.Close()
	return s3ErrorToErrno(err)
}
