//go:build linux

package arpcfuse

import (
	"context"
	"io"
	"os"
	"strconv"
	"syscall"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	arpcfs "github.com/pbs-plus/pbs-plus/internal/backend/vfs/arpc"
)

func newRoot(fs *arpcfs.ARPCFS) fs.InodeEmbedder {
	rootNode := &Node{}
	rootNode.fs = fs
	rootNode.name = ""
	rootNode.parent = nil
	return rootNode
}

// Mount mounts the billy filesystem at the specified mountpoint
func Mount(mountpoint string, fsName string, afs *arpcfs.ARPCFS) (*fuse.Server, error) {
	root := newRoot(afs)

	timeout := 2 * time.Second

	options := &fs.Options{
		MountOptions: fuse.MountOptions{
			Debug:              false,
			FsName:             fsName,
			Name:               "pbsagent",
			AllowOther:         true,
			DisableXAttrs:      false,
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
	fs     *arpcfs.ARPCFS
	name   string
	parent *Node
}

func (n *Node) getPath() string {
	path := n.Inode.Path(nil)
	if path == "" {
		return "/"
	}
	return "/" + path
}

var _ = (fs.NodeGetattrer)((*Node)(nil))
var _ = (fs.NodeListxattrer)((*Node)(nil))
var _ = (fs.NodeGetxattrer)((*Node)(nil))
var _ = (fs.NodeLookuper)((*Node)(nil))
var _ = (fs.NodeReaddirer)((*Node)(nil))
var _ = (fs.NodeOpener)((*Node)(nil))
var _ = (fs.NodeStatfser)((*Node)(nil))
var _ = (fs.NodeAccesser)((*Node)(nil))
var _ = (fs.NodeOpendirer)((*Node)(nil))
var _ = (fs.NodeReleaser)((*Node)(nil))
var _ = (fs.NodeStatxer)((*Node)(nil))

func (n *Node) Access(ctx context.Context, mask uint32) syscall.Errno {
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

func (n *Node) Release(ctx context.Context, f fs.FileHandle) syscall.Errno {
	if fh, ok := f.(fs.FileReleaser); ok {
		return fh.Release(ctx)
	}

	return 0
}

func (n *Node) Statx(ctx context.Context, f fs.FileHandle, flags uint32, mask uint32, out *fuse.StatxOut) syscall.Errno {
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
func (n *Node) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	fi, err := n.fs.Attr(ctx, n.getPath(), false)
	if err != nil {
		return syscall.ENOENT
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

func (n *Node) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	fi, err := n.fs.Xattr(ctx, n.getPath(), attr)
	if err != nil {
		return 0, syscall.ENOTSUP
	}

	var data []byte
	switch attr {
	case "user.creationtime":
		data = []byte(strconv.FormatInt(fi.CreationTime, 10))
	case "user.owner":
		data = []byte(fi.Owner)
	case "user.fileattributes":
		data, _ = cbor.Marshal(fi.FileAttributes)
	case "user.acls":
		if fi.PosixACLs != nil {
			data, _ = cbor.Marshal(fi.PosixACLs)
		} else if fi.WinACLs != nil {
			data, _ = cbor.Marshal(fi.WinACLs)
		} else {
			return 0, syscall.ENODATA
		}
	default:
		return 0, syscall.ENODATA
	}

	if data == nil {
		return 0, syscall.ENODATA
	}

	if dest == nil {
		return uint32(len(data)), 0
	}

	if len(dest) < len(data) {
		return uint32(len(data)), syscall.ERANGE
	}

	nCopied := copy(dest, data)
	return uint32(nCopied), 0
}

func (n *Node) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	fi, err := n.fs.ListXattr(ctx, n.getPath())
	if err != nil {
		return 0, syscall.ENOTSUP
	}

	attrs := []string{
		"user.creationtime",
		"user.lastaccesstime",
		"user.lastwritetime",
		"user.owner",
		"user.group",
		"user.fileattributes",
	}

	if fi.PosixACLs != nil || fi.WinACLs != nil {
		attrs = append(attrs, "user.acls")
	}

	var list []byte
	for _, attr := range attrs {
		list = append(list, attr...)
		list = append(list, 0)
	}

	length := uint32(len(list))

	if dest == nil {
		return length, 0
	}

	if len(dest) < len(list) {
		return length, syscall.E2BIG
	}

	copy(dest, list)
	return length, 0
}

// Lookup implements NodeLookuper
func (n *Node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	parentPath := n.getPath()
	var fullPath string
	if parentPath == "/" {
		fullPath = "/" + name
	} else {
		fullPath = parentPath + "/" + name
	}

	fi, err := n.fs.Attr(ctx, fullPath, true)
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	childNode := &Node{
		fs:     n.fs,
		name:   name,
		parent: n,
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
	mtime := time.Unix(0, fi.ModTime)
	out.SetTimes(nil, &mtime, nil)

	return child, 0
}

// Readdir implements NodeReaddirer
func (n *Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, err := n.fs.ReadDir(ctx, n.getPath())
	if err != nil {
		return nil, syscall.EBADF
	}

	return &entries, 0
}

// Open implements NodeOpener
func (n *Node) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	file, err := n.fs.OpenFile(ctx, n.getPath(), int(flags), 0)
	if err != nil {
		return nil, 0, syscall.EACCES
	}

	return &FileHandle{
		fs:   n.fs,
		file: &file,
	}, 0, 0
}

func (n *Node) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	stat, err := n.fs.StatFS(ctx)
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

// FileHandle handles file operations
type FileHandle struct {
	fs   *arpcfs.ARPCFS
	file *arpcfs.ARPCFile
}

var _ = (fs.FileReader)((*FileHandle)(nil))
var _ = (fs.FileReleaser)((*FileHandle)(nil))
var _ = (fs.FileLseeker)((*FileHandle)(nil))

// Read implements FileReader
func (fh *FileHandle) Read(ctx context.Context, dest []byte, offset int64) (fuse.ReadResult, syscall.Errno) {
	n, err := fh.file.ReadAt(ctx, dest, offset)
	if err != nil && err != io.EOF {
		return nil, syscall.EBADF
	}

	return fuse.ReadResultData(dest[:n]), 0
}

func (fh *FileHandle) Lseek(ctx context.Context, off uint64, whence uint32) (uint64, syscall.Errno) {
	n, err := fh.file.Lseek(ctx, int64(off), int(whence))
	if err != nil && err != io.EOF {
		return 0, syscall.EBADF
	}

	return n, 0
}

func (fh *FileHandle) Release(ctx context.Context) syscall.Errno {
	err := fh.file.Close(ctx)
	return fs.ToErrno(err)
}
