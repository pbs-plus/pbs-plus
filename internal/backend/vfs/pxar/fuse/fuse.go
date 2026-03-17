//go:build linux

package fuse

import (
	"context"
	"io"
	"path"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pbs-plus/pbs-plus/internal/pxar/native"
)

func newRoot(accessor *native.Accessor) fs.InodeEmbedder {
	rootNode := &Node{accessor: accessor}
	return rootNode
}

func Mount(mountpoint string, fsName string, accessor *native.Accessor) (*fuse.Server, error) {
	root := newRoot(accessor)

	timeout := 2 * time.Second

	options := &fs.Options{
		MountOptions: fuse.MountOptions{
			Debug:         false,
			FsName:        fsName,
			Name:          "pbspxar",
			AllowOther:    true,
			DisableXAttrs: false,
			Options: []string{
				"ro",
				"allow_other",
				"noatime",
				"default_permissions",
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

type Node struct {
	fs.Inode
	accessor *native.Accessor
	entry    *native.FileEntry
	name     string
}

func (n *Node) getPath() string {
	p := n.Inode.Path(nil)
	if p == "" {
		return "/"
	}
	return "/" + p
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
var _ = (fs.NodeReadlinker)((*Node)(nil))
var _ = (fs.NodeGetxattrer)((*Node)(nil))
var _ = (fs.NodeListxattrer)((*Node)(nil))

func (n *Node) Access(ctx context.Context, mask uint32) syscall.Errno {
	if mask&2 != 0 {
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
	var attrOut fuse.AttrOut
	errno := n.Getattr(ctx, f, &attrOut)
	if errno != 0 {
		return errno
	}

	const (
		STATX_TYPE  = 0x00000001
		STATX_MODE  = 0x00000002
		STATX_NLINK = 0x00000004
		STATX_SIZE  = 0x00000200
		STATX_MTIME = 0x00000020
	)

	out.Mask = STATX_TYPE | STATX_MODE | STATX_NLINK | STATX_SIZE
	out.Mode = uint16(attrOut.Mode)
	out.Size = attrOut.Size
	out.Nlink = attrOut.Nlink

	if mask&STATX_MTIME != 0 {
		out.Mask |= STATX_MTIME
		out.Mtime.Sec = attrOut.Mtime
		out.Mtime.Nsec = attrOut.Mtimensec
	}

	return 0
}

func (n *Node) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	entry, err := n.getEntry(ctx)
	if err != nil {
		return syscall.ENOENT
	}

	var mode uint32
	var size uint64

	switch entry.Kind() {
	case native.EntryKindDirectory:
		mode = syscall.S_IFDIR | 0755
		size = 4096
	case native.EntryKindFile:
		mode = syscall.S_IFREG | 0644
		size = entry.FileSize()
	case native.EntryKindSymlink:
		mode = syscall.S_IFLNK | 0777
		size = entry.FileSize()
	case native.EntryKindDevice:
		mode = syscall.S_IFCHR | 0600
		size = 0
	case native.EntryKindFifo:
		mode = syscall.S_IFIFO | 0600
		size = 0
	case native.EntryKindSocket:
		mode = syscall.S_IFSOCK | 0600
		size = 0
	default:
		mode = syscall.S_IFREG | 0644
		size = entry.FileSize()
	}

	if metadata := entry.Metadata(); metadata != nil {
		mode = uint32(metadata.Stat.Mode)
		out.Uid = metadata.Stat.UID
		out.Gid = metadata.Stat.GID
		out.Mtime = uint64(metadata.Stat.Mtime.Secs)
		out.Mtimensec = metadata.Stat.Mtime.Nanos
	}

	out.Mode = mode
	out.Size = size
	out.Blocks = (size + 511) / 512

	return 0
}

func (n *Node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	fullPath := path.Join(n.getPath(), name)

	entry, err := n.accessor.Lookup(fullPath)
	if err != nil {
		return nil, syscall.ENOENT
	}

	childNode := &Node{
		accessor: n.accessor,
		entry:    entry,
		name:     name,
	}

	var mode uint32
	switch entry.Kind() {
	case native.EntryKindDirectory:
		mode = syscall.S_IFDIR | uint32(entry.Metadata().Stat.Mode&0777)
	case native.EntryKindFile:
		mode = syscall.S_IFREG | uint32(entry.Metadata().Stat.Mode&0777)
	case native.EntryKindSymlink:
		mode = syscall.S_IFLNK | uint32(entry.Metadata().Stat.Mode&0777)
	default:
		mode = syscall.S_IFREG | 0644
	}

	stable := fs.StableAttr{Mode: mode}
	child := n.NewInode(ctx, childNode, stable)

	out.Mode = mode
	out.Size = entry.FileSize()
	if metadata := entry.Metadata(); metadata != nil {
		out.Uid = metadata.Stat.UID
		out.Gid = metadata.Stat.GID
		mtime := time.Unix(metadata.Stat.Mtime.Secs, int64(metadata.Stat.Mtime.Nanos))
		out.SetTimes(nil, &mtime, nil)
	}

	return child, 0
}

func (n *Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entry, err := n.getEntry(ctx)
	if err != nil {
		return nil, syscall.ENOENT
	}

	entries, err := entry.ReadDir()
	if err != nil {
		return nil, syscall.EIO
	}

	dirEntries := make([]fuse.DirEntry, 0, len(entries))
	for _, e := range entries {
		var mode uint32
		switch e.Kind() {
		case native.EntryKindDirectory:
			mode = syscall.S_IFDIR
		case native.EntryKindFile:
			mode = syscall.S_IFREG
		case native.EntryKindSymlink:
			mode = syscall.S_IFLNK
		default:
			mode = syscall.S_IFREG
		}

		dirEntries = append(dirEntries, fuse.DirEntry{
			Name: e.Name(),
			Mode: mode,
		})
	}

	return fs.NewListDirStream(dirEntries), 0
}

func (n *Node) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	entry, err := n.getEntry(ctx)
	if err != nil {
		return nil, 0, syscall.ENOENT
	}

	contents, err := entry.Contents()
	if err != nil {
		return nil, 0, syscall.EIO
	}

	return &FileHandle{contents: contents}, 0, 0
}

func (n *Node) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	out.Blocks = 1 << 30
	out.Bfree = 0
	out.Bavail = 0
	out.Files = 1 << 20
	out.Ffree = 0
	out.Bsize = 4096
	out.NameLen = 255
	out.Frsize = 4096
	return 0
}

func (n *Node) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	entry, err := n.getEntry(ctx)
	if err != nil {
		return nil, syscall.ENOENT
	}

	if entry.Kind() != native.EntryKindSymlink {
		return nil, syscall.EINVAL
	}

	target, err := entry.GetSymlink()
	if err != nil {
		return nil, syscall.EIO
	}

	return []byte(target), 0
}

func (n *Node) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	entry, err := n.getEntry(ctx)
	if err != nil {
		return 0, syscall.ENOENT
	}

	metadata := entry.Metadata()
	if metadata == nil {
		return 0, syscall.ENODATA
	}

	if attr == "security.capability" && len(metadata.FCaps) > 0 {
		if dest == nil {
			return uint32(len(metadata.FCaps)), 0
		}
		if len(dest) < len(metadata.FCaps) {
			return uint32(len(metadata.FCaps)), syscall.ERANGE
		}
		copy(dest, metadata.FCaps)
		return uint32(len(metadata.FCaps)), 0
	}

	for _, xattr := range metadata.Xattrs() {
		name := xattr.Name()
		if string(name) == attr {
			value := xattr.Value()
			if dest == nil {
				return uint32(len(value)), 0
			}
			if len(dest) < len(value) {
				return uint32(len(value)), syscall.ERANGE
			}
			copy(dest, value)
			return uint32(len(value)), 0
		}
	}

	return 0, syscall.ENODATA
}

func (n *Node) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	entry, err := n.getEntry(ctx)
	if err != nil {
		return 0, syscall.ENOENT
	}

	metadata := entry.Metadata()
	if metadata == nil {
		return 0, 0
	}

	var list []byte

	if len(metadata.FCaps) > 0 {
		list = append(list, "security.capability"...)
		list = append(list, 0)
	}

	for _, xattr := range metadata.Xattrs() {
		list = append(list, xattr.Name()...)
		list = append(list, 0)
	}

	if dest == nil {
		return uint32(len(list)), 0
	}

	if len(dest) < len(list) {
		return uint32(len(list)), syscall.ERANGE
	}

	copy(dest, list)
	return uint32(len(list)), 0
}

func (n *Node) getEntry(ctx context.Context) (*native.FileEntry, error) {
	if n.entry != nil {
		return n.entry, nil
	}
	return n.accessor.Lookup(n.getPath())
}

type FileHandle struct {
	contents *native.FileContents
}

var _ = (fs.FileReader)((*FileHandle)(nil))
var _ = (fs.FileReleaser)((*FileHandle)(nil))

func (fh *FileHandle) Read(ctx context.Context, dest []byte, offset int64) (fuse.ReadResult, syscall.Errno) {
	n, err := fh.contents.ReadAt(dest, offset)
	if err != nil && err != io.EOF {
		return nil, syscall.EIO
	}

	return fuse.ReadResultData(dest[:n]), 0
}

func (fh *FileHandle) Release(ctx context.Context) syscall.Errno {
	return 0
}
