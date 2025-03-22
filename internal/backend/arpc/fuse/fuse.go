//go:build linux

package fuse

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/agentfs/types"
	arpcfs "github.com/sonroyaalmerol/pbs-plus/internal/backend/arpc"
)

var nodePool = &sync.Pool{
	New: func() any {
		return &Node{}
	},
}

var pathPool = &sync.Pool{
	New: func() interface{} {
		return make([]byte, 256)
	},
}

var pathBuilderPool = &sync.Pool{
	New: func() interface{} {
		return make([]string, 128)
	},
}

func newRoot(fs *arpcfs.ARPCFS) fs.InodeEmbedder {
	rootNode := nodePool.Get().(*Node)
	rootNode.fs = fs
	rootNode.fullPathCache = ""
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
	fs            *arpcfs.ARPCFS
	name          string
	fullPathCache string
	parent        *Node
}

func (n *Node) getPath() string {
	if len(n.fullPathCache) > 0 || n.parent == nil {
		return n.fullPathCache
	}

	pathBytes := pathPool.Get().([]byte)

	parts := pathBuilderPool.Get().([]string)
	defer pathBuilderPool.Put(parts)
	numParts := 0

	if n.parent.fullPathCache != "" {
		parts[0] = n.name
		parts[1] = n.parent.fullPathCache
		numParts = 2
	} else {
		for current := n; current != nil; current = current.parent {
			if current.parent != nil {
				if numParts <= len(parts) {
					parts[numParts] = current.name
				} else {
					parts = append(parts, current.name)
				}
				numParts++
			}
		}
	}

	totalLen := 0
	for i := numParts - 1; i >= 0; i-- {
		currPart := parts[i]
		currLen := len(parts[i])

		copy(pathBytes[totalLen:], currPart)
		totalLen += currLen
		if currLen > 0 && currPart[currLen-1] != '/' && i-1 >= 0 {
			if totalLen <= len(pathBytes) {
				pathBytes[totalLen] = '/'
			} else {
				pathBytes = append(pathBytes, '/')
			}
			totalLen++
		}
	}

	n.fullPathCache = unsafe.String(unsafe.SliceData(pathBytes[:totalLen]), totalLen)
	return n.fullPathCache
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

func (n *Node) Release(ctx context.Context, f fs.FileHandle) syscall.Errno {
	if fh, ok := f.(fs.FileReleaser); ok {
		return fh.Release(ctx)
	}

	if n.fullPathCache != "" {
		pathPtr := unsafe.Slice(unsafe.StringData(n.fullPathCache), max(4096, len(n.fullPathCache)))
		pathPool.Put(pathPtr)
	}
	nodePool.Put(n)

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
	fi, err := n.fs.Attr(n.getPath())
	if err != nil {
		return fs.ToErrno(err)
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
	fi, err := n.fs.Xattr(n.getPath())
	if err != nil {
		return 0, fs.ToErrno(err)
	}

	var data []byte
	switch attr {
	case "user.creationtime":
		data = strconv.AppendInt(data, fi.CreationTime, 10)
	case "user.lastaccesstime":
		data = strconv.AppendInt(data, fi.LastAccessTime, 10)
	case "user.lastwritetime":
		data = strconv.AppendInt(data, fi.LastWriteTime, 10)
	case "user.owner":
		data = append(data, fi.Owner...)
	case "user.group":
		data = append(data, fi.Group...)
	case "user.fileattributes":
		data, err = json.Marshal(fi.FileAttributes)
		if err != nil {
			return 0, syscall.EIO
		}
	case "user.acls":
		if fi.PosixACLs != nil {
			data, err = json.Marshal(fi.PosixACLs)
			if err != nil {
				return 0, syscall.EIO
			}
		} else if fi.WinACLs != nil {
			data, err = json.Marshal(fi.WinACLs)
			if err != nil {
				return 0, syscall.EIO
			}
		} else {
			return 0, syscall.ENODATA
		}
	default:
		return 0, syscall.ENODATA
	}

	length := uint32(len(data))

	if dest == nil {
		return length, 0
	}

	if len(dest) < len(data) {
		return length, syscall.ERANGE
	}

	copy(dest, data)
	return length, 0
}

func (n *Node) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	// Retrieve extended attribute information for the node.
	fi, err := n.fs.Xattr(n.getPath())
	if err != nil {
		return 0, fs.ToErrno(err)
	}

	// Build our list of supported attribute keys.
	attrs := []string{
		"user.creationtime",
		"user.lastaccesstime",
		"user.lastwritetime",
		"user.owner",
		"user.group",
		"user.fileattributes",
	}

	// Only add ACLs if available.
	if fi.PosixACLs != nil || fi.WinACLs != nil {
		attrs = append(attrs, "user.acls")
	}

	// Create the null-terminated list of attribute names.
	var list []byte
	for _, attr := range attrs {
		list = append(list, attr...)
		list = append(list, 0) // Add null terminator.
	}

	length := uint32(len(list))

	// If dest is nil, just return the required length.
	if dest == nil {
		return length, 0
	}

	// If the provided dest slice is too small, return ERANGE.
	if len(dest) < len(list) {
		return length, syscall.ERANGE
	}

	// Copy the extended attribute list into dest.
	copy(dest, list)
	return length, 0
}

// Lookup implements NodeLookuper
func (n *Node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childNode := nodePool.Get().(*Node)
	childNode.fs = n.fs
	childNode.parent = n
	childNode.name = name
	childNode.fullPathCache = ""

	path := childNode.getPath()
	fi, err := childNode.fs.Attr(path)
	if err != nil {
		return nil, fs.ToErrno(err)
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
	mtime := fi.ModTime
	out.SetTimes(nil, &mtime, nil)

	return child, 0
}

// Readdir implements NodeReaddirer
func (n *Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, err := n.fs.ReadDir(n.getPath())
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	if entries == nil {
		entries = types.ReadDirEntries{}
	}

	result := make([]fuse.DirEntry, 0, len(entries))
	for _, e := range entries {
		mode := os.FileMode(e.Mode)
		modeBits := uint32(0)

		// Determine the file type using fuse.S_IF* constants
		switch {
		case mode.IsDir():
			modeBits = fuse.S_IFDIR
		case mode&os.ModeSymlink != 0:
			modeBits = fuse.S_IFLNK
		default:
			modeBits = fuse.S_IFREG
		}

		// Create a DirEntry with Name, Mode, and Ino
		result = append(result, fuse.DirEntry{
			Name: e.Name,
			Mode: modeBits,
		})
	}

	return fs.NewListDirStream(result), 0
}

// Open implements NodeOpener
func (n *Node) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	file, err := n.fs.OpenFile(n.getPath(), int(flags), 0)
	if err != nil {
		return nil, 0, fs.ToErrno(err)
	}

	return &FileHandle{
		fs:   n.fs,
		file: &file,
	}, 0, 0
}

func (n *Node) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
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
	n, err := fh.file.ReadAt(dest, offset)
	if err != nil && err != io.EOF {
		return nil, fs.ToErrno(err)
	}

	return fuse.ReadResultData(dest[:n]), 0
}

func (fh *FileHandle) Lseek(ctx context.Context, off uint64, whence uint32) (uint64, syscall.Errno) {
	n, err := fh.file.Lseek(int64(off), int(whence))
	if err != nil && err != io.EOF {
		return 0, fs.ToErrno(err)
	}

	return n, 0
}

func (fh *FileHandle) Release(ctx context.Context) syscall.Errno {
	err := fh.file.Close()
	return fs.ToErrno(err)
}
