package pxarmount

import (
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/format"
)

const (
	RootInode uint64 = 1
	NonDirBit uint64 = 1 << 63

	// JournalDir is the hidden directory inside the mutable backing dir
	// where the SQLite journal database lives.
	JournalDir = ".pxar-journal"

	// xattr flags.
	XattrCreate  = 1
	XattrReplace = 2
)

// ToInode computes a stable inode number from a pxar entry.
func ToInode(e *pxar.Entry) uint64 {
	if e.IsDir() {
		return e.FileOffset + e.FileSize
	}
	return e.FileOffset | NonDirBit
}

// node holds cached metadata for a single filesystem entry.
// Fields ordered largest-to-smallest to minimize padding (56 bytes total).
type node struct {
	inode         uint64
	parent        uint64
	entryStart    uint64
	contentOffset uint64
	fileSize      uint64
	mode          uint64
	refs          int64
	mtimeSecs     int64
	uid           uint32
	gid           uint32
	mtimeNanos    uint32
	isDir         bool
	isSymlink     bool
	isReg         bool
}

// dirEntrySlim is a lightweight directory entry for readdir results.
// Fields ordered largest-to-smallest for minimal padding.
type dirEntrySlim struct {
	name          string
	inode         uint64
	entryStart    uint64
	contentOffset uint64
	fileSize      uint64
	mtimeSecs     int64
	mode          uint32
	uid           uint32
	gid           uint32
	mtimeNanos    uint32
	isDir         bool
	isSymlink     bool
	isReg         bool
}

// ResolvedEntry is the result of path resolution.
// It tells the caller where metadata and data come from.
type ResolvedEntry struct {
	Path       string
	Inode      uint64
	Node       *GraphNode // non-nil if the inode graph has a node for this path
	PxarNode   *node      // non-nil if there's an immutable backing entry
	DataIsMut  bool       // data comes from mutable dir
	IsDir      bool
	Mode       uint32
	UID        uint32
	GID        uint32
	Size       uint64
	MtimeNs    int64
	CtimeNs    int64
	SymlinkTgt string
}

// copyBufPool provides 1MB buffers for file copy operations.
var copyBufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 1024*1024)
		return &buf
	},
}

// dirEntryPool reuses dir entry slices across ReadDir calls.
var dirEntryPool = sync.Pool{
	New: func() any {
		s := make([]dirEntrySlim, 0, 64)
		return &s
	},
}

// passFh is an open file handle.
type passFh struct {
	fd int
}

// snapshotRef identifies a PBS snapshot for commit dedup.
type snapshotRef struct {
	BackupType  string
	BackupID    string
	Namespace   string
	ArchiveName string
	BackupTime  int64
}

// ACLConfig configures default ownership behavior.
type ACLConfig struct {
	OwnerUID   int
	OwnerGID   int
	ForceOwner bool
	ForceGroup bool
}

// MountConfig holds all parameters needed to start a pxar-mount FUSE server.
type MountConfig struct {
	PBSStore      string
	Reader        any
	OrigPpxarDidx string
	BackingDir    string
	MountPoint    string
	SocketPath    string
	Namespace     string
	FuseOpts      string
	Verbose       bool
	InitMode      bool
	ACL           ACLConfig
}

// --- Helpers ---

// newNodeFromEntry creates a node from a pxar entry with cached metadata.
func newNodeFromEntry(e *pxar.Entry, inode, parent uint64) node {
	st := e.Metadata.Stat
	return node{
		entryStart:    e.FileOffset,
		contentOffset: e.ContentOffset,
		fileSize:      e.FileSize,
		mode:          st.Mode,
		inode:         inode,
		parent:        parent,
		refs:          1,
		mtimeSecs:     st.Mtime.Secs,
		uid:           st.UID,
		mtimeNanos:    st.Mtime.Nanos,
		gid:           st.GID,
		isDir:         e.IsDir(),
		isSymlink:     e.IsSymlink(),
		isReg:         e.IsRegularFile(),
	}
}

// fillEntryOut fills a FUSE EntryOut from a cached node.
func fillEntryOut(inode uint64, n *node, out *fuse.EntryOut) {
	out.NodeId = inode
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	out.AttrValidNsec = uint32(time.Second)
	fillAttr(&out.Attr, n)
}

// fillAttrOut fills a FUSE AttrOut from a cached node.
func fillAttrOut(n *node, out *fuse.AttrOut) {
	out.AttrValid = 1
	out.AttrValidNsec = uint32(time.Second)
	fillAttr(&out.Attr, n)
}

// fillAttr fills FUSE attributes from a cached node.
func fillAttr(attr *fuse.Attr, n *node) {
	attr.Ino = n.inode
	attr.Size = n.fileSize
	attr.Blocks = (n.fileSize + 511) / 512
	attr.Atime = uint64(n.mtimeSecs)
	attr.Mtime = uint64(n.mtimeSecs)
	attr.Ctime = uint64(n.mtimeSecs)
	attr.Atimensec = n.mtimeNanos
	attr.Mtimensec = n.mtimeNanos
	attr.Ctimensec = n.mtimeNanos
	attr.Mode = statMode(n.mode)
	if n.isDir {
		attr.Nlink = 2
	} else {
		attr.Nlink = 1
	}
	attr.Uid = n.uid
	attr.Gid = n.gid
	attr.Blksize = 4096
}

// statMode converts a pxar mode to a syscall mode with file type bits.
func statMode(mode uint64) uint32 {
	var ft uint32
	switch mode & format.ModeIFMT {
	case format.ModeIFDIR:
		ft = syscall.S_IFDIR
	case format.ModeIFREG:
		ft = syscall.S_IFREG
	case format.ModeIFLNK:
		ft = syscall.S_IFLNK
	case format.ModeIFBLK:
		ft = syscall.S_IFBLK
	case format.ModeIFCHR:
		ft = syscall.S_IFCHR
	case format.ModeIFIFO:
		ft = syscall.S_IFIFO
	case format.ModeIFSOCK:
		ft = syscall.S_IFSOCK
	}
	return ft | uint32(mode&0o7777)
}

// joinPath joins a parent path and a name component.
func joinPath(parent, name string) string {
	if parent == "/" {
		return "/" + name
	}
	return parent + "/" + name
}

// splitPath splits a path into components.
func splitPath(path string) []string {
	if path == "/" || path == "" {
		return nil
	}
	p := path
	if p[0] == '/' {
		p = p[1:]
	}
	var parts []string
	start := 0
	for i := 0; i < len(p); i++ {
		if p[i] == '/' {
			parts = append(parts, p[start:i])
			start = i + 1
		}
	}
	parts = append(parts, p[start:])
	return parts
}

// ensureModeType ensures mode has the correct file type bits for the given node kind.
func ensureModeType(mode uint32, kind uint8) uint32 {
	perm := mode & 0o7777
	var ft uint32
	switch kind {
	case NodeDir:
		ft = syscall.S_IFDIR
	case NodeSymlink:
		ft = syscall.S_IFLNK
	default:
		ft = syscall.S_IFREG
	}
	return ft | perm
}

// nodeKindFromPxar returns the journal node kind for a pxar node.
func nodeKindFromPxar(n *node) uint8 {
	if n.isDir {
		return NodeDir
	}
	if n.isSymlink {
		return NodeSymlink
	}
	return NodeFile
}
