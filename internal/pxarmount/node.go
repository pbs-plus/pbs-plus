package pxarmount

import (
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/format"
)

func ToInode(e *pxar.Entry) uint64 {
	if e.IsDir() {
		return e.FileOffset + e.FileSize
	}
	return e.FileOffset | NonDirBit
}

type node struct {
	inode         uint64
	parent        uint64
	entryStart    uint64
	contentOffset uint64
	fileSize      uint64
	mode          uint64
	refs          int64
	mtimeSecs     int64
	atimeNs       int64
	mtimeNs       int64
	uid           uint32
	gid           uint32
	mtimeNanos    uint32
	isDir         bool
	isSymlink     bool
	isReg         bool
	timesResolved bool
}

// dirEntrySlim is a lightweight directory entry for readdir results.
// Fields ordered largest-to-smallest for minimal padding.

// dirEntrySlim is a lightweight directory entry for readdir results.
// Fields ordered largest-to-smallest for minimal padding.
type dirEntrySlim struct {
	name          string
	inode         uint64
	entryStart    uint64
	contentOffset uint64
	payloadOffset uint64
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

// ResolvedEntry is the result of path resolution.
type ResolvedEntry struct {
	Path       string
	Inode      uint64
	Node       *GraphNode // non-nil if the inode graph has a node for this path
	PxarNode   *node
	DataIsMut  bool
	IsDir      bool
	Mode       uint32
	UID        uint32
	GID        uint32
	Size       uint64
	AtimeNs    int64 // effective atime (xattr-derived, mirroring restore)
	MtimeNs    int64
	CtimeNs    int64
	SymlinkTgt string
}

var copyBufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 1024*1024)
		return &buf
	},
}

type passFh struct {
	fd int
}

type snapshotRef struct {
	BackupType  string
	BackupID    string
	Namespace   string
	ArchiveName string
	BackupTime  int64
}

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

func fillEntryOut(inode uint64, n *node, out *fuse.EntryOut) {
	out.NodeId = inode
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	out.AttrValidNsec = uint32(time.Second)
	fillAttr(&out.Attr, n)
}

func fillAttrOut(n *node, out *fuse.AttrOut) {
	out.AttrValid = 1
	out.AttrValidNsec = uint32(time.Second)
	fillAttr(&out.Attr, n)
}

func fillAttr(attr *fuse.Attr, n *node) {
	attr.Ino = n.inode
	attr.Size = n.fileSize
	attr.Blocks = (n.fileSize + 511) / 512
	// Atime/Mtime/Ctime mirror restore's precedence (restore_unix.go
	// applyMeta): default to pxar Stat.Mtime; override atime/mtime from the
	// user.lastaccesstime/user.lastwritetime xattrs (Unix seconds, nanos
	// dropped) when present. Ctime is not preserved by restore (kernel-owned),
	// so we report mtime for it as the closest stable approximation.
	if n.timesResolved {
		attr.Atime = uint64(n.atimeNs / 1_000_000_000)
		attr.Mtime = uint64(n.mtimeNs / 1_000_000_000)
		attr.Ctime = uint64(n.mtimeNs / 1_000_000_000)
		attr.Atimensec = uint32(n.atimeNs % 1_000_000_000)
		attr.Mtimensec = uint32(n.mtimeNs % 1_000_000_000)
		attr.Ctimensec = uint32(n.mtimeNs % 1_000_000_000)
	} else {
		attr.Atime = uint64(n.mtimeSecs)
		attr.Mtime = uint64(n.mtimeSecs)
		attr.Ctime = uint64(n.mtimeSecs)
		attr.Atimensec = n.mtimeNanos
		attr.Mtimensec = n.mtimeNanos
		attr.Ctimensec = n.mtimeNanos
	}
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

func nodeKindFromPxar(n *node) uint8 {
	if n.isDir {
		return NodeDir
	}
	if n.isSymlink {
		return NodeSymlink
	}
	return NodeFile
}

const (
	RootInode uint64 = 1
	NonDirBit uint64 = 1 << 63

	// JournalDir is the hidden directory inside the mutable backing dir
	// where the SQLite journal database lives.
	JournalDir = ".pxar-journal"

	XattrCreate  = 1
	XattrReplace = 2
)

// POSIX ACL tag constants (matching Linux kernel definitions).
const (
	ACLUserObj  uint16 = 0x01
	ACLUser     uint16 = 0x02
	ACLGroupObj uint16 = 0x04
	ACLGroup    uint16 = 0x08
	ACLMask     uint16 = 0x10
	ACLOther    uint16 = 0x20

	ACLXAttrVersion uint32 = 0x0002
)
