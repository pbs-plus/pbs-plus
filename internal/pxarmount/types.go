package pxarmount

import (
	"io"
	"os"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/hanwen/go-fuse/v2/fuse"
	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/format"
)

const (
	RootInode uint64 = 1
	NonDirBit uint64 = 1 << 63

	// Backed inodes start above the pxar range.
	backedInoBase uint64 = 1 << 60

	// TransactionsDir is the special directory inside the backing dir
	// where the transaction log is stored. Hidden from FUSE readdir
	// and excluded from overlay walks.
	TransactionsDir = ".pxar-transactions"

	// RENAME flags (Linux FUSE protocol, fuse_kernel.h).
	renameNoReplace = 1 << 0
	renameExchange  = 1 << 1

	// XATTR flags (Linux FUSE protocol).
	xattrCreate  = 1
	xattrReplace = 2
)

// IsDirInode reports whether the inode represents a directory.
func IsDirInode(ino uint64) bool { return ino&NonDirBit == 0 }

// ToInode computes a stable inode number from a pxar entry.
func ToInode(e *pxar.Entry) uint64 {
	if e.IsDir() {
		return e.FileOffset + e.FileSize
	}
	return e.FileOffset | NonDirBit
}

// node holds cached metadata for a single filesystem entry.
// Fields ordered largest→smallest to minimize padding.
type node struct {
	entryStart    uint64 // FileOffset in archive
	contentOffset uint64 // ContentOffset for directories (children start)
	fileSize      uint64
	mode          uint64
	inode         uint64
	parent        uint64
	refs          int64
	mtimeSecs     int64
	uid           uint32
	mtimeNanos    uint32
	gid           uint32
	isDir         bool
	isSymlink     bool
	isReg         bool
	_             byte // explicit pad
}

// dirEntrySlim is a lightweight directory entry for readdir results.
type dirEntrySlim struct {
	name          string
	entryStart    uint64
	contentOffset uint64
	fileSize      uint64
	mode          uint32
	uid           uint32
	gid           uint32
	inode         uint64
	mtimeSecs     int64
	mtimeNanos    uint32
	isDir         bool
	isSymlink     bool
	isReg         bool
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

// passFh is an open file handle backed by a real fd.
type passFh struct {
	fd    int
	inode uint64
}

// snapshotRef identifies a PBS snapshot for commit dedup.
type snapshotRef struct {
	BackupType  string
	BackupID    string
	Namespace   string
	ArchiveName string
	BackupTime  int64
}

// --- helpers ---

// newNodeFromEntry creates a value-type node from a pxar.Entry.
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

// nodeFromStat creates a minimal node from a syscall.Stat_t.
func nodeFromStat(st *syscall.Stat_t) *node {
	return &node{
		fileSize:   uint64(st.Size),
		mode:       uint64(st.Mode),
		mtimeSecs:  int64(st.Mtim.Sec),
		uid:        st.Uid,
		mtimeNanos: uint32(st.Mtim.Nsec),
		gid:        st.Gid,
		isDir:      st.Mode&syscall.S_IFDIR != 0,
		isSymlink:  st.Mode&syscall.S_IFLNK != 0,
		isReg:      st.Mode&syscall.S_IFREG != 0,
	}
}

// fillEntryOut populates a FUSE EntryOut from a node.
func fillEntryOut(inode uint64, n *node, out *fuse.EntryOut) {
	out.NodeId = inode
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	out.AttrValidNsec = uint32(time.Second)
	fillAttr(&out.Attr, n)
}

// fillAttrOut populates a FUSE AttrOut from a node.
func fillAttrOut(n *node, out *fuse.AttrOut) {
	out.AttrValid = 1
	out.AttrValidNsec = uint32(time.Second)
	fillAttr(&out.Attr, n)
}

// fillAttr fills a FUSE Attr from a node.
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

// statMode converts a pxar mode to a Linux syscall mode.
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

// joinPath builds a child path from a parent path and a name.
func joinPath(parent, name string) string {
	if parent == "/" {
		return "/" + name
	}
	return parent + "/" + name
}

// xattrValue copies an xattr value into dest, or returns the size if dest is nil.
func xattrValue(val []byte, dest []byte) (uint32, fuse.Status) {
	if dest == nil {
		return uint32(len(val)), fuse.OK
	}
	if uint32(len(dest)) < uint32(len(val)) {
		return 0, fuse.Status(syscall.ERANGE)
	}
	copy(dest, val)
	return uint32(len(val)), fuse.OK
}

// bytesEq compares a byte slice with a string without allocation.
func bytesEq(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := range b {
		if b[i] != s[i] {
			return false
		}
	}
	return true
}

// unsafeStringBytes returns the byte slice backing a string.
func unsafeStringBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// mmapFile memory-maps a file for reading without heap allocation.
func mmapFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if fi.Size() == 0 {
		return nil, nil
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		// Fallback to ReadFile if mmap unavailable.
		if _, err2 := f.Seek(0, io.SeekStart); err2 != nil {
			return nil, err
		}
		return io.ReadAll(f)
	}
	return data, nil
}

// munmap releases a memory-mapped region.
func munmap(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return syscall.Munmap(data)
}
