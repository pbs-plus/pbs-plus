package pxarmount

import (
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"strings"
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

func ToInode(e *pxar.Entry) uint64 {
	if e.IsDir() {
		return e.FileOffset + e.FileSize
	}
	return e.FileOffset | NonDirBit
}

// node holds cached metadata for a single filesystem entry.
// Fields ordered for minimal padding: 8-byte words first, then 4-byte,
// then bools packed together. Total: 80 bytes (down from 96 with padding).
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

type ACLConfig struct {
	OwnerUID int
	OwnerGID int

	// Full POSIX ACL entries. When set, these are served as virtual
	// system.posix_acl_access / system.posix_acl_default xattrs.
	ACLEntries        []ACLEntry
	DefaultACLEntries []ACLEntry
}

type ACLEntry struct {
	Tag  uint16 // ACL_USER_OBJ, ACL_USER, ACL_GROUP_OBJ, ACL_GROUP, ACL_MASK, ACL_OTHER
	Perm uint16 // permission bits (r=4, w=2, x=1)
	ID   uint32 // UID or GID for ACL_USER / ACL_GROUP entries
}

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

// MarshalACL encodes POSIX ACL entries into the kernel binary format
// used by system.posix_acl_access and system.posix_acl_default.
func MarshalACL(entries []ACLEntry) []byte {
	// version (4 bytes) + N entries × 8 bytes each
	buf := make([]byte, 4+len(entries)*8)
	binary.LittleEndian.PutUint32(buf[:4], ACLXAttrVersion)
	for i, e := range entries {
		off := 4 + i*8
		binary.LittleEndian.PutUint16(buf[off:off+2], e.Tag)
		binary.LittleEndian.PutUint16(buf[off+2:off+4], e.Perm)
		binary.LittleEndian.PutUint32(buf[off+4:off+8], e.ID)
	}
	return buf
}

// Format: one entry per line, e.g.
//
//	user::rwx
//	user:backupadmin:rwx
//	group::rwx
//	group:it:rwx
//	mask::rwx
//	other::---
func ParseACLSpec(spec string) ([]ACLEntry, error) {
	var entries []ACLEntry
	// Accept both \n and ; as delimiters.
	spec = strings.ReplaceAll(spec, ";", "\n")
	for line := range strings.SplitSeq(spec, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		e, err := parseACLEntry(line)
		if err != nil {
			return nil, fmt.Errorf("parse ACL line %q: %w", line, err)
		}
		entries = append(entries, e)
	}
	return entries, nil
}

func parseACLEntry(line string) (ACLEntry, error) {
	// Split into type:name:perm or type::perm
	parts := strings.SplitN(line, ":", 3)
	if len(parts) < 3 {
		return ACLEntry{}, fmt.Errorf("invalid format")
	}
	kind, name, perm := parts[0], parts[1], parts[2]

	var e ACLEntry
	switch kind {
	case "user":
		if name == "" {
			e.Tag = ACLUserObj
		} else {
			e.Tag = ACLUser
			uid, err := lookupUID(name)
			if err != nil {
				return ACLEntry{}, fmt.Errorf("unknown user %q: %w", name, err)
			}
			e.ID = uid
		}
	case "group":
		if name == "" {
			e.Tag = ACLGroupObj
		} else {
			e.Tag = ACLGroup
			gid, err := lookupGID(name)
			if err != nil {
				return ACLEntry{}, fmt.Errorf("unknown group %q: %w", name, err)
			}
			e.ID = gid
		}
	case "mask":
		e.Tag = ACLMask
	case "other":
		e.Tag = ACLOther
	default:
		return ACLEntry{}, fmt.Errorf("unknown ACL type %q", kind)
	}

	e.Perm = parsePerm(perm)
	return e, nil
}

func parsePerm(s string) uint16 {
	var p uint16
	for _, c := range s {
		switch c {
		case 'r':
			p |= 4
		case 'w':
			p |= 2
		case 'x':
			p |= 1
		}
	}
	return p
}

func lookupUID(name string) (uint32, error) {
	// Try Go's user.Lookup first (uses NSS when dynamically linked).
	if u, err := user.Lookup(name); err == nil {
		uid, _ := strconv.ParseUint(u.Uid, 10, 32)
		return uint32(uid), nil
	}
	// Fallback: try getent which respects NSS/winbind even from
	// statically-linked binaries.
	out, err := exec.Command("getent", "passwd", name).Output()
	if err != nil {
		return 0, fmt.Errorf("unknown user %q", name)
	}
	// getent passwd output: name:*:uid:gid:...
	fields := strings.SplitN(strings.TrimSpace(string(out)), ":", 4)
	if len(fields) < 3 {
		return 0, fmt.Errorf("malformed getent output for user %q", name)
	}
	uid, err := strconv.ParseUint(fields[2], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("bad uid for user %q: %w", name, err)
	}
	return uint32(uid), nil
}

func lookupGID(name string) (uint32, error) {
	if g, err := user.LookupGroup(name); err == nil {
		gid, _ := strconv.ParseUint(g.Gid, 10, 32)
		return uint32(gid), nil
	}
	out, err := exec.Command("getent", "group", name).Output()
	if err != nil {
		return 0, fmt.Errorf("unknown group %q", name)
	}
	fields := strings.SplitN(strings.TrimSpace(string(out)), ":", 4)
	if len(fields) < 3 {
		return 0, fmt.Errorf("malformed getent output for group %q", name)
	}
	gid, err := strconv.ParseUint(fields[2], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("bad gid for group %q: %w", name, err)
	}
	return uint32(gid), nil
}

func (c ACLConfig) HasACLs() bool {
	return len(c.ACLEntries) > 0
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

func joinPath(parent, name string) string {
	if parent == "/" {
		return "/" + name
	}
	return parent + "/" + name
}

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

func BuildACLConfig(ownerUID, ownerGID int, aclSpec, defaultAclSpec string) ACLConfig {
	cfg := ACLConfig{
		OwnerUID: ownerUID,
		OwnerGID: ownerGID,
	}
	if aclSpec != "" {
		entries, err := ParseACLSpec(aclSpec)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing acl-spec: %v\n", err)
			os.Exit(1)
		}
		cfg.ACLEntries = entries
	}
	if defaultAclSpec != "" {
		entries, err := ParseACLSpec(defaultAclSpec)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing default-acl-spec: %v\n", err)
			os.Exit(1)
		}
		cfg.DefaultACLEntries = entries
	}
	return cfg
}
