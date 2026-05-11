// Command pxar-mount mounts a PBS pxar archive via FUSE with minimal memory.
// It uses lazy chunk loading through transfer.SplitArchiveReader — only chunks
// needed for the current FUSE operation are fetched from the datastore.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/accessor"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"
)

const (
	rootInode uint64 = 1
	nonDirBit uint64 = 1 << 63
)

// isDirInode reports whether the inode represents a directory.
func isDirInode(ino uint64) bool { return ino&nonDirBit == 0 }

// pxarFS implements fuse.RawFileSystem backed by a lazy-loading SplitArchiveReader.
type pxarFS struct {
	fuse.RawFileSystem

	reader *transfer.SplitArchiveReader
	size   int64

	mu    sync.Mutex
	nodes map[uint64]*node         // inode → cached entry
	dirs  map[uint64][]cachedEntry // inode → children
}

type node struct {
	entry  pxar.Entry // copy of the full pxar entry
	inode  uint64
	parent uint64
	refs   int64
}

type cachedEntry struct {
	name  string
	inode uint64
	mode  uint32
}

func (n *node) ref() { n.refs++ }

// toInode computes the inode number from a pxar.Entry.
func toInode(e *pxar.Entry) uint64 {
	if e.IsDir() {
		return e.FileOffset + e.FileSize
	}
	return e.FileOffset | nonDirBit
}

func main() {
	pbsStore := flag.String("pbs-store", "", "PBS datastore root path")
	mpxarDidx := flag.String("mpxar-didx", "", "Path to metadata dynamic index (.mpxar.didx)")
	ppxarDidx := flag.String("ppxar-didx", "", "Path to payload dynamic index (.ppxar.didx)")
	verbose := flag.Bool("verbose", false, "Enable verbose logging")

	flag.Parse()

	if *pbsStore == "" || *mpxarDidx == "" || *ppxarDidx == "" {
		fmt.Fprintf(os.Stderr, "Usage: pxar-mount --pbs-store <path> --mpxar-didx <path> --ppxar-didx <path> [--verbose] <mountpoint>\n")
		os.Exit(1)
	}

	args := flag.Args()
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Error: mountpoint required\n")
		os.Exit(1)
	}
	mountPoint := args[0]

	if *verbose {
		fmt.Fprintf(os.Stderr, "pxar-mount: datastore=%s metadata=%s payload=%s mount=%s\n",
			*pbsStore, *mpxarDidx, *ppxarDidx, mountPoint)
	}

	store, err := datastore.NewChunkStore(*pbsStore)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening chunk store: %v\n", err)
		os.Exit(1)
	}
	source := datastore.NewChunkStoreSource(store)

	metaData, err := os.ReadFile(*mpxarDidx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading metadata index: %v\n", err)
		os.Exit(1)
	}
	payloadData, err := os.ReadFile(*ppxarDidx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading payload index: %v\n", err)
		os.Exit(1)
	}

	reader, err := transfer.NewSplitArchiveReader(metaData, payloadData, source)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating split archive reader: %v\n", err)
		os.Exit(1)
	}
	defer reader.Close()

	root, err := reader.ReadRoot()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading root: %v\n", err)
		os.Exit(1)
	}
	fs := &pxarFS{
		reader: reader,
		nodes:  make(map[uint64]*node),
		dirs:   make(map[uint64][]cachedEntry),
	}
	fs.size = int64(root.FileOffset + root.FileSize)

	rootNode := &node{
		entry:  *root,
		inode:  rootInode,
		parent: rootInode,
		refs:   1,
	}
	fs.nodes[rootInode] = rootNode

	server, err := fuse.NewServer(fs, mountPoint, &fuse.MountOptions{
		Name:    "pxar-mount",
		Options: []string{"ro", "default_permissions"},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating FUSE server: %v\n", err)
		os.Exit(1)
	}

	if *verbose {
		fmt.Fprintf(os.Stderr, "pxar-mount: serving at %s (lazy, no eager reconstruction)\n", mountPoint)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		_ = server.Unmount()
	}()

	server.Serve()
}

// --- fuse.RawFileSystem ---

func (fs *pxarFS) Init(server *fuse.Server) {
	fs.RawFileSystem = fuse.NewDefaultRawFileSystem()
	fs.RawFileSystem.Init(server)
}

func (fs *pxarFS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	fs.mu.Lock()
	children, err := fs.loadDir(header.NodeId)
	fs.mu.Unlock()
	if err != nil {
		return fuse.ToStatus(err)
	}

	for _, c := range children {
		if c.name == name {
			fs.mu.Lock()
			n := fs.nodes[c.inode]
			if n != nil {
				n.ref()
			}
			fs.mu.Unlock()
			if n == nil {
				return fuse.ENOENT
			}
			fillEntryOut(c.inode, n, out)
			return fuse.OK
		}
	}
	return fuse.ENOENT
}

func (fs *pxarFS) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	fs.mu.Lock()
	n := fs.nodes[input.NodeId]
	fs.mu.Unlock()
	if n == nil {
		return fuse.ENOENT
	}
	fillAttrOut(n, out)
	return fuse.OK
}

func (fs *pxarFS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	return fuse.OK
}

func (fs *pxarFS) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	fs.mu.Lock()
	children, err := fs.loadDir(input.NodeId)
	fs.mu.Unlock()
	if err != nil {
		return fuse.ToStatus(err)
	}

	if input.Offset == 0 {
		fs.mu.Lock()
		n := fs.nodes[input.NodeId]
		fs.mu.Unlock()
		mode := uint32(syscall.S_IFDIR | 0o555)
		if n != nil {
			mode = statMode(n.entry.Metadata.Stat.Mode)
		}
		if !out.AddDirEntry(fuse.DirEntry{Name: ".", Ino: input.NodeId, Mode: mode}) {
			return fuse.OK
		}
	}

	offset := input.Offset
	if offset == 0 {
		offset = 1
	}
	for _, c := range children {
		if offset > 1 {
			offset--
			continue
		}
		if !out.AddDirEntry(fuse.DirEntry{Name: c.name, Ino: c.inode, Mode: c.mode}) {
			break
		}
	}
	return fuse.OK
}

func (fs *pxarFS) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	return fuse.OK
}

func (fs *pxarFS) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	fs.mu.Lock()
	n := fs.nodes[input.NodeId]
	fs.mu.Unlock()
	if n == nil {
		return nil, fuse.ENOENT
	}
	if isDirInode(input.NodeId) {
		return nil, fuse.EISDIR
	}

	rc, err := fs.reader.ReadFileContentReader(&n.entry)
	if err != nil {
		return nil, fuse.EIO
	}
	defer rc.Close()

	if input.Offset > 0 {
		if _, err := io.CopyN(io.Discard, rc, int64(input.Offset)); err != nil {
			return nil, fuse.EIO
		}
	}

	nr, err := io.ReadFull(rc, buf)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return nil, fuse.EIO
	}
	if nr == 0 {
		return fuse.ReadResultData(nil), fuse.OK
	}
	return fuse.ReadResultData(buf[:nr]), fuse.OK
}

func (fs *pxarFS) Readlink(cancel <-chan struct{}, header *fuse.InHeader) ([]byte, fuse.Status) {
	fs.mu.Lock()
	n := fs.nodes[header.NodeId]
	fs.mu.Unlock()
	if n == nil {
		return nil, fuse.ENOENT
	}
	if n.entry.LinkTarget == "" {
		return nil, fuse.EINVAL
	}
	return []byte(n.entry.LinkTarget), fuse.OK
}

func (fs *pxarFS) ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (uint32, fuse.Status) {
	fs.mu.Lock()
	n := fs.nodes[header.NodeId]
	fs.mu.Unlock()
	if n == nil {
		return 0, fuse.ENOENT
	}

	var sz uint32
	for _, xa := range n.entry.Metadata.XAttrs {
		sz += uint32(len(xa.Name())) + 1
	}
	if n.entry.Metadata.FCaps != nil {
		sz += uint32(len("security.capability")) + 1
	}
	if dest == nil {
		return sz, fuse.OK
	}
	if uint32(len(dest)) < sz {
		return 0, fuse.Status(syscall.ERANGE)
	}

	pos := 0
	for _, xa := range n.entry.Metadata.XAttrs {
		name := xa.Name()
		copy(dest[pos:], name)
		pos += len(name)
		dest[pos] = 0
		pos++
	}
	if n.entry.Metadata.FCaps != nil {
		name := "security.capability"
		copy(dest[pos:], name)
		pos += len(name)
		dest[pos] = 0
	}
	return sz, fuse.OK
}

func (fs *pxarFS) GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, dest []byte) (uint32, fuse.Status) {
	fs.mu.Lock()
	n := fs.nodes[header.NodeId]
	fs.mu.Unlock()
	if n == nil {
		return 0, fuse.ENOENT
	}

	if attr == "security.capability" && n.entry.Metadata.FCaps != nil {
		return xattrValue(n.entry.Metadata.FCaps, dest)
	}
	for _, xa := range n.entry.Metadata.XAttrs {
		if bytesEq(xa.Name(), attr) {
			return xattrValue(xa.Value(), dest)
		}
	}
	return 0, fuse.Status(syscall.ENODATA)
}

func (fs *pxarFS) StatFs(cancel <-chan struct{}, header *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	out.Blocks = uint64(fs.size / 512)
	out.Bsize = 4096
	out.NameLen = 255
	out.Frsize = 512
	return fuse.OK
}

func (fs *pxarFS) Access(cancel <-chan struct{}, input *fuse.AccessIn) fuse.Status { return fuse.OK }

func (fs *pxarFS) Release(cancel <-chan struct{}, input *fuse.ReleaseIn) {}

func (fs *pxarFS) Forget(nodeID, nlookup uint64) {
	fs.mu.Lock()
	if n, ok := fs.nodes[nodeID]; ok {
		n.refs -= int64(nlookup)
		if n.refs <= 0 {
			delete(fs.nodes, nodeID)
			delete(fs.dirs, nodeID)
		}
	}
	fs.mu.Unlock()
}

// --- directory loading ---

func (fs *pxarFS) loadDir(inode uint64) ([]cachedEntry, error) {
	if cached, ok := fs.dirs[inode]; ok {
		return cached, nil
	}

	n, ok := fs.nodes[inode]
	if !ok {
		return nil, syscall.ENOENT
	}
	if !isDirInode(inode) {
		return nil, syscall.ENOTDIR
	}
	if !n.entry.IsDir() {
		return nil, syscall.ENOTDIR
	}

	var children []cachedEntry
	listErr := fs.reader.ListDirectory(int64(n.entry.ContentOffset), accessor.ListOption{}, func(e *pxar.Entry) error {
		childIno := toInode(e)
		if _, exists := fs.nodes[childIno]; !exists {
			fs.nodes[childIno] = &node{
				entry:  *e, // copy the full entry (slices alias the accessor's buffers)
				inode:  childIno,
				parent: inode,
				refs:   1,
			}
		}
		children = append(children, cachedEntry{
			name:  e.FileName(),
			inode: childIno,
			mode:  statMode(e.Metadata.Stat.Mode),
		})
		return nil
	})
	if listErr != nil {
		return nil, listErr
	}

	fs.dirs[inode] = children
	return children, nil
}

// --- helpers ---

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
	sz := n.entry.FileSize
	stat := n.entry.Metadata.Stat
	attr.Ino = n.inode
	attr.Size = sz
	attr.Blocks = (sz + 511) / 512
	attr.Atime = uint64(stat.Mtime.Secs)
	attr.Mtime = uint64(stat.Mtime.Secs)
	attr.Ctime = uint64(stat.Mtime.Secs)
	attr.Atimensec = stat.Mtime.Nanos
	attr.Mtimensec = stat.Mtime.Nanos
	attr.Ctimensec = stat.Mtime.Nanos
	attr.Mode = statMode(stat.Mode)
	if isDirInode(n.inode) {
		attr.Nlink = 2
	} else {
		attr.Nlink = 1
	}
	attr.Uid = stat.UID
	attr.Gid = stat.GID
	attr.Blksize = 4096
}

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
