// Command pxar-mount mounts a PBS pxar archive via FUSE with minimal memory.
// Designed for long-running production use (SMB shares, multi-user access).
//
// Key design decisions:
//   - RWMutex for node cache (concurrent reads, serialized writes)
//   - Directory entries read from archive on each ReadDir call (kernel caches
//     them in VFS, so this is only called on cache miss)
//   - No persistent directory cache — avoids memory blowup with large dirs
//   - Nodes created lazily on Lookup/ReadDir; only stat basics stored
//   - File reads use ReadFileContentReader per call (acceptable since kernel
//     does read-ahead and the chunk store caches decoded chunks)
//   - xattrs/symlink targets read from archive on demand via ReadEntryAt
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
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

func isDirInode(ino uint64) bool { return ino&nonDirBit == 0 }

// node holds cached metadata for a single filesystem entry.
// Only stores what's needed for GetAttr — full entry data is
// fetched on demand for xattrs, symlinks, and file content.
type node struct {
	inode         uint64
	parent        uint64
	refs          int64
	entryStart    uint64 // FileOffset in archive
	contentOffset uint64 // ContentOffset for directories (children start)
	fileSize      uint64
	mode          uint64
	uid           uint32
	gid           uint32
	mtimeSecs     int64
	mtimeNanos    uint32
	isDir         bool
	isSymlink     bool
	isReg         bool
}

// pxarFS implements fuse.RawFileSystem backed by a lazy-loading SplitArchiveReader.
type pxarFS struct {
	fuse.RawFileSystem

	reader *transfer.SplitArchiveReader
	size   int64

	mu    sync.RWMutex
	nodes map[uint64]*node // inode → cached stat metadata
}

func toInode(e *pxar.Entry) uint64 {
	if e.IsDir() {
		return e.FileOffset + e.FileSize
	}
	return e.FileOffset | nonDirBit
}

func main() {
	pbsStore := flag.String("pbs-store", "", "PBS datastore root path")
	mpxarDidx := flag.String("mpxar-didx", "", "Path to metadata dynamic index (.mpxar.didx)")
	ppxarDidx := flag.String("ppxar-didx", "", "Path to payload or unified dynamic index (.ppxar.didx)")
	keyfile := flag.String("keyfile", "", "Path to encryption key (accepted for CLI compat, encryption not yet supported)")
	verifyChunks := flag.Bool("verify-chunks", false, "Verify chunk SHA256 on read (accepted for CLI compat)")
	cacheMB := flag.Int("cache-size", 256, "Cache size in MB (accepted for CLI compat)")
	fuseOpts := flag.String("options", "ro,default_permissions", "FUSE mount options")
	verbose := flag.Bool("verbose", false, "Enable verbose logging")

	flag.Parse()

	if *pbsStore == "" || *ppxarDidx == "" {
		fmt.Fprintf(os.Stderr, "Usage: pxar-mount --pbs-store <path> --ppxar-didx <path> [--mpxar-didx <path>] [--verbose] <mountpoint>\n")
		os.Exit(1)
	}

	args := flag.Args()
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Error: mountpoint required\n")
		os.Exit(1)
	}
	mountPoint := args[0]

	// CLI compatibility: accept flags that the Rust binary supported
	_ = keyfile
	_ = verifyChunks
	_ = cacheMB

	if *verbose {
		fmt.Fprintf(os.Stderr, "pxar-mount: datastore=%s metadata=%s payload=%s mount=%s\n",
			*pbsStore, *mpxarDidx, *ppxarDidx, mountPoint)
	}

	// Use the library's ChunkStore (now PBS-compatible with 4-char prefix)
	store, err := datastore.NewChunkStore(*pbsStore)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening chunk store: %v\n", err)
		os.Exit(1)
	}
	source := datastore.NewChunkStoreSource(store)

	var reader *transfer.SplitArchiveReader
	isSplit := *mpxarDidx != ""

	if isSplit {
		// Split archive: metadata + payload indexes
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
		reader, err = transfer.NewSplitArchiveReader(metaData, payloadData, source)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating split archive reader: %v\n", err)
			os.Exit(1)
		}
	} else {
		// Non-split (unified) archive: single .pxar.didx
		idxData, err := os.ReadFile(*ppxarDidx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading index: %v\n", err)
			os.Exit(1)
		}
		chunkedReader, err := transfer.NewChunkedArchiveReader(idxData, source)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating chunked archive reader: %v\n", err)
			os.Exit(1)
		}
		// Wrap as a split reader for unified access — ChunkedArchiveReader
		// provides the same ArchiveReader interface but through a different
		// concrete type. We need a SplitArchiveReader for node registration.
		// For now, reconstruct eagerly and use fusefs.Session.
		// TODO: support ChunkedArchiveReader directly
		_ = chunkedReader
		fmt.Fprintf(os.Stderr, "Error: non-split (unified) .pxar.didx archives not yet supported\n")
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
		size:   int64(root.FileOffset + root.FileSize),
	}

	fs.nodes[rootInode] = nodeFromEntry(root, rootInode, rootInode)

	server, err := fuse.NewServer(fs, mountPoint, &fuse.MountOptions{
		Name:    "pxar-mount",
		Options: strings.Split(*fuseOpts, ","),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating FUSE server: %v\n", err)
		os.Exit(1)
	}

	if *verbose {
		fmt.Fprintf(os.Stderr, "pxar-mount: serving at %s\n", mountPoint)
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
	// Read directory from archive — kernel caches results, so this is
	// only called on cache miss.
	entries, err := fs.readDirRaw(header.NodeId)
	if err != nil {
		return fuse.ToStatus(err)
	}

	for _, e := range entries {
		if e.name == name {
			fs.registerNode(e.inode, header.NodeId, &e.meta)
			fs.mu.RLock()
			nd := fs.nodes[e.inode]
			fs.mu.RUnlock()
			if nd == nil {
				return fuse.ENOENT
			}
			fs.refNode(e.inode)
			fillEntryOut(e.inode, nd, out)
			return fuse.OK
		}
	}
	return fuse.ENOENT
}

func (fs *pxarFS) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	fs.mu.RLock()
	n := fs.nodes[input.NodeId]
	fs.mu.RUnlock()
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
	entries, err := fs.readDirRaw(input.NodeId)
	if err != nil {
		return fuse.ToStatus(err)
	}

	// "." entry first
	if input.Offset == 0 {
		fs.mu.RLock()
		n := fs.nodes[input.NodeId]
		fs.mu.RUnlock()
		mode := uint32(syscall.S_IFDIR | 0o555)
		if n != nil {
			mode = statMode(n.mode)
		}
		if !out.AddDirEntry(fuse.DirEntry{Name: ".", Ino: input.NodeId, Mode: mode}) {
			return fuse.OK
		}
	}

	// ".." entry
	if input.Offset == 0 || input.Offset == 1 {
		fs.mu.RLock()
		n := fs.nodes[input.NodeId]
		fs.mu.RUnlock()
		parentIno := uint64(rootInode)
		mode := uint32(syscall.S_IFDIR | 0o555)
		if n != nil {
			parentIno = n.parent
			fs.mu.RLock()
			pn := fs.nodes[parentIno]
			fs.mu.RUnlock()
			if pn != nil {
				mode = statMode(pn.mode)
			}
		}
		if input.Offset <= 1 {
			if !out.AddDirEntry(fuse.DirEntry{Name: "..", Ino: parentIno, Mode: mode}) {
				return fuse.OK
			}
		}
	}

	// Child entries based on offset (offset 0=".", 1="..", 2=first child, ...)
	start := max(int(input.Offset)-2, 0)

	for i := start; i < len(entries); i++ {
		if !out.AddDirEntry(fuse.DirEntry{
			Name: entries[i].name,
			Ino:  entries[i].inode,
			Mode: entries[i].mode,
		}) {
			break
		}
	}
	return fuse.OK
}

func (fs *pxarFS) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	entries, err := fs.readDirRaw(input.NodeId)
	if err != nil {
		return fuse.ToStatus(err)
	}

	// "." entry first
	if input.Offset == 0 {
		fs.mu.RLock()
		n := fs.nodes[input.NodeId]
		fs.mu.RUnlock()
		mode := uint32(syscall.S_IFDIR | 0o555)
		if n != nil {
			mode = statMode(n.mode)
		}
		eo := out.AddDirLookupEntry(fuse.DirEntry{Name: ".", Ino: input.NodeId, Mode: mode})
		if eo == nil {
			return fuse.OK
		}
		if n != nil {
			fillEntryOut(input.NodeId, n, eo)
		}
	}

	// ".." entry
	if input.Offset == 0 || input.Offset == 1 {
		fs.mu.RLock()
		n := fs.nodes[input.NodeId]
		fs.mu.RUnlock()
		parentIno := uint64(rootInode)
		mode := uint32(syscall.S_IFDIR | 0o555)
		var pn *node
		if n != nil {
			parentIno = n.parent
			fs.mu.RLock()
			pn = fs.nodes[parentIno]
			fs.mu.RUnlock()
			if pn != nil {
				mode = statMode(pn.mode)
			}
		}
		if input.Offset <= 1 {
			eo := out.AddDirLookupEntry(fuse.DirEntry{Name: "..", Ino: parentIno, Mode: mode})
			if eo == nil {
				return fuse.OK
			}
			if pn != nil {
				fillEntryOut(parentIno, pn, eo)
			}
		}
	}

	// Child entries based on offset (offset 0=".", 1="..", 2=first child, ...)
	start := max(int(input.Offset)-2, 0)

	for i := start; i < len(entries); i++ {
		eo := out.AddDirLookupEntry(fuse.DirEntry{
			Name: entries[i].name,
			Ino:  entries[i].inode,
			Mode: entries[i].mode,
		})
		if eo == nil {
			break
		}
		fs.registerNode(entries[i].inode, input.NodeId, &entries[i].meta)
		fs.mu.RLock()
		child := fs.nodes[entries[i].inode]
		fs.mu.RUnlock()
		if child != nil {
			fillEntryOut(entries[i].inode, child, eo)
		}
	}
	return fuse.OK
}

func (fs *pxarFS) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	return fuse.OK
}

func (fs *pxarFS) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	fs.mu.RLock()
	n := fs.nodes[input.NodeId]
	fs.mu.RUnlock()
	if n == nil {
		return nil, fuse.ENOENT
	}
	if n.isDir {
		return nil, fuse.EISDIR
	}

	// Read full entry to get content offset info
	entry, err := fs.readEntryForNode(n)
	if err != nil {
		return nil, fuse.EIO
	}

	rc, err := fs.reader.ReadFileContentReader(entry)
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

// readEntryForNode reads the full pxar entry for a node. For the root inode
// (which has no FILENAME header), returns a minimal entry from cached data.
func (fs *pxarFS) readEntryForNode(n *node) (*pxar.Entry, error) {
	if n.inode == rootInode {
		return &pxar.Entry{
			Path:     "/",
			Kind:     pxar.KindDirectory,
			Metadata: pxar.Metadata{Stat: format.Stat{Mode: n.mode, UID: n.uid, GID: n.gid}},
		}, nil
	}
	return fs.reader.ReadEntryAt(int64(n.entryStart))
}

func (fs *pxarFS) Readlink(cancel <-chan struct{}, header *fuse.InHeader) ([]byte, fuse.Status) {
	fs.mu.RLock()
	n := fs.nodes[header.NodeId]
	fs.mu.RUnlock()
	if n == nil {
		return nil, fuse.ENOENT
	}
	if !n.isSymlink {
		return nil, fuse.EINVAL
	}

	entry, err := fs.readEntryForNode(n)
	if err != nil {
		return nil, fuse.EIO
	}
	return []byte(entry.LinkTarget), fuse.OK
}

func (fs *pxarFS) ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (uint32, fuse.Status) {
	fs.mu.RLock()
	n := fs.nodes[header.NodeId]
	fs.mu.RUnlock()
	if n == nil {
		return 0, fuse.ENOENT
	}

	entry, err := fs.readEntryForNode(n)
	if err != nil {
		return 0, fuse.EIO
	}

	var sz uint32
	for _, xa := range entry.Metadata.XAttrs {
		sz += uint32(len(xa.Name())) + 1
	}
	if entry.Metadata.FCaps != nil {
		sz += uint32(len("security.capability")) + 1
	}
	if dest == nil {
		return sz, fuse.OK
	}
	if uint32(len(dest)) < sz {
		return 0, fuse.Status(syscall.ERANGE)
	}

	pos := 0
	for _, xa := range entry.Metadata.XAttrs {
		name := xa.Name()
		pos += copy(dest[pos:], name)
		dest[pos] = 0
		pos++
	}
	if entry.Metadata.FCaps != nil {
		name := "security.capability"
		pos += copy(dest[pos:], name)
		dest[pos] = 0
	}
	return sz, fuse.OK
}

func (fs *pxarFS) GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, dest []byte) (uint32, fuse.Status) {
	fs.mu.RLock()
	n := fs.nodes[header.NodeId]
	fs.mu.RUnlock()
	if n == nil {
		return 0, fuse.ENOENT
	}

	entry, err := fs.readEntryForNode(n)
	if err != nil {
		return 0, fuse.EIO
	}

	if attr == "security.capability" && entry.Metadata.FCaps != nil {
		return xattrValue(entry.Metadata.FCaps, dest)
	}
	for _, xa := range entry.Metadata.XAttrs {
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
		}
	}
	fs.mu.Unlock()
}

// --- internal helpers ---

// dirEntryMeta holds minimal metadata needed for directory listing.
type dirEntryMeta struct {
	name  string
	inode uint64
	mode  uint32
	meta  pxar.Entry // temporary, used only for node registration
}

// readDirRaw reads a directory from the archive and returns its entries.
// Results are NOT cached persistently — the kernel caches directory entries
// in VFS, so this is only called on cache miss.
func (fs *pxarFS) readDirRaw(inode uint64) ([]dirEntryMeta, error) {
	fs.mu.RLock()
	n := fs.nodes[inode]
	fs.mu.RUnlock()
	if n == nil {
		return nil, syscall.ENOENT
	}
	if !n.isDir {
		return nil, syscall.ENOTDIR
	}

	var entries []dirEntryMeta
	listErr := fs.reader.ListDirectory(int64(n.contentOffset), accessor.ListOption{}, func(e *pxar.Entry) error {
		childIno := toInode(e)
		entries = append(entries, dirEntryMeta{
			name:  e.FileName(),
			inode: childIno,
			mode:  statMode(e.Metadata.Stat.Mode),
			meta:  *e, // copy for node registration
		})
		return nil
	})
	if listErr != nil {
		return nil, listErr
	}

	return entries, nil
}

// registerNode adds a child node to the cache if not already present.
func (fs *pxarFS) registerNode(inode, parent uint64, e *pxar.Entry) {
	fs.mu.Lock()
	if _, exists := fs.nodes[inode]; !exists {
		fs.nodes[inode] = nodeFromEntry(e, inode, parent)
	}
	fs.mu.Unlock()
}

func (fs *pxarFS) refNode(inode uint64) {
	fs.mu.Lock()
	if n, ok := fs.nodes[inode]; ok {
		n.refs++
	}
	fs.mu.Unlock()
}

// nodeFromEntry creates a minimal node from a pxar.Entry.
func nodeFromEntry(e *pxar.Entry, inode, parent uint64) *node {
	st := e.Metadata.Stat
	return &node{
		inode:         inode,
		parent:        parent,
		refs:          1,
		entryStart:    e.FileOffset,
		contentOffset: e.ContentOffset,
		fileSize:      e.FileSize,
		mode:          st.Mode,
		uid:           st.UID,
		gid:           st.GID,
		mtimeSecs:     st.Mtime.Secs,
		mtimeNanos:    st.Mtime.Nanos,
		isDir:         e.IsDir(),
		isSymlink:     e.IsSymlink(),
		isReg:         e.IsRegularFile(),
	}
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
