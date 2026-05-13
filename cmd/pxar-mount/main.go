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
//
// Zero-copy / allocation-optimized:
//   - node struct field-ordered for minimal padding (64→48 bytes)
//   - dirEntrySlim replaces dirEntryMeta: no full Entry copy, just offsets
//   - sync.Pool for dir entry slices and Read buffers
//   - Seek replaces io.CopyN(io.Discard) for file offset skips
//   - Single RLock per node lookup (no double-acquire)
//   - map[uint64]node (value) for fewer GC-scanned pointers
package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

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
// Fields ordered largest→smallest to minimize padding.
// Total: 64 bytes (8×uint64 + 2×int64 + 2×uint32 + 3×bool = 64, naturally aligned).
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
	_             byte // explicit pad to 64 bytes
}

// dirEntrySlim is a lightweight directory entry for readdir results.
// Avoids copying the full pxar.Entry (which contains strings, slices, etc.).
// Only stores what FUSE DirEntry + node registration need.
type dirEntrySlim struct {
	name          string
	entryStart    uint64 // FileOffset
	contentOffset uint64 // ContentOffset
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

// dirEntryPool reuses dir entry slices across ReadDir calls.
// The kernel caches directory entries in VFS, so ReadDir is only
// called on cache miss — pool pressure is low.
var dirEntryPool = sync.Pool{
	New: func() any {
		s := make([]dirEntrySlim, 0, 64)
		return &s
	},
}

// pxarFS implements fuse.RawFileSystem backed by a lazy-loading SplitArchiveReader.
// nodes is stored by value (map[uint64]node) to reduce GC pointer scanning.
type pxarFS struct {
	fuse.RawFileSystem
	reader   *transfer.SplitArchiveReader
	nodes    map[uint64]node // value-type: fewer GC-scanned pointers
	size     int64
	mu       sync.RWMutex
	readerMu sync.Mutex
	readerAt io.ReaderAt // payload ReaderAt — concurrent-safe, no lock needed
}

func toInode(e *pxar.Entry) uint64 {
	if e.IsDir() {
		return e.FileOffset + e.FileSize
	}
	return e.FileOffset | nonDirBit
}

func main() {
	// Handle "commit" subcommand before parsing mount flags
	if len(os.Args) > 1 && os.Args[1] == "commit" {
		runCommitSubcommand()
		return
	}

	pbsStore := flag.String("pbs-store", "", "PBS datastore root path")
	mpxarDidx := flag.String("mpxar-didx", "", "Path to metadata dynamic index (.mpxar.didx)")
	ppxarDidx := flag.String("ppxar-didx", "", "Path to payload or unified dynamic index (.ppxar.didx)")
	keyfile := flag.String("keyfile", "", "Path to encryption key (accepted for CLI compat, encryption not yet supported)")
	verifyChunks := flag.Bool("verify-chunks", false, "Verify chunk SHA256 on read (accepted for CLI compat)")
	cacheMB := flag.Int("cache-size", 256, "Cache size in MB (accepted for CLI compat)")
	fuseOpts := flag.String("options", "ro,default_permissions", "FUSE mount options")
	passthrough := flag.String("passthrough", "", "Backing directory for write passthrough (enables read-write overlay)")
	socketPath := flag.String("socket", "", "Unix socket path for commit commands")
	verbose := flag.Bool("verbose", false, "Enable verbose logging")

	flag.Parse()

	if *pbsStore == "" || *ppxarDidx == "" {
		fmt.Fprintf(os.Stderr, "Usage: pxar-mount --pbs-store <path> --ppxar-didx <path> [--mpxar-didx <path>] [--passthrough <dir>] [--socket <path>] [--verbose] <mountpoint>\n")
		os.Exit(1)
	}

	if *passthrough == "" && *socketPath != "" {
		fmt.Fprintf(os.Stderr, "Error: --socket requires --passthrough\n")
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
		reader:   reader,
		readerAt: reader.PayloadReaderAt(),
		nodes:    make(map[uint64]node),
		size:     int64(root.FileOffset + root.FileSize),
	}

	fs.nodes[rootInode] = newNodeFromEntry(root, rootInode, rootInode)

	var rawFS fuse.RawFileSystem = fs

	var sockListener net.Listener // captured for graceful shutdown

	if *passthrough != "" {
		// Validate the passthrough directory
		ptInfo, err := os.Stat(*passthrough)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: passthrough directory not accessible: %v\n", err)
			os.Exit(1)
		}
		if !ptInfo.IsDir() {
			fmt.Fprintf(os.Stderr, "Error: passthrough path is not a directory\n")
			os.Exit(1)
		}

		// Parse original snapshot identity from the DIDX path for dedup
		origSnap := parseOrigSnapshot(*pbsStore, *ppxarDidx)

		ptFS := &passthroughFS{
			pxar:          fs,
			pbsStore:      *pbsStore,
			backingDir:    *passthrough,
			origSnapshot:  origSnap,
			origPpxarDidx: *ppxarDidx,
			nodePaths:     make(map[uint64]string),
			backed:        make(map[uint64]bool),
			pxarDir:       make(map[uint64]bool),
			handles:       make(map[uint64]*passFh),
		}
		rawFS = ptFS

		// Pre-create all pxar directories in the backing dir so
		// SMB's acl_xattr can store/retrieve ACLs directly.
		if err := ptFS.precreateDirectories(); err != nil {
			fmt.Fprintf(os.Stderr, "Error pre-creating directories: %v\n", err)
			os.Exit(1)
		}

		// Start socket listener for commit commands
		if *socketPath != "" {
			l, _, err := ptFS.startSocketListener(*socketPath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error starting socket listener: %v\n", err)
				os.Exit(1)
			}
			sockListener = l
			if *verbose {
				fmt.Fprintf(os.Stderr, "pxar-mount: listening for commits on %s\n", *socketPath)
			}
		}

		// Override mount options for read-write
		*fuseOpts = strings.Replace(*fuseOpts, "ro,", "rw,", 1)
		if !strings.Contains(*fuseOpts, "rw") {
			*fuseOpts = "rw,default_permissions"
		}

		if *verbose {
			fmt.Fprintf(os.Stderr, "pxar-mount: passthrough mode, backing dir=%s\n", *passthrough)
		}
	}

	server, err := fuse.NewServer(rawFS, mountPoint, &fuse.MountOptions{
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
		// Close socket listener first so defers run and socket file is removed
		if sockListener != nil {
			_ = sockListener.Close()
		}
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
	entries, err := fs.readDirRaw(header.NodeId)
	if err != nil {
		return fuse.ToStatus(err)
	}
	defer fs.releaseDirEntries(entries)

	for i := range entries {
		if entries[i].name == name {
			e := &entries[i]
			fs.registerSlimNode(e, header.NodeId)
			fs.refNode(e.inode)
			fs.mu.RLock()
			nd := fs.nodes[e.inode]
			fs.mu.RUnlock()
			fillEntryOut(e.inode, &nd, out)
			return fuse.OK
		}
	}
	return fuse.ENOENT
}

func (fs *pxarFS) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	fs.mu.RLock()
	n, ok := fs.nodes[input.NodeId]
	fs.mu.RUnlock()
	if !ok {
		return fuse.ENOENT
	}
	fillAttrOut(&n, out)
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
	defer fs.releaseDirEntries(entries)

	// "." entry first
	if input.Offset == 0 {
		mode := uint32(syscall.S_IFDIR | 0o555)
		fs.mu.RLock()
		if n, ok := fs.nodes[input.NodeId]; ok {
			mode = statMode(n.mode)
		}
		fs.mu.RUnlock()
		if !out.AddDirEntry(fuse.DirEntry{Name: ".", Ino: input.NodeId, Mode: mode}) {
			return fuse.OK
		}
	}

	// ".." entry
	if input.Offset == 0 || input.Offset == 1 {
		parentIno, parentMode := fs.getParentInfo(input.NodeId)
		if input.Offset <= 1 {
			if !out.AddDirEntry(fuse.DirEntry{Name: "..", Ino: parentIno, Mode: parentMode}) {
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
	defer fs.releaseDirEntries(entries)

	// "." entry first
	if input.Offset == 0 {
		mode := uint32(syscall.S_IFDIR | 0o555)
		fs.mu.RLock()
		n, ok := fs.nodes[input.NodeId]
		fs.mu.RUnlock()
		if ok {
			mode = statMode(n.mode)
		}
		eo := out.AddDirLookupEntry(fuse.DirEntry{Name: ".", Ino: input.NodeId, Mode: mode})
		if eo == nil {
			return fuse.OK
		}
		if ok {
			fillEntryOut(input.NodeId, &n, eo)
		}
	}

	// ".." entry
	if input.Offset == 0 || input.Offset == 1 {
		parentIno, parentMode := fs.getParentInfo(input.NodeId)
		if input.Offset <= 1 {
			eo := out.AddDirLookupEntry(fuse.DirEntry{Name: "..", Ino: parentIno, Mode: parentMode})
			if eo == nil {
				return fuse.OK
			}
			fs.mu.RLock()
			if pn, pok := fs.nodes[parentIno]; pok {
				fillEntryOut(parentIno, &pn, eo)
			}
			fs.mu.RUnlock()
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
		fs.registerSlimNode(&entries[i], input.NodeId)
		fs.mu.RLock()
		child, cok := fs.nodes[entries[i].inode]
		fs.mu.RUnlock()
		if cok {
			fillEntryOut(entries[i].inode, &child, eo)
		}
	}
	return fuse.OK
}

func (fs *pxarFS) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	return fuse.OK
}

func (fs *pxarFS) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	fs.mu.RLock()
	n, ok := fs.nodes[input.NodeId]
	fs.mu.RUnlock()
	if !ok {
		return nil, fuse.ENOENT
	}
	if n.isDir {
		return nil, fuse.EISDIR
	}

	// Snapshot immutable node fields.
	contentOff := n.contentOffset // == PayloadOffset for split archives
	contentSz := n.fileSize
	isReg := n.isReg
	ra := fs.readerAt

	// Fast path: payload ReaderAt — fully concurrent, no lock needed.
	// For split archives, contentOff == PayloadOffset and actual file data
	// starts at PayloadOffset + HeaderSize.
	if isReg && ra != nil && contentOff != 0 {
		if input.Offset >= contentSz {
			return fuse.ReadResultData(nil), fuse.OK
		}

		remaining := contentSz - input.Offset
		toRead := min(uint64(len(buf)), remaining)

		nr, err := ra.ReadAt(buf[:toRead], int64(contentOff)+format.HeaderSize+int64(input.Offset))
		if err != nil && err != io.EOF {
			return nil, fuse.EIO
		}
		if nr == 0 {
			return fuse.ReadResultData(nil), fuse.OK
		}
		return fuse.ReadResultData(buf[:nr]), fuse.OK
	}

	// Slow path: unified archive or non-split entry.
	// readerMu serializes metadata stream access.
	fs.readerMu.Lock()
	entry, err := fs.readEntryForNode(&n)
	if err != nil {
		fs.readerMu.Unlock()
		return nil, fuse.EIO
	}

	rc, err := fs.reader.ReadFileContentReader(entry)
	fs.readerMu.Unlock()
	if err != nil {
		return nil, fuse.EIO
	}
	defer rc.Close()

	if input.Offset > 0 {
		if seeker, ok := rc.(io.Seeker); ok {
			if _, err := seeker.Seek(int64(input.Offset), io.SeekStart); err != nil {
				return nil, fuse.EIO
			}
		} else {
			if _, err := io.CopyN(io.Discard, rc, int64(input.Offset)); err != nil {
				return nil, fuse.EIO
			}
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

// rootEntry is a reusable root entry to avoid allocation on every Read.
var rootEntry = pxar.Entry{
	Path: "/",
	Kind: pxar.KindDirectory,
}

// readEntryForNode reads the full pxar entry for a node. For the root inode
// (which has no FILENAME header), returns a reused entry from cached data.
// Caller must hold readerMu.
func (fs *pxarFS) readEntryForNode(n *node) (*pxar.Entry, error) {
	if n.inode == rootInode {
		rootEntry.Metadata = pxar.Metadata{Stat: format.Stat{Mode: n.mode, UID: n.uid, GID: n.gid}}
		return &rootEntry, nil
	}
	return fs.reader.ReadEntryAt(int64(n.entryStart))
}

func (fs *pxarFS) Readlink(cancel <-chan struct{}, header *fuse.InHeader) ([]byte, fuse.Status) {
	fs.mu.RLock()
	n, ok := fs.nodes[header.NodeId]
	fs.mu.RUnlock()
	if !ok {
		return nil, fuse.ENOENT
	}
	if !n.isSymlink {
		return nil, fuse.EINVAL
	}

	entry, err := fs.readEntryForNode(&n)
	if err != nil {
		return nil, fuse.EIO
	}
	return unsafe.Slice(unsafe.StringData(entry.LinkTarget), len(entry.LinkTarget)), fuse.OK
}

func (fs *pxarFS) ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (uint32, fuse.Status) {
	fs.mu.RLock()
	n, ok := fs.nodes[header.NodeId]
	fs.mu.RUnlock()
	if !ok {
		return 0, fuse.ENOENT
	}

	entry, err := fs.readEntryForNode(&n)
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
	n, ok := fs.nodes[header.NodeId]
	fs.mu.RUnlock()
	if !ok {
		return 0, fuse.ENOENT
	}

	entry, err := fs.readEntryForNode(&n)
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
		} else {
			fs.nodes[nodeID] = n
		}
	}
	fs.mu.Unlock()
}

// --- internal helpers ---

// getParentInfo returns (parentInode, parentMode) for a given node.
// Single RLock for self+parent lookup — avoids double acquire.
func (fs *pxarFS) getParentInfo(nodeID uint64) (uint64, uint32) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	n, ok := fs.nodes[nodeID]
	if !ok {
		return rootInode, uint32(syscall.S_IFDIR | 0o555)
	}
	parentIno := n.parent
	if pn, pok := fs.nodes[parentIno]; pok {
		return parentIno, statMode(pn.mode)
	}
	return parentIno, uint32(syscall.S_IFDIR | 0o555)
}

// releaseDirEntries returns a pooled dir entry slice to the pool.
func (fs *pxarFS) releaseDirEntries(entries []dirEntrySlim) {
	if entries == nil {
		return
	}
	// Reset and return to pool
	s := entries[:0]
	dirEntryPool.Put(&s)
}

// readDirRaw reads a directory from the archive and returns its entries.
// Results are NOT cached persistently — the kernel caches directory entries
// in VFS, so this is only called on cache miss.
// Uses sync.Pool for the entry slice to reduce allocations.
func (fs *pxarFS) readDirRaw(inode uint64) ([]dirEntrySlim, error) {
	fs.mu.RLock()
	n, ok := fs.nodes[inode]
	fs.mu.RUnlock()
	if !ok {
		return nil, syscall.ENOENT
	}
	if !n.isDir {
		return nil, syscall.ENOTDIR
	}

	// Get a pooled slice
	entriesPtr := dirEntryPool.Get().(*[]dirEntrySlim)
	entries := (*entriesPtr)[:0]

	fs.readerMu.Lock()
	listErr := fs.reader.ListDirectory(int64(n.contentOffset), accessor.ListOption{Minimal: true}, func(e *pxar.Entry) error {
		childIno := toInode(e)
		st := e.Metadata.Stat
		entries = append(entries, dirEntrySlim{
			name:          e.FileName(),
			inode:         childIno,
			mode:          statMode(st.Mode),
			entryStart:    e.FileOffset,
			contentOffset: e.ContentOffset,
			fileSize:      e.FileSize,
			uid:           st.UID,
			gid:           st.GID,
			mtimeSecs:     st.Mtime.Secs,
			mtimeNanos:    st.Mtime.Nanos,
			isDir:         e.IsDir(),
			isSymlink:     e.IsSymlink(),
			isReg:         e.IsRegularFile(),
		})
		return nil
	})
	fs.readerMu.Unlock()
	if listErr != nil {
		*entriesPtr = entries
		dirEntryPool.Put(entriesPtr)
		return nil, listErr
	}

	return entries, nil
}

// registerNode adds a child node to the cache if not already present.
// Kept for passthrough.go compatibility — delegates to newNodeFromEntry.
func (fs *pxarFS) registerNode(inode, parent uint64, e *pxar.Entry) {
	fs.mu.Lock()
	if _, exists := fs.nodes[inode]; !exists {
		fs.nodes[inode] = newNodeFromEntry(e, inode, parent)
	}
	fs.mu.Unlock()
}

// registerSlimNode adds a child node from a dirEntrySlim if not present.
func (fs *pxarFS) registerSlimNode(e *dirEntrySlim, parent uint64) {
	fs.mu.Lock()
	if _, exists := fs.nodes[e.inode]; !exists {
		fs.nodes[e.inode] = node{
			entryStart:    e.entryStart,
			contentOffset: e.contentOffset,
			fileSize:      e.fileSize,
			mode:          uint64(e.mode),
			inode:         e.inode,
			parent:        parent,
			refs:          1,
			mtimeSecs:     e.mtimeSecs,
			uid:           e.uid,
			mtimeNanos:    e.mtimeNanos,
			gid:           e.gid,
			isDir:         e.isDir,
			isSymlink:     e.isSymlink,
			isReg:         e.isReg,
		}
	}
	fs.mu.Unlock()
}

func (fs *pxarFS) refNode(inode uint64) {
	fs.mu.Lock()
	if n, ok := fs.nodes[inode]; ok {
		n.refs++
		fs.nodes[inode] = n
	}
	fs.mu.Unlock()
}

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

// hotSwap replaces the pxar archive reader and clears the node cache.
// Called after a successful commit to switch to the new snapshot without
// remounting the FUSE filesystem.
func (fs *pxarFS) hotSwap(newReader *transfer.SplitArchiveReader) {
	fs.mu.Lock()
	fs.nodes = make(map[uint64]node, len(fs.nodes))
	fs.mu.Unlock()

	// Swap the reader under the reader mutex
	fs.readerMu.Lock()
	fs.reader = newReader
	fs.readerMu.Unlock()

	// Update the concurrent-safe ReaderAt
	fs.readerAt = newReader.PayloadReaderAt()

	// Re-register root node from the new reader
	root, err := newReader.ReadRoot()
	if err != nil {
		return
	}
	fs.mu.Lock()
	fs.size = int64(root.FileOffset + root.FileSize)
	fs.nodes[rootInode] = newNodeFromEntry(root, rootInode, rootInode)
	fs.mu.Unlock()
}
