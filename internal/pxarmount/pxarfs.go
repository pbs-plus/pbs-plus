package pxarmount

import (
	"io"
	"sync"
	"syscall"
	"unsafe"

	"github.com/hanwen/go-fuse/v2/fuse"
	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/accessor"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"
)

// PxarFS implements fuse.RawFileSystem backed by a lazy-loading SplitArchiveReader.
// This is the immutable layer that provides read-only access to the pxar archive.
type PxarFS struct {
	fuse.RawFileSystem
	reader   *transfer.SplitArchiveReader
	nodes    map[uint64]node
	mu       sync.RWMutex
	readerMu sync.Mutex
	readerAt io.ReaderAt
}

// NewPxarFS creates a pxar-backed FUSE filesystem.
func NewPxarFS(reader *transfer.SplitArchiveReader) (*PxarFS, error) {
	fs := &PxarFS{
		reader: reader,
		nodes:  make(map[uint64]node),
	}
	if reader != nil {
		root, err := reader.ReadRoot()
		if err != nil {
			return nil, err
		}
		fs.readerAt = reader.PayloadReaderAt()
		fs.nodes[RootInode] = newNodeFromEntry(root, RootInode, RootInode)
	} else {
		fs.nodes[RootInode] = node{
			inode:  RootInode,
			parent: RootInode,
			mode:   uint64(syscall.S_IFDIR | 0o755),
			isDir:  true,
			refs:   1,
		}
	}
	return fs, nil
}

func (fs *PxarFS) Init(server *fuse.Server) {
	fs.RawFileSystem = fuse.NewDefaultRawFileSystem()
	fs.RawFileSystem.Init(server)
}

func (fs *PxarFS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
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

func (fs *PxarFS) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	fs.mu.RLock()
	n, ok := fs.nodes[input.NodeId]
	fs.mu.RUnlock()
	if !ok {
		return fuse.ENOENT
	}
	fillAttrOut(&n, out)
	return fuse.OK
}

func (fs *PxarFS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	return fuse.OK
}

func (fs *PxarFS) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	entries, err := fs.readDirRaw(input.NodeId)
	if err != nil {
		return fuse.ToStatus(err)
	}
	defer fs.releaseDirEntries(entries)

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

	if input.Offset <= 1 {
		parentIno, parentMode := fs.getParentInfo(input.NodeId)
		if !out.AddDirEntry(fuse.DirEntry{Name: "..", Ino: parentIno, Mode: parentMode}) {
			return fuse.OK
		}
	}

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

func (fs *PxarFS) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	entries, err := fs.readDirRaw(input.NodeId)
	if err != nil {
		return fuse.ToStatus(err)
	}
	defer fs.releaseDirEntries(entries)

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

	if input.Offset <= 1 {
		parentIno, parentMode := fs.getParentInfo(input.NodeId)
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

func (fs *PxarFS) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	return fuse.OK
}

func (fs *PxarFS) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	fs.mu.RLock()
	n, ok := fs.nodes[input.NodeId]
	fs.mu.RUnlock()
	if !ok {
		return nil, fuse.ENOENT
	}
	if n.isDir {
		return nil, fuse.EISDIR
	}

	return fs.readFileContent(input.NodeId, int64(input.Offset), int64(len(buf)), buf)
}

// rootEntry is reused for the root inode to avoid allocation.
var rootEntry = pxar.Entry{
	Path: "/",
	Kind: pxar.KindDirectory,
}

// readEntryForNode reads the full pxar entry for a cached node.
func (fs *PxarFS) readEntryForNode(n *node) (*pxar.Entry, error) {
	if n.inode == RootInode {
		rootEntry.Metadata = pxar.Metadata{Stat: format.Stat{Mode: n.mode, UID: n.uid, GID: n.gid}}
		return &rootEntry, nil
	}
	return fs.reader.ReadEntryAt(int64(n.entryStart))
}

func (fs *PxarFS) Readlink(cancel <-chan struct{}, header *fuse.InHeader) ([]byte, fuse.Status) {
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

func (fs *PxarFS) ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (uint32, fuse.Status) {
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

func (fs *PxarFS) GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, dest []byte) (uint32, fuse.Status) {
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

func (fs *PxarFS) Release(cancel <-chan struct{}, input *fuse.ReleaseIn) {}

func (fs *PxarFS) Forget(nodeID, nlookup uint64) {
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

func (fs *PxarFS) Access(cancel <-chan struct{}, input *fuse.AccessIn) fuse.Status {
	return fuse.OK
}

// --- Internal Helpers ---

func (fs *PxarFS) readDirRaw(inode uint64) ([]dirEntrySlim, error) {
	fs.mu.RLock()
	n, ok := fs.nodes[inode]
	fs.mu.RUnlock()
	if !ok {
		return nil, syscall.ENOENT
	}
	if !n.isDir {
		return nil, syscall.ENOTDIR
	}

	// Init mode: no backing archive, every dir is empty.
	if fs.reader == nil {
		return nil, nil
	}

	entriesPtr := dirEntryPool.Get().(*[]dirEntrySlim)
	entries := (*entriesPtr)[:0]

	fs.readerMu.Lock()
	listErr := fs.reader.ListDirectory(int64(n.contentOffset), accessor.ListOption{Minimal: true}, func(e *pxar.Entry) error {
		entries = append(entries, dirEntrySlim{
			name:          e.FileName(),
			inode:         ToInode(e),
			mode:          statMode(e.Metadata.Stat.Mode),
			entryStart:    e.FileOffset,
			contentOffset: e.ContentOffset,
			fileSize:      e.FileSize,
			uid:           e.Metadata.Stat.UID,
			gid:           e.Metadata.Stat.GID,
			mtimeSecs:     e.Metadata.Stat.Mtime.Secs,
			mtimeNanos:    e.Metadata.Stat.Mtime.Nanos,
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

// releaseDirEntries returns a dir entry slice to the pool.
func (fs *PxarFS) releaseDirEntries(entries []dirEntrySlim) {
	if entries == nil {
		return
	}
	s := entries[:0]
	dirEntryPool.Put(&s)
}

// readFileContent reads file data from the pxar payload stream.
func (fs *PxarFS) readFileContent(ino uint64, off, size int64, dest []byte) (fuse.ReadResult, fuse.Status) {
	fs.mu.RLock()
	n, ok := fs.nodes[ino]
	fs.mu.RUnlock()
	if !ok {
		return nil, fuse.ENOENT
	}

	// Clamp to file bounds.
	fileSize := int64(n.fileSize)
	if off >= fileSize {
		return fuse.ReadResultData(nil), fuse.OK
	}
	remaining := fileSize - off
	if size > remaining {
		size = remaining
	}
	if int64(len(dest)) < size {
		size = int64(len(dest))
	}

	// Zero-copy read: PayloadOffset + HeaderSize (16) gives the file
	// content start in the ppxar stream. ReadAt avoids the SectionReader
	// allocation and the mpxar entry read (readEntryForNode).
	start := int64(n.contentOffset) + 16 + off
	nr, err := fs.readerAt.ReadAt(dest[:size], start)
	if err != nil && err != io.EOF {
		return nil, fuse.EIO
	}
	if nr == 0 {
		return fuse.ReadResultData(nil), fuse.OK
	}
	return fuse.ReadResultData(dest[:nr]), fuse.OK
}

// registerSlimNode caches a directory entry as a full node.
func (fs *PxarFS) registerSlimNode(e *dirEntrySlim, parent uint64) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if _, ok := fs.nodes[e.inode]; !ok {
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
}

// RegisterSlimNode is a public wrapper for registerSlimNode.
func (fs *PxarFS) RegisterSlimNode(e *dirEntrySlim, parent uint64) {
	fs.registerSlimNode(e, parent)
}

// refNode increments the reference count for an inode.
func (fs *PxarFS) refNode(ino uint64) {
	fs.mu.Lock()
	if n, ok := fs.nodes[ino]; ok {
		n.refs++
		fs.nodes[ino] = n
	}
	fs.mu.Unlock()
}

// getParentInfo returns the parent inode and mode for a given inode.
func (fs *PxarFS) getParentInfo(ino uint64) (parentIno uint64, parentMode uint32) {
	parentIno = RootInode
	parentMode = uint32(syscall.S_IFDIR | 0o555)
	fs.mu.RLock()
	if n, ok := fs.nodes[ino]; ok {
		parentIno = n.parent
		if pn, pok := fs.nodes[parentIno]; pok {
			parentMode = statMode(pn.mode)
		}
	}
	fs.mu.RUnlock()
	return
}

// GetNode returns the cached node for an inode, or nil.
func (fs *PxarFS) GetNode(ino uint64) *node {
	fs.mu.RLock()
	n, ok := fs.nodes[ino]
	fs.mu.RUnlock()
	if !ok {
		return nil
	}
	return &n
}

// GetPxarEntry reads and returns the full pxar.Entry for an inode.
func (fs *PxarFS) GetPxarEntry(ino uint64) (*pxar.Entry, error) {
	fs.mu.RLock()
	n, ok := fs.nodes[ino]
	fs.mu.RUnlock()
	if !ok {
		return nil, syscall.ENOENT
	}
	fs.readerMu.Lock()
	defer fs.readerMu.Unlock()
	return fs.readEntryForNode(&n)
}

// ReadDirRaw is the public version of readDirRaw.
func (fs *PxarFS) ReadDirRaw(ino uint64) ([]dirEntrySlim, error) {
	return fs.readDirRaw(ino)
}

// HotSwap replaces the underlying archive reader.
func (fs *PxarFS) HotSwap(reader *transfer.SplitArchiveReader) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fs.reader = reader
	fs.nodes = make(map[uint64]node)

	if reader != nil {
		root, err := reader.ReadRoot()
		if err == nil {
			fs.readerAt = reader.PayloadReaderAt()
			fs.nodes[RootInode] = newNodeFromEntry(root, RootInode, RootInode)
		}
	}
}

// Reader returns the underlying SplitArchiveReader.
func (fs *PxarFS) Reader() *transfer.SplitArchiveReader {
	return fs.reader
}

// bytesEq compares a byte slice to a string without allocation.
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
