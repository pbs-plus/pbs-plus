package pxarmount

import (
	"fmt"
	"io"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/accessor"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"
)

// PxarFS implements fuse.RawFileSystem backed by a lazy-loading SplitArchiveReader.
type PxarFS struct {
	fuse.RawFileSystem
	reader   *transfer.SplitArchiveReader
	nodes    map[uint64]node
	size     int64
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
		fs.size = int64(root.FileOffset + root.FileSize)
		fs.nodes[RootInode] = newNodeFromEntry(root, RootInode, RootInode)
	} else {
		// Empty filesystem — synthetic root directory only.
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

// Size returns the total archive size.
func (fs *PxarFS) Size() int64 { return fs.size }

func (fs *PxarFS) Init(server *fuse.Server) {
	fs.RawFileSystem = fuse.NewDefaultRawFileSystem()
	fs.RawFileSystem.Init(server)
}

func (fs *PxarFS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	entries, err := fs.ReadDirRaw(header.NodeId)
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
	entries, err := fs.ReadDirRaw(input.NodeId)
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
	entries, err := fs.ReadDirRaw(input.NodeId)
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

	contentOff := n.contentOffset
	contentSz := n.fileSize
	isReg := n.isReg

	fs.mu.RLock()
	ra := fs.readerAt
	fs.mu.RUnlock()

	// Fast path: payload ReaderAt — fully concurrent, no lock needed.
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
	fs.readerMu.Lock()
	entry, err := fs.ReadEntryForNode(&n)
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

	fs.readerMu.Lock()
	entry, err := fs.ReadEntryForNode(&n)
	fs.readerMu.Unlock()
	if err != nil {
		return nil, fuse.EIO
	}
	return unsafeStringBytes(entry.LinkTarget), fuse.OK
}

func (fs *PxarFS) ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (uint32, fuse.Status) {
	fs.mu.RLock()
	n, ok := fs.nodes[header.NodeId]
	fs.mu.RUnlock()
	if !ok {
		return 0, fuse.ENOENT
	}

	fs.readerMu.Lock()
	entry, err := fs.ReadEntryForNode(&n)
	fs.readerMu.Unlock()
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

	fs.readerMu.Lock()
	entry, err := fs.ReadEntryForNode(&n)
	fs.readerMu.Unlock()
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

func (fs *PxarFS) StatFs(cancel <-chan struct{}, header *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	out.Blocks = uint64(fs.size / 512)
	out.Bsize = 4096
	out.NameLen = 255
	out.Frsize = 512
	return fuse.OK
}

func (fs *PxarFS) Access(cancel <-chan struct{}, input *fuse.AccessIn) fuse.Status { return fuse.OK }

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

// HotSwap replaces the pxar archive reader and clears the node cache.
func (fs *PxarFS) HotSwap(newReader *transfer.SplitArchiveReader) {
	fs.mu.Lock()
	fs.nodes = make(map[uint64]node, len(fs.nodes))
	fs.mu.Unlock()

	fs.readerMu.Lock()
	fs.reader = newReader
	fs.readerMu.Unlock()

	root, err := newReader.ReadRoot()
	if err != nil {
		return
	}
	fs.mu.Lock()
	fs.size = int64(root.FileOffset + root.FileSize)
	fs.nodes[RootInode] = newNodeFromEntry(root, RootInode, RootInode)
	fs.readerAt = newReader.PayloadReaderAt()
	fs.mu.Unlock()
}

// ReadEntryForNode reads the full pxar entry for a node. Caller must hold readerMu.
func (fs *PxarFS) ReadEntryForNode(n *node) (*pxar.Entry, error) {
	if n.inode == RootInode {
		e := &pxar.Entry{
			Path: "/",
			Kind: pxar.KindDirectory,
			Metadata: pxar.Metadata{Stat: format.Stat{
				Mode: n.mode, UID: n.uid, GID: n.gid,
			}},
		}
		return e, nil
	}
	return fs.reader.ReadEntryAt(int64(n.entryStart))
}

// ReadDirRaw reads a directory from the archive and returns its entries.
func (fs *PxarFS) ReadDirRaw(inode uint64) ([]dirEntrySlim, error) {
	fs.mu.RLock()
	n, ok := fs.nodes[inode]
	fs.mu.RUnlock()
	if !ok {
		return nil, syscall.ENOENT
	}
	if !n.isDir {
		return nil, syscall.ENOTDIR
	}
	if fs.reader == nil {
		return nil, nil
	}

	entriesPtr := dirEntryPool.Get().(*[]dirEntrySlim)
	entries := (*entriesPtr)[:0]

	fs.readerMu.Lock()
	listErr := fs.reader.ListDirectory(int64(n.contentOffset), accessor.ListOption{Minimal: true}, func(e *pxar.Entry) error {
		childIno := ToInode(e)
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

// RegisterSlimNode adds a child node from a dirEntrySlim if not present.
func (fs *PxarFS) RegisterSlimNode(e *dirEntrySlim, parent uint64) {
	fs.registerSlimNode(e, parent)
}

// RegisterNode adds a child node from a full pxar.Entry if not present.
func (fs *PxarFS) RegisterNode(inode, parent uint64, e *pxar.Entry) {
	fs.mu.Lock()
	if _, exists := fs.nodes[inode]; !exists {
		fs.nodes[inode] = newNodeFromEntry(e, inode, parent)
	}
	fs.mu.Unlock()
}

func (fs *PxarFS) refNode(inode uint64) {
	fs.mu.Lock()
	if n, ok := fs.nodes[inode]; ok {
		n.refs++
		fs.nodes[inode] = n
	}
	fs.mu.Unlock()
}

func (fs *PxarFS) getParentInfo(nodeID uint64) (uint64, uint32) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	n, ok := fs.nodes[nodeID]
	if !ok {
		return RootInode, uint32(syscall.S_IFDIR | 0o555)
	}
	parentIno := n.parent
	if pn, pok := fs.nodes[parentIno]; pok {
		return parentIno, statMode(pn.mode)
	}
	return parentIno, uint32(syscall.S_IFDIR | 0o555)
}

func (fs *PxarFS) releaseDirEntries(entries []dirEntrySlim) {
	if entries == nil {
		return
	}
	s := entries[:0]
	dirEntryPool.Put(&s)
}

func (fs *PxarFS) registerSlimNode(e *dirEntrySlim, parent uint64) {
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

func (fs *PxarFS) ReadRoot() (*pxar.Entry, error) {
	if fs.reader == nil {
		return &pxar.Entry{Path: "/", Kind: pxar.KindDirectory}, nil
	}
	return fs.reader.ReadRoot()
}

func (fs *PxarFS) ReadEntryAt(offset int64) (*pxar.Entry, error) {
	if fs.reader == nil {
		return nil, fmt.Errorf("no archive reader")
	}
	return fs.reader.ReadEntryAt(offset)
}

func (fs *PxarFS) ReadFileContentReader(entry *pxar.Entry) (io.ReadCloser, error) {
	if fs.reader == nil {
		return nil, fmt.Errorf("no archive reader")
	}
	return fs.reader.ReadFileContentReader(entry)
}

func (fs *PxarFS) Close() error {
	if fs.reader == nil {
		return nil
	}
	return fs.reader.Close()
}
