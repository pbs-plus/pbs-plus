package pxarmount

import (
	"errors"
	"io"
	"strconv"
	"sync"
	"syscall"
	"unsafe"

	"github.com/hanwen/go-fuse/v2/fuse"
	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/accessor"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"
)

type PxarFS struct {
	fuse.RawFileSystem
	reader   *transfer.SplitReader
	nodes    map[uint64]node
	mu       sync.RWMutex
	readerMu sync.RWMutex
	readerAt io.ReaderAt
}

const maxCachedNodes = 1 << 20

func NewPxarFS(reader *transfer.SplitReader) (*PxarFS, error) {
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

	for i := range entries {
		if entries[i].name == name {
			e := &entries[i]
			nd := fs.registerSlimNode(e, header.NodeId)
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
	if !n.timesResolved {
		fs.ensureNodeTimes(&n)
		if n.timesResolved {
			fs.mu.Lock()
			fs.nodes[input.NodeId] = n
			fs.mu.Unlock()
		}
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

	// un-emitted entries don't leak ref counts.
	fs.mu.Lock()
	for i := start; i < len(entries); i++ {
		eo := out.AddDirLookupEntry(fuse.DirEntry{
			Name: entries[i].name,
			Ino:  entries[i].inode,
			Mode: entries[i].mode,
		})
		if eo == nil {
			break
		}
		e := &entries[i]
		if n, exists := fs.nodes[e.inode]; exists {
			n.refs++
			fs.nodes[e.inode] = n
		} else {
			fs.nodes[e.inode] = node{
				entryStart:    e.entryStart,
				contentOffset: e.contentOffset,
				fileSize:      e.fileSize,
				mode:          uint64(e.mode),
				inode:         e.inode,
				parent:        input.NodeId,
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
		child := fs.nodes[e.inode]
		fillEntryOut(e.inode, &child, eo)
	}
	if len(fs.nodes) > maxCachedNodes {
		fs.evictStaleLocked()
	}
	fs.mu.Unlock()
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

var rootEntry = pxar.Entry{
	Path: "/",
	Kind: pxar.KindDirectory,
}

func (fs *PxarFS) readEntryForNode(n *node) (*pxar.Entry, error) {
	if n.inode == RootInode {
		e := rootEntry
		e.Metadata = pxar.Metadata{Stat: format.Stat{Mode: n.mode, UID: n.uid, GID: n.gid}}
		return &e, nil
	}
	if fs.reader == nil {
		return nil, errors.New("no reader available")
	}
	return fs.reader.ReadEntryAt(int64(n.entryStart))
}

// so that a pxar-mount getattr reports the same atime/mtime restore would set.
//   - default atime and mtime are both pxar Stat.Mtime (Secs+Nanos)
//   - user.lastaccesstime (decimal Unix seconds) overrides atime, dropping nanos
//   - user.lastwritetime  (decimal Unix seconds) overrides mtime, dropping nanos
//
// Returns atimeNs and mtimeNs in Unix nanoseconds. Malformed/absent xattrs
// fall back to Stat.Mtime, exactly like parseXattrUnixSecs on the restore side.
func resolvePxarTimes(entry *pxar.Entry) (atimeNs, mtimeNs int64) {
	mtimeNs = int64(entry.Metadata.Stat.Mtime.Secs)*1_000_000_000 + int64(entry.Metadata.Stat.Mtime.Nanos)
	atimeNs = mtimeNs
	for _, xa := range entry.Metadata.XAttrs {
		name := xa.Name()
		switch string(name) {
		case "user.lastaccesstime":
			if secs, ok := parseXattrUnixSecsLocal(xa.Value()); ok {
				atimeNs = secs * 1_000_000_000
			}
		case "user.lastwritetime":
			if secs, ok := parseXattrUnixSecsLocal(xa.Value()); ok {
				mtimeNs = secs * 1_000_000_000
			}
		}
	}
	return atimeNs, mtimeNs
}

// parseXattrUnixSecsLocal decodes a decimal ASCII Unix-seconds xattr value
// Kept local to avoid coupling the mount to the restore package.
func parseXattrUnixSecsLocal(d []byte) (int64, bool) {
	if len(d) == 0 {
		return 0, false
	}
	v, err := strconv.ParseInt(string(d), 10, 64)
	if err != nil || v < 0 {
		return 0, false
	}
	const unixSecsMax = 32503680000
	if v > unixSecsMax {
		return 0, false
	}
	return v, true
}

// from its pxar entry. It is a no-op once resolved, so the per-file archive
func (fs *PxarFS) ensureNodeTimes(n *node) {
	if n.timesResolved {
		return
	}
	entry, err := fs.readEntryForNode(n)
	if err != nil || entry == nil {
		return
	}
	n.atimeNs, n.mtimeNs = resolvePxarTimes(entry)
	n.timesResolved = true
}

// applying restore's xattr precedence. Used by the mutable overlay so that
// unmodified (pxar-backed) files report the same times as a restore.
func (fs *PxarFS) ResolvedTimes(n *node) (atimeNs, mtimeNs int64) {
	fs.ensureNodeTimes(n)
	return n.atimeNs, n.mtimeNs
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

	if fs.reader == nil {
		return nil, nil
	}

	entries := make([]dirEntrySlim, 0, 64)

	fs.readerMu.RLock()
	listErr := fs.reader.ListDirectory(int64(n.contentOffset), accessor.ListOption{Minimal: true}, func(e *pxar.Entry) error {
		entries = append(entries, dirEntrySlim{
			name:          e.FileName(),
			inode:         ToInode(e),
			mode:          statMode(e.Metadata.Stat.Mode),
			entryStart:    e.FileOffset,
			contentOffset: e.ContentOffset,
			payloadOffset: e.PayloadOffset,
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
	fs.readerMu.RUnlock()
	if listErr != nil {
		return nil, listErr
	}

	return entries, nil
}

func (fs *PxarFS) readFileContent(ino uint64, off, size int64, dest []byte) (fuse.ReadResult, fuse.Status) {
	fs.mu.RLock()
	n, ok := fs.nodes[ino]
	fs.mu.RUnlock()
	if !ok {
		return nil, fuse.ENOENT
	}

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

func (fs *PxarFS) registerSlimNode(e *dirEntrySlim, parent uint64) node {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if n, ok := fs.nodes[e.inode]; ok {
		n.refs++
		fs.nodes[e.inode] = n
		if len(fs.nodes) > maxCachedNodes {
			fs.evictStaleLocked()
		}
		return n
	}
	n := node{
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
	fs.nodes[e.inode] = n
	if len(fs.nodes) > maxCachedNodes {
		fs.evictStaleLocked()
	}
	return n
}

func (fs *PxarFS) preregisterSlimNode(e *dirEntrySlim, parent uint64) node {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if n, ok := fs.nodes[e.inode]; ok {
		if len(fs.nodes) > maxCachedNodes {
			fs.evictStaleLocked()
		}
		return n
	}
	n := node{
		entryStart:    e.entryStart,
		contentOffset: e.contentOffset,
		fileSize:      e.fileSize,
		mode:          uint64(e.mode),
		inode:         e.inode,
		parent:        parent,
		refs:          0,
		mtimeSecs:     e.mtimeSecs,
		uid:           e.uid,
		mtimeNanos:    e.mtimeNanos,
		gid:           e.gid,
		isDir:         e.isDir,
		isSymlink:     e.isSymlink,
		isReg:         e.isReg,
	}
	fs.nodes[e.inode] = n
	if len(fs.nodes) > maxCachedNodes {
		fs.evictStaleLocked()
	}
	return n
}

func (fs *PxarFS) evictStaleLocked() {
	target := maxCachedNodes * 9 / 10
	for ino, n := range fs.nodes {
		if ino != RootInode && n.refs <= 0 {
			delete(fs.nodes, ino)
			if len(fs.nodes) <= target {
				return
			}
		}
	}
}

func (fs *PxarFS) RegisterSlimNode(e *dirEntrySlim, parent uint64) *node {
	n := fs.preregisterSlimNode(e, parent)
	return &n
}

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

func (fs *PxarFS) GetNode(ino uint64) *node {
	fs.mu.RLock()
	n, ok := fs.nodes[ino]
	fs.mu.RUnlock()
	if !ok {
		return nil
	}
	return &n
}

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

func (fs *PxarFS) ReadDirRaw(ino uint64) ([]dirEntrySlim, error) {
	return fs.readDirRaw(ino)
}

func (fs *PxarFS) ReadDirFull(ino uint64, entryCache map[uint64]*pxar.Entry) ([]dirEntrySlim, error) {
	fs.mu.RLock()
	n, ok := fs.nodes[ino]
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

	entries := make([]dirEntrySlim, 0, 64)

	fs.readerMu.RLock()
	listErr := fs.reader.ListDirectory(int64(n.contentOffset), accessor.ListOption{Minimal: false}, func(e *pxar.Entry) error {
		slim := dirEntrySlim{
			name:          e.FileName(),
			inode:         ToInode(e),
			entryStart:    e.FileOffset,
			contentOffset: e.ContentOffset,
			payloadOffset: e.PayloadOffset,
			fileSize:      e.FileSize,
			mode:          statMode(e.Metadata.Stat.Mode),
			uid:           e.Metadata.Stat.UID,
			gid:           e.Metadata.Stat.GID,
			mtimeSecs:     e.Metadata.Stat.Mtime.Secs,
			mtimeNanos:    e.Metadata.Stat.Mtime.Nanos,
			isDir:         e.IsDir(),
			isSymlink:     e.IsSymlink(),
			isReg:         e.IsRegularFile(),
		}
		entries = append(entries, slim)
		if entryCache != nil && slim.isReg {
			entryCache[slim.entryStart] = e
		}
		return nil
	})
	fs.readerMu.RUnlock()
	if listErr != nil {
		return nil, listErr
	}

	return entries, nil
}

func (fs *PxarFS) HotSwap(reader *transfer.SplitReader) {
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

func (fs *PxarFS) Reader() *transfer.SplitReader {
	return fs.reader
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
