package pxarmount

// passthroughFS implements fuse.RawFileSystem as an overlay:
//   - PxarFS provides the read-only lower layer
//   - backingDir provides the read-write upper layer
//
// When mutationMode is enabled, pxar-backed entries can be mutated.
// Mutations are recorded in txnLog and applied during commit.

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
)

type ACLConfig struct {
	OwnerUID   int  // default owner UID for new files/dirs (0 = inherit)
	OwnerGID   int  // default group GID for new files/dirs (0 = inherit)
	ForceOwner bool // force chown on mount and after commit
	ForceGroup bool // force chgrp on mount and after commit
}

// metaOverride stores pending metadata changes for a non-materialized pxar entry.
// These are applied in-memory until the file is materialized (for content writes)
// or committed. This avoids copying entire file contents just for chmod/chown/xattr.
type metaOverride struct {
	mode  *uint32
	uid   *uint32
	gid   *uint32
	mtime *int64
	atime *int64
	size  *uint64
	xadd  map[string][]byte // xattrs to add/replace
	xdel  map[string]bool   // xattrs to remove
}

type PassthroughFS struct {
	fuse.RawFileSystem
	nodePaths     map[uint64]string
	pathToIno     map[string]uint64
	pxar          *PxarFS
	handles       map[uint64]*passFh
	backed        map[uint64]bool
	pxarDir       map[uint64]bool
	deletedPaths  map[string]bool
	origSnapshot  snapshotRef
	pbsStore      string
	origPpxarDidx string
	backingDir    string
	nextBackIno   uint64
	nextFh        uint64
	mu            sync.RWMutex
	fhmu          sync.Mutex
	matMu         sync.Mutex
	materialize   map[uint64]*sync.Mutex
	mmapData      [][]byte
	mutationMode  bool
	txnLog        *TransactionLog
	acl           ACLConfig
	metaOverlay   map[string]*metaOverride // pending metadata overrides for non-backed pxar entries
}

// NewPassthroughFS creates a new overlay filesystem.
func NewPassthroughFS(pxar *PxarFS, backingDir, pbsStore, ppxarDidx string, mutationMode bool, txnLog *TransactionLog) *PassthroughFS {
	return &PassthroughFS{
		pxar:          pxar,
		backingDir:    backingDir,
		pbsStore:      pbsStore,
		origPpxarDidx: ppxarDidx,
		mutationMode:  mutationMode,
		txnLog:        txnLog,
		nodePaths:     make(map[uint64]string),
		pathToIno:     make(map[string]uint64),
		backed:        make(map[uint64]bool),
		pxarDir:       make(map[uint64]bool),
		deletedPaths:  make(map[string]bool),
		handles:       make(map[uint64]*passFh),
		metaOverlay:   make(map[string]*metaOverride),
	}
}

// SetACLConfig applies the ACL policy. Must be called before InitPassthroughRoot.
func (fs *PassthroughFS) SetACLConfig(cfg ACLConfig) {
	fs.acl = cfg
}

// InitPassthroughRoot initializes the root directory in the backing dir.
func (fs *PassthroughFS) InitPassthroughRoot() error {
	if err := os.MkdirAll(fs.backingDir, 0o755); err != nil {
		return err
	}
	fs.setNode(RootInode, "/", true)
	fs.mu.Lock()
	fs.pxarDir[RootInode] = true
	fs.mu.Unlock()
	return nil
}

func (fs *PassthroughFS) Init(server *fuse.Server) {
	fs.RawFileSystem = fuse.NewDefaultRawFileSystem()
	fs.RawFileSystem.Init(server)
}
func (fs *PassthroughFS) String() string    { return "pxar-passthrough" }
func (fs *PassthroughFS) SetDebug(dbg bool) {}
func (fs *PassthroughFS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	return fuse.OK
}
func (fs *PassthroughFS) ReleaseDir(input *fuse.ReleaseIn) {}
func (fs *PassthroughFS) FsyncDir(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	return fuse.OK
}
func (fs *PassthroughFS) Link(cancel <-chan struct{}, input *fuse.LinkIn, name string, out *fuse.EntryOut) fuse.Status {
	return fuse.ENOSYS
}

// --- inode management ---

func (fs *PassthroughFS) absPath(rel string) string {
	return filepath.Join(fs.backingDir, rel)
}

func (fs *PassthroughFS) isBacked(ino uint64) bool {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.backed[ino]
}

func (fs *PassthroughFS) nodePath(ino uint64) string {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.nodePaths[ino]
}

func (fs *PassthroughFS) setNode(ino uint64, relPath string, backed bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if oldIno, exists := fs.pathToIno[relPath]; exists && oldIno != ino {
		delete(fs.nodePaths, oldIno)
		delete(fs.backed, oldIno)
		delete(fs.pxarDir, oldIno)
	}
	fs.nodePaths[ino] = relPath
	fs.pathToIno[relPath] = ino
	fs.backed[ino] = backed
}

func (fs *PassthroughFS) allocBackedIno(isDir bool) uint64 {
	fs.mu.Lock()
	ino := fs.nextBackIno + backedInoBase
	fs.nextBackIno++
	fs.mu.Unlock()
	if !isDir {
		ino |= NonDirBit
	}
	return ino
}

func (fs *PassthroughFS) lookupOrAllocIno(relPath string, isDir bool) (uint64, bool) {
	fs.mu.Lock()
	if existing, ok := fs.pathToIno[relPath]; ok {
		fs.mu.Unlock()
		return existing, false
	}
	ino := fs.nextBackIno + backedInoBase
	fs.nextBackIno++
	if !isDir {
		ino |= NonDirBit
	}
	fs.pathToIno[relPath] = ino
	fs.nodePaths[ino] = relPath
	fs.mu.Unlock()
	return ino, true
}

// cleanInodeMappings removes all inode state for a path.
func (fs *PassthroughFS) cleanInodeMappings(childPath string) {
	fs.mu.Lock()
	if ino, ok := fs.pathToIno[childPath]; ok {
		delete(fs.nodePaths, ino)
		delete(fs.backed, ino)
		delete(fs.pxarDir, ino)
		delete(fs.pathToIno, childPath)
	}
	fs.mu.Unlock()
}

func (fs *PassthroughFS) ensureBackingParent(rel string) error {
	parent := filepath.Dir(rel)
	if parent == "." || parent == "/" {
		return nil
	}
	return os.MkdirAll(fs.absPath(parent), 0o755)
}

func (fs *PassthroughFS) ensureDirBacking(ino uint64, relPath string) {
	absPath := fs.absPath(relPath)
	if err := os.MkdirAll(absPath, 0o755); err != nil {
		return
	}
	fs.setNode(ino, relPath, true)
	fs.mu.Lock()
	fs.pxarDir[ino] = true
	fs.mu.Unlock()
	fs.applyACLOwnership(absPath, true)

	// Restore pxar timestamps on the directory.
	if n := fs.getPxarNode(ino); n != nil {
		tv := []unix.Timeval{
			{Sec: n.mtimeSecs, Usec: int64(n.mtimeNanos) / 1000},
			{Sec: n.mtimeSecs, Usec: int64(n.mtimeNanos) / 1000},
		}
		_ = unix.Lutimes(absPath, tv)
	}
}

func (fs *PassthroughFS) statBacked(rel string) (*node, error) {
	var st syscall.Stat_t
	if err := syscall.Lstat(fs.absPath(rel), &st); err != nil {
		return nil, err
	}
	mode := uint64(st.Mode)
	ino, _ := fs.lookupOrAllocIno(rel, mode&syscall.S_IFDIR != 0)
	nd := &node{
		inode:     ino,
		parent:    RootInode,
		refs:      1,
		fileSize:  uint64(st.Size),
		mode:      mode,
		uid:       st.Uid,
		gid:       st.Gid,
		mtimeSecs: int64(st.Mtim.Sec),
		isDir:     mode&syscall.S_IFDIR != 0,
		isSymlink: mode&syscall.S_IFLNK != 0,
		isReg:     mode&syscall.S_IFREG != 0,
	}
	return nd, nil
}

func (fs *PassthroughFS) handleForNode(nodeID, fhID uint64) *passFh {
	fs.fhmu.Lock()
	defer fs.fhmu.Unlock()
	fh, ok := fs.handles[fhID]
	if !ok || fh.inode != nodeID {
		return nil
	}
	return fh
}

func (fs *PassthroughFS) registerFh(rel string, nodeID uint64, flags int) (uint64, error) {
	abs := fs.absPath(rel)
	fd, err := syscall.Open(abs, flags, 0)
	if err != nil {
		return 0, err
	}
	fs.fhmu.Lock()
	defer fs.fhmu.Unlock()
	id := fs.nextFh
	fs.nextFh++
	fs.handles[id] = &passFh{fd: fd, inode: nodeID}
	return id, nil
}

func (fs *PassthroughFS) getPxarNode(ino uint64) *node {
	fs.pxar.mu.RLock()
	defer fs.pxar.mu.RUnlock()
	n, ok := fs.pxar.nodes[ino]
	if !ok {
		return nil
	}
	return &n
}

func (fs *PassthroughFS) isPxarBacked(ino uint64) bool {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.pxarDir[ino]
}

func (fs *PassthroughFS) isPxarChild(parentIno uint64, name string) bool {
	entries, err := fs.pxar.ReadDirRaw(parentIno)
	if err != nil {
		parentPath := fs.nodePath(parentIno)
		if parentPath != "" {
			if recovered := fs.recoverPxarDirNode(parentPath); recovered != 0 {
				fs.setNode(recovered, parentPath, true)
				fs.mu.Lock()
				fs.pxarDir[recovered] = true
				fs.mu.Unlock()
				if entries2, err2 := fs.pxar.ReadDirRaw(recovered); err2 == nil {
					entries = entries2
					err = nil
				}
			}
		}
		if err != nil {
			return false
		}
	}
	for _, e := range entries {
		if e.name == name {
			return true
		}
	}
	return false
}

func (fs *PassthroughFS) isPathDeleted(relPath string) bool {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.deletedPaths[relPath]
}

func (fs *PassthroughFS) markPathDeleted(relPath string) {
	fs.mu.Lock()
	if fs.deletedPaths[relPath] {
		fs.mu.Unlock()
		return
	}
	fs.deletedPaths[relPath] = true
	fs.mu.Unlock()
	if fs.txnLog != nil {
		_, _ = fs.txnLog.Record(TxnDelete, relPath)
	}
}

func (fs *PassthroughFS) unDeletePath(relPath string) {
	fs.mu.Lock()
	delete(fs.deletedPaths, relPath)
	fs.mu.Unlock()
}

// undeleteAndEnsureParent un-deletes a path and ensures the backing parent exists.
// Returns the wasDeleted state for restoration on error.
func (fs *PassthroughFS) undeleteAndEnsureParent(childPath string) (wasDeleted bool, err error) {
	if fs.mutationMode {
		wasDeleted = fs.isPathDeleted(childPath)
		fs.unDeletePath(childPath)
	}
	if err := fs.ensureBackingParent(childPath); err != nil {
		if wasDeleted {
			fs.markPathDeleted(childPath)
		}
		return wasDeleted, err
	}
	return wasDeleted, nil
}

// --- materialization ---

func (fs *PassthroughFS) materializePxarFile(ino uint64) (string, error) {
	inoMu := fs.getInoMu(ino)
	inoMu.Lock()
	defer inoMu.Unlock()
	return fs.materializePxarFileLocked(ino)
}

func (fs *PassthroughFS) getInoMu(ino uint64) *sync.Mutex {
	fs.matMu.Lock()
	if fs.materialize == nil {
		fs.materialize = make(map[uint64]*sync.Mutex)
	}
	m, ok := fs.materialize[ino]
	if !ok {
		m = &sync.Mutex{}
		fs.materialize[ino] = m
	}
	fs.matMu.Unlock()
	return m
}

func (fs *PassthroughFS) materializePxarFileLocked(ino uint64) (string, error) {
	relPath := fs.nodePath(ino)
	if relPath == "" {
		return "", syscall.ENOENT
	}

	if fs.isBacked(ino) {
		abs := fs.absPath(relPath)
		if _, err := os.Lstat(abs); err == nil {
			return relPath, nil
		}
	}

	fs.pxar.mu.RLock()
	n, ok := fs.pxar.nodes[ino]
	fs.pxar.mu.RUnlock()
	if !ok {
		return "", syscall.ENOENT
	}

	if n.isDir {
		return relPath, nil
	}

	if err := fs.ensureBackingParent(relPath); err != nil {
		return "", err
	}

	abs := fs.absPath(relPath)

	if n.isSymlink {
		fs.pxar.readerMu.Lock()
		entry, err := fs.pxar.ReadEntryForNode(&n)
		fs.pxar.readerMu.Unlock()
		if err != nil {
			return "", err
		}
		if err := syscall.Symlink(entry.LinkTarget, abs); err != nil {
			return "", err
		}
		fs.applyACLOwnership(abs, false)
	} else if n.isReg {
		fs.pxar.readerMu.Lock()
		entry, err := fs.pxar.ReadEntryForNode(&n)
		fs.pxar.readerMu.Unlock()
		if err != nil {
			return "", err
		}

		f, err := os.OpenFile(abs, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return "", err
		}
		defer func() { _ = f.Close() }()

		rc, err := fs.pxar.reader.ReadFileContentReader(entry)
		if err != nil {
			return "", err
		}
		defer func() { _ = rc.Close() }()

		bufp := copyBufPool.Get().(*[]byte)
		defer copyBufPool.Put(bufp)
		if _, err := io.CopyBuffer(f, rc, *bufp); err != nil {
			return "", err
		}

		if err := os.Chmod(abs, os.FileMode(statMode(n.mode)&0o7777)); err != nil {
			return "", err
		}
	}

	ino2, _ := fs.lookupOrAllocIno(relPath, n.isDir)
	fs.setNode(ino2, relPath, true)
	fs.applyACLOwnership(abs, n.isDir)

	// Flush any pending metadata overlay onto the newly-materialized file.
	if mo := fs.getOverlay(relPath); mo != nil {
		fs.flushOverlayToDisk(abs, mo)
		delete(fs.metaOverlay, relPath)
	}

	if fs.txnLog != nil {
		_, _ = fs.txnLog.Record(TxnModify, relPath)
	}

	return relPath, nil
}

func (fs *PassthroughFS) materializePxarDir(ino uint64, relPath string) {
	fs.ensureDirBacking(ino, relPath)
}

// --- Lookup ---

func (fs *PassthroughFS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	if name == TransactionsDir {
		return fuse.ENOENT
	}

	parentPath := fs.nodePath(header.NodeId)
	childPath := joinPath(parentPath, name)

	if fs.mutationMode && fs.isPathDeleted(childPath) {
		return fuse.ENOENT
	}

	backedNode, _ := fs.statBacked(childPath)

	if backedNode != nil && backedNode.isDir {
		if pxarSt := fs.pxar.Lookup(cancel, header, name, out); pxarSt == fuse.OK {
			fs.setNode(out.NodeId, childPath, true)
			fs.mu.Lock()
			fs.pxarDir[out.NodeId] = true
			fs.mu.Unlock()
			return fuse.OK
		}
		if pxarIno := fs.recoverPxarDirNode(childPath); pxarIno != 0 {
			fs.setNode(pxarIno, childPath, true)
			fs.mu.Lock()
			fs.pxarDir[pxarIno] = true
			fs.mu.Unlock()
			fs.pxar.mu.RLock()
			if pn, ok := fs.pxar.nodes[pxarIno]; ok {
				fillEntryOut(pxarIno, &pn, out)
				fs.pxar.mu.RUnlock()
				return fuse.OK
			}
			fs.pxar.mu.RUnlock()
		}
	}

	if backedNode != nil {
		fs.setNode(backedNode.inode, childPath, true)
		fs.mu.Lock()
		backedNode.refs++
		backedNode.parent = header.NodeId
		fs.mu.Unlock()
		fillEntryOut(backedNode.inode, backedNode, out)
		return fuse.OK
	}

	st := fs.pxar.Lookup(cancel, header, name, out)
	if st == fuse.OK {
		if out.Mode&syscall.S_IFDIR != 0 {
			fs.ensureDirBacking(out.NodeId, childPath)
		} else {
			fs.setNode(out.NodeId, childPath, false)
		}
		fs.mu.Lock()
		if _, exists := fs.nodePaths[header.NodeId]; !exists {
			fs.nodePaths[header.NodeId] = parentPath
		}
		fs.mu.Unlock()
	}
	return st
}

// --- Metadata Overlay ---

// getOrCreateOverlay returns the existing metaOverride for path, or creates one.
func (fs *PassthroughFS) getOrCreateOverlay(relPath string) *metaOverride {
	mo := fs.metaOverlay[relPath]
	if mo == nil {
		mo = &metaOverride{
			xadd: make(map[string][]byte),
			xdel: make(map[string]bool),
		}
		fs.metaOverlay[relPath] = mo
	}
	return mo
}

// getOverlay returns the metaOverride for path, or nil.
func (fs *PassthroughFS) getOverlay(relPath string) *metaOverride {
	return fs.metaOverlay[relPath]
}

// flushOverlayToDisk applies pending metadata overrides to a newly-materialized file.
// Called after materialization to ensure the overlay state is on the real filesystem.
func (fs *PassthroughFS) flushOverlayToDisk(absPath string, mo *metaOverride) {
	if mo.mode != nil {
		_ = unix.Chmod(absPath, *mo.mode)
	}
	if mo.uid != nil && mo.gid != nil {
		_ = unix.Lchown(absPath, int(*mo.uid), int(*mo.gid))
	} else if mo.uid != nil {
		_ = unix.Lchown(absPath, int(*mo.uid), -1)
	} else if mo.gid != nil {
		_ = unix.Lchown(absPath, -1, int(*mo.gid))
	}
	if mo.size != nil {
		_ = os.Truncate(absPath, int64(*mo.size))
	}
	for name, val := range mo.xadd {
		_ = unix.Setxattr(absPath, name, val, 0)
	}
	for name := range mo.xdel {
		_ = unix.Removexattr(absPath, name)
	}
	if mo.mtime != nil || mo.atime != nil {
		var st syscall.Stat_t
		if syscall.Lstat(absPath, &st) == nil {
			atime := st.Atim
			mtime := st.Mtim
			if mo.atime != nil {
				atime.Sec = *mo.atime / 1000000000
				atime.Nsec = *mo.atime % 1000000000
			}
			if mo.mtime != nil {
				mtime.Sec = *mo.mtime / 1000000000
				mtime.Nsec = *mo.mtime % 1000000000
			}
			tv := []unix.Timeval{
				{Sec: atime.Sec, Usec: atime.Nsec / 1000},
				{Sec: mtime.Sec, Usec: mtime.Nsec / 1000},
			}
			_ = unix.Lutimes(absPath, tv)
		}
	}
}

// RebuildOverlayFromLog replays the transaction log to reconstruct the metadata overlay.
// Called on mount to restore pending metadata changes from a previous session.
func (fs *PassthroughFS) RebuildOverlayFromLog() error {
	if fs.txnLog == nil {
		return nil
	}
	txns, err := fs.txnLog.ReadAll()
	if err != nil {
		return err
	}
	for _, txn := range txns {
		switch txn.Type {
		case TxnSetAttr:
			if txn.Attrs == nil {
				continue
			}
			mo := fs.getOrCreateOverlay(txn.Path)
			if txn.Attrs.Mode != nil {
				mo.mode = txn.Attrs.Mode
			}
			if txn.Attrs.UID != nil {
				mo.uid = txn.Attrs.UID
			}
			if txn.Attrs.GID != nil {
				mo.gid = txn.Attrs.GID
			}
			if txn.Attrs.Size != nil {
				mo.size = txn.Attrs.Size
			}
			if txn.Attrs.Mtime != nil {
				mo.mtime = txn.Attrs.Mtime
			}
			if txn.Attrs.Atime != nil {
				mo.atime = txn.Attrs.Atime
			}
			// Remove from xdel if re-added
			delete(mo.xdel, "") // no-op placeholder
		case TxnSetXAttr:
			if txn.XAttr == nil {
				continue
			}
			mo := fs.getOrCreateOverlay(txn.Path)
			mo.xadd[txn.XAttr.Name] = txn.XAttr.Value
			delete(mo.xdel, txn.XAttr.Name)
		case TxnRemoveXAttr:
			if txn.XAttr == nil {
				continue
			}
			mo := fs.getOrCreateOverlay(txn.Path)
			delete(mo.xadd, txn.XAttr.Name)
			mo.xdel[txn.XAttr.Name] = true
		}
	}
	return nil
}

// --- GetAttr / SetAttr ---

func (fs *PassthroughFS) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	if fs.isBacked(input.NodeId) {
		rel := fs.nodePath(input.NodeId)
		var st syscall.Stat_t
		if err := syscall.Lstat(fs.absPath(rel), &st); err != nil {
			return fuse.ToStatus(err)
		}
		n := nodeFromStat(&st)
		n.inode = input.NodeId
		fillAttrOut(n, out)
		return fuse.OK
	}
	status := fs.pxar.GetAttr(cancel, input, out)
	if status != fuse.OK {
		return status
	}

	// Overlay pending metadata changes on top of pxar attributes.
	rel := fs.nodePath(input.NodeId)
	if mo := fs.getOverlay(rel); mo != nil {
		if mo.mode != nil {
			out.Attr.Mode = *mo.mode
		}
		if mo.uid != nil {
			out.Attr.Uid = *mo.uid
		}
		if mo.gid != nil {
			out.Attr.Gid = *mo.gid
		}
		if mo.size != nil {
			out.Attr.Size = *mo.size
		}
		if mo.mtime != nil {
			sec := *mo.mtime / 1_000_000_000
			nsec := *mo.mtime % 1_000_000_000
			out.Attr.Mtime = uint64(sec)
			out.Attr.Mtimensec = uint32(nsec)
		}
		if mo.atime != nil {
			sec := *mo.atime / 1_000_000_000
			nsec := *mo.atime % 1_000_000_000
			out.Attr.Atime = uint64(sec)
			out.Attr.Atimensec = uint32(nsec)
		}
	}

	return fuse.OK
}

func (fs *PassthroughFS) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {
	if !fs.mutationMode && fs.isPxarBacked(input.NodeId) {
		n := fs.getPxarNode(input.NodeId)
		if n == nil || !n.isDir {
			return fuse.EPERM
		}
	}

	// Size changes require materialization (content modification).
	if _, ok := input.GetSize(); ok && !fs.isBacked(input.NodeId) {
		if !fs.mutationMode {
			return fuse.EROFS
		}
		if _, err := fs.materializePxarFile(input.NodeId); err != nil {
			return fuse.ToStatus(err)
		}
	}

	rel := fs.nodePath(input.NodeId)
	isBacked := fs.isBacked(input.NodeId)

	// Build transaction attrs and overlay update.
	if fs.mutationMode && fs.txnLog != nil {
		attrs := &TxnAttrs{}
		hasAttr := false
		if v, ok := input.GetMode(); ok {
			m := uint32(v)
			attrs.Mode = &m
			hasAttr = true
		}
		if v, ok := input.GetUID(); ok {
			u := uint32(v)
			attrs.UID = &u
			hasAttr = true
		}
		if v, ok := input.GetGID(); ok {
			g := uint32(v)
			attrs.GID = &g
			hasAttr = true
		}
		if v, ok := input.GetSize(); ok {
			sz := v
			attrs.Size = &sz
			hasAttr = true
		}
		if a, ok := input.GetATime(); ok {
			aNs := a.UnixNano()
			attrs.Atime = &aNs
			hasAttr = true
		}
		if m, ok := input.GetMTime(); ok {
			mNs := m.UnixNano()
			attrs.Mtime = &mNs
			hasAttr = true
		}
		if hasAttr {
			_, _ = fs.txnLog.RecordSetAttr(rel, attrs)
		}

		// Update in-memory overlay for non-backed entries.
		if !isBacked {
			mo := fs.getOrCreateOverlay(rel)
			if attrs.Mode != nil {
				mo.mode = attrs.Mode
			}
			if attrs.UID != nil {
				mo.uid = attrs.UID
			}
			if attrs.GID != nil {
				mo.gid = attrs.GID
			}
			if attrs.Size != nil {
				mo.size = attrs.Size
			}
			if attrs.Atime != nil {
				mo.atime = attrs.Atime
			}
			if attrs.Mtime != nil {
				mo.mtime = attrs.Mtime
			}
		}
	}

	// Apply to backing filesystem.
	if isBacked {
		abs := fs.absPath(rel)
		if v, ok := input.GetMode(); ok {
			if err := unix.Chmod(abs, v); err != nil {
				return fuse.ToStatus(err)
			}
		}
		if v, ok := input.GetUID(); ok {
			if err := unix.Lchown(abs, int(v), -1); err != nil {
				return fuse.ToStatus(err)
			}
		}
		if v, ok := input.GetGID(); ok {
			if err := unix.Lchown(abs, -1, int(v)); err != nil {
				return fuse.ToStatus(err)
			}
		}
		if v, ok := input.GetSize(); ok {
			if err := os.Truncate(abs, int64(v)); err != nil {
				return fuse.ToStatus(err)
			}
		}
		if atime, aok := input.GetATime(); aok {
			if mtime, mok := input.GetMTime(); mok {
				tv := []unix.Timeval{
					{Sec: atime.Unix(), Usec: int64(atime.Nanosecond() / 1000)},
					{Sec: mtime.Unix(), Usec: int64(mtime.Nanosecond() / 1000)},
				}
				if err := unix.Lutimes(abs, tv); err != nil {
					return fuse.ToStatus(err)
				}
			}
		}
	}

	return fs.GetAttr(cancel, &fuse.GetAttrIn{InHeader: input.InHeader}, out)
}

// --- Create / Mkdir / Mknod / Symlink ---

func (fs *PassthroughFS) Create(cancel <-chan struct{}, input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	parentPath := fs.nodePath(input.NodeId)
	childPath := joinPath(parentPath, name)

	wasDeleted, err := fs.undeleteAndEnsureParent(childPath)
	if err != nil {
		return fuse.ToStatus(err)
	}

	abs := fs.absPath(childPath)

	flags := int(input.Flags) | os.O_CREATE | os.O_EXCL
	fd, createErr := syscall.Open(abs, flags, uint32(input.Mode&0o777))
	if createErr == nil {
		ino, _ := fs.lookupOrAllocIno(childPath, false)
		fs.setNode(ino, childPath, true)
		fs.applyACLOwnership(abs, false)
		return fs.finishCreate(ino, fd, out)
	}

	ino, _ := fs.lookupOrAllocIno(childPath, false)
	fs.setNode(ino, childPath, true)
	fd, err = syscall.Open(abs, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		if wasDeleted {
			fs.markPathDeleted(childPath)
		}
		return fuse.ToStatus(err)
	}
	return fs.finishCreate(ino, fd, out)
}

func (fs *PassthroughFS) finishCreate(ino uint64, fd int, out *fuse.CreateOut) fuse.Status {
	fs.fhmu.Lock()
	fhID := fs.nextFh
	fs.nextFh++
	fs.handles[fhID] = &passFh{fd: fd, inode: ino}
	fs.fhmu.Unlock()

	var st syscall.Stat_t
	if err := syscall.Fstat(fd, &st); err != nil {
		return fuse.ToStatus(err)
	}

	out.NodeId = ino
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	out.Fh = fhID
	out.OpenFlags = fuse.FOPEN_KEEP_CACHE
	n := nodeFromStat(&st)
	n.inode = ino
	n.mtimeNanos = uint32(st.Mtim.Nsec)
	n.isReg = true
	fillAttr(&out.Attr, n)
	return fuse.OK
}

// Mkdir creates a directory in the backing filesystem.
func (fs *PassthroughFS) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) fuse.Status {
	parentPath := fs.nodePath(input.NodeId)
	childPath := joinPath(parentPath, name)

	wasDeleted, err := fs.undeleteAndEnsureParent(childPath)
	if err != nil {
		return fuse.ToStatus(err)
	}

	abs := fs.absPath(childPath)
	if err := syscall.Mkdir(abs, input.Mode&0o777); err != nil {
		if err == syscall.EEXIST {
			ino, _ := fs.lookupOrAllocIno(childPath, true)
			fs.setNode(ino, childPath, true)
			var st syscall.Stat_t
			if err2 := syscall.Lstat(abs, &st); err2 != nil {
				return fuse.ToStatus(err2)
			}
			n := nodeFromStat(&st)
			n.inode = ino
			n.mtimeNanos = uint32(st.Mtim.Nsec)
			n.isDir = true
			fillEntryOut(ino, n, out)
			return fuse.OK
		}
		_ = wasDeleted // EEXIST handled; other errors are not wasDeleted-restorable per pattern
		return fuse.ToStatus(err)
	}
	fs.applyACLOwnership(abs, true)

	ino := fs.allocBackedIno(true)
	fs.setNode(ino, childPath, true)

	var st syscall.Stat_t
	if err := syscall.Lstat(abs, &st); err != nil {
		return fuse.ToStatus(err)
	}

	n := nodeFromStat(&st)
	n.inode = ino
	n.mtimeNanos = uint32(st.Mtim.Nsec)
	n.isDir = true
	fillEntryOut(ino, n, out)
	return fuse.OK
}

func (fs *PassthroughFS) Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) fuse.Status {
	parentPath := fs.nodePath(input.NodeId)
	childPath := joinPath(parentPath, name)

	wasDeleted, err := fs.undeleteAndEnsureParent(childPath)
	if err != nil {
		return fuse.ToStatus(err)
	}

	abs := fs.absPath(childPath)
	isDir := input.Mode&syscall.S_IFDIR != 0
	if err := syscall.Mknod(abs, input.Mode, int(input.Rdev)); err != nil {
		if err == syscall.EEXIST {
			ino, _ := fs.lookupOrAllocIno(childPath, isDir)
			fs.setNode(ino, childPath, true)
			var st syscall.Stat_t
			if err2 := syscall.Lstat(abs, &st); err2 != nil {
				return fuse.ToStatus(err2)
			}
			n := nodeFromStat(&st)
			n.inode = ino
			n.mtimeNanos = uint32(st.Mtim.Nsec)
			fillEntryOut(ino, n, out)
			return fuse.OK
		}
		_ = wasDeleted
		return fuse.ToStatus(err)
	}

	ino := fs.allocBackedIno(isDir)
	fs.applyACLOwnership(abs, isDir)
	fs.setNode(ino, childPath, true)

	var st syscall.Stat_t
	if err := syscall.Lstat(abs, &st); err != nil {
		return fuse.ToStatus(err)
	}

	n := nodeFromStat(&st)
	n.inode = ino
	n.mtimeNanos = uint32(st.Mtim.Nsec)
	fillEntryOut(ino, n, out)
	return fuse.OK
}

func (fs *PassthroughFS) Symlink(cancel <-chan struct{}, header *fuse.InHeader, target string, linkName string, out *fuse.EntryOut) fuse.Status {
	parentPath := fs.nodePath(header.NodeId)
	childPath := joinPath(parentPath, linkName)

	wasDeleted, err := fs.undeleteAndEnsureParent(childPath)
	if err != nil {
		return fuse.ToStatus(err)
	}

	abs := fs.absPath(childPath)
	if err := syscall.Symlink(target, abs); err != nil {
		if err == syscall.EEXIST {
			ino, _ := fs.lookupOrAllocIno(childPath, false)
			fs.setNode(ino, childPath, true)
			var st syscall.Stat_t
			if err2 := syscall.Lstat(abs, &st); err2 != nil {
				return fuse.ToStatus(err2)
			}
			n := nodeFromStat(&st)
			n.inode = ino
			n.mtimeNanos = uint32(st.Mtim.Nsec)
			n.isSymlink = true
			fillEntryOut(ino, n, out)
			return fuse.OK
		}
		_ = wasDeleted
		return fuse.ToStatus(err)
	}

	ino := fs.allocBackedIno(false)
	fs.setNode(ino, childPath, true)

	var st syscall.Stat_t
	if err := syscall.Lstat(abs, &st); err != nil {
		return fuse.ToStatus(err)
	}

	n := nodeFromStat(&st)
	n.inode = ino
	n.mtimeNanos = uint32(st.Mtim.Nsec)
	n.isSymlink = true
	fillEntryOut(ino, n, out)
	return fuse.OK
}

// --- Unlink / Rmdir ---

// removeEntry is the shared implementation for Unlink and Rmdir.
func (fs *PassthroughFS) removeEntry(parentIno uint64, name string, removeFn func(string) error) fuse.Status {
	parentPath := fs.nodePath(parentIno)
	childPath := joinPath(parentPath, name)

	isPxar := fs.isPxarChild(parentIno, name)

	if isPxar && !fs.mutationMode {
		return fuse.EPERM
	}

	if isPxar && fs.mutationMode {
		fs.markPathDeleted(childPath)

		abs := fs.absPath(childPath)
		if _, err := os.Lstat(abs); err == nil {
			_ = removeFn(abs)
		}

		fs.cleanInodeMappings(childPath)
		return fuse.OK
	}

	abs := fs.absPath(childPath)

	if _, err := os.Lstat(abs); err != nil {
		return fuse.ENOENT
	}
	if err := removeFn(abs); err != nil {
		return fuse.ToStatus(err)
	}

	fs.cleanInodeMappings(childPath)
	return fuse.OK
}

func (fs *PassthroughFS) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	return fs.removeEntry(header.NodeId, name, syscall.Unlink)
}

func (fs *PassthroughFS) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	return fs.removeEntry(header.NodeId, name, syscall.Rmdir)
}

// --- Rename ---

func (fs *PassthroughFS) renamePaths(oldPath, newPath string) {
	fs.mu.Lock()
	ino, ok := fs.pathToIno[oldPath]
	if ok {
		oldDstIno, hadDst := fs.pathToIno[newPath]

		delete(fs.pathToIno, oldPath)
		fs.pathToIno[newPath] = ino
		fs.nodePaths[ino] = newPath

		if hadDst && oldDstIno != ino {
			delete(fs.nodePaths, oldDstIno)
			delete(fs.backed, oldDstIno)
			delete(fs.pxarDir, oldDstIno)
		}
	}
	fs.mu.Unlock()
}

func (fs *PassthroughFS) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName string, newName string) fuse.Status {
	oldIsPxar := fs.isPxarChild(input.NodeId, oldName)
	newIsPxar := fs.isPxarChild(input.Newdir, newName)

	if !fs.mutationMode {
		if oldIsPxar {
			return fuse.EPERM
		}
		if newIsPxar {
			return fuse.EPERM
		}
	}

	oldParentPath := fs.nodePath(input.NodeId)
	newParentPath := fs.nodePath(input.Newdir)
	oldPath := joinPath(oldParentPath, oldName)
	newPath := joinPath(newParentPath, newName)

	wasDeleted := fs.isPathDeleted(newPath)

	if fs.mutationMode {
		if oldIsPxar {
			if entries, err := fs.pxar.ReadDirRaw(input.NodeId); err == nil {
				for _, e := range entries {
					if e.name == oldName {
						fs.pxar.RegisterSlimNode(&e, input.NodeId)
						ino := e.inode
						if e.isDir {
							fs.materializePxarDir(ino, oldPath)
						} else {
							if _, err := fs.materializePxarFile(ino); err != nil {
								return fuse.ToStatus(err)
							}
						}
						break
					}
				}
			}
		}

		if newIsPxar && input.Flags&renameExchange == 0 {
			fs.markPathDeleted(newPath)
		}
	}

	oldAbs := fs.absPath(oldPath)

	if _, err := os.Lstat(oldAbs); err != nil {
		if wasDeleted {
			fs.markPathDeleted(newPath)
		}
		return fuse.EROFS
	}

	if fs.mutationMode && wasDeleted {
		fs.unDeletePath(newPath)
	}

	if err := fs.ensureBackingParent(newPath); err != nil {
		if wasDeleted {
			fs.markPathDeleted(newPath)
		}
		return fuse.ToStatus(err)
	}

	newAbs := fs.absPath(newPath)

	if oldIsPxar && fs.mutationMode {
		fs.markPathDeleted(oldPath)
	}

	if input.Flags&renameNoReplace != 0 {
		if _, err := os.Lstat(newAbs); err == nil {
			if wasDeleted {
				fs.markPathDeleted(newPath)
			}
			return fuse.Status(syscall.EEXIST)
		}
		if err := os.Rename(oldAbs, newAbs); err != nil {
			if wasDeleted {
				fs.markPathDeleted(newPath)
			}
			return fuse.ToStatus(err)
		}
		fs.renamePaths(oldPath, newPath)

		if oldIsPxar && fs.txnLog != nil {
			_, _ = fs.txnLog.RecordRename(oldPath, newPath)
		}
		return fuse.OK
	}

	if input.Flags&renameExchange != 0 {
		if err := unix.Renameat2(unix.AT_FDCWD, oldAbs, unix.AT_FDCWD, newAbs, unix.RENAME_EXCHANGE); err != nil {
			if wasDeleted {
				fs.markPathDeleted(newPath)
			}
			return fuse.ToStatus(err)
		}

		fs.mu.Lock()
		oldIno, oldOk := fs.pathToIno[oldPath]
		newIno, newOk := fs.pathToIno[newPath]

		if oldOk && newOk {
			fs.nodePaths[oldIno] = newPath
			fs.nodePaths[newIno] = oldPath
			fs.pathToIno[oldPath] = newIno
			fs.pathToIno[newPath] = oldIno
		} else if oldOk {
			delete(fs.pathToIno, oldPath)
			fs.nodePaths[oldIno] = newPath
			fs.pathToIno[newPath] = oldIno
		} else if newOk {
			delete(fs.pathToIno, newPath)
			fs.nodePaths[newIno] = oldPath
			fs.pathToIno[oldPath] = newIno
		}
		fs.mu.Unlock()

		if fs.txnLog != nil {
			if oldIsPxar {
				_, _ = fs.txnLog.RecordRename(oldPath, newPath)
			}
			if newIsPxar {
				_, _ = fs.txnLog.RecordRename(newPath, oldPath)
			}
		}
		return fuse.OK
	}

	if err := os.Rename(oldAbs, newAbs); err != nil {
		if wasDeleted {
			fs.markPathDeleted(newPath)
		}
		return fuse.ToStatus(err)
	}
	fs.renamePaths(oldPath, newPath)

	if oldIsPxar && fs.txnLog != nil {
		_, _ = fs.txnLog.RecordRename(oldPath, newPath)
	}
	return fuse.OK
}

// --- Open / Read / Write / Flush / Fsync / Release ---

func (fs *PassthroughFS) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	if !fs.isBacked(input.NodeId) {
		if fs.mutationMode && input.Flags&(uint32(syscall.O_WRONLY|syscall.O_RDWR)) != 0 {
			// Defer materialization — don't copy file content just for opening.
			// Return a sentinel fh=0; Write/Fallocate will materialize lazily.
			out.Fh = 0
			out.OpenFlags = fuse.FOPEN_KEEP_CACHE
			return fuse.OK
		}
		return fs.pxar.Open(cancel, input, out)
	}

	if fs.isPxarBacked(input.NodeId) && !fs.mutationMode && input.Flags&(uint32(syscall.O_WRONLY|syscall.O_RDWR)) != 0 {
		return fuse.EROFS
	}

	rel := fs.nodePath(input.NodeId)
	flags := int(input.Flags) & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)
	fhID, err := fs.registerFh(rel, input.NodeId, flags)
	if err != nil {
		return fuse.ToStatus(err)
	}
	out.Fh = fhID
	out.OpenFlags = fuse.FOPEN_KEEP_CACHE
	return fuse.OK
}

func (fs *PassthroughFS) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	if !fs.isBacked(input.NodeId) {
		return fs.pxar.Read(cancel, input, buf)
	}

	fh := fs.handleForNode(input.NodeId, input.Fh)
	if fh == nil {
		return nil, fuse.EBADF
	}

	n, err := syscall.Pread(fh.fd, buf, int64(input.Offset))
	if err != nil {
		return nil, fuse.ToStatus(err)
	}
	if n == 0 {
		return fuse.ReadResultData(nil), fuse.OK
	}
	return fuse.ReadResultData(buf[:n]), fuse.OK
}

func (fs *PassthroughFS) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (uint32, fuse.Status) {
	if !fs.isBacked(input.NodeId) {
		if !fs.mutationMode {
			return 0, fuse.EROFS
		}
		if _, err := fs.materializePxarFile(input.NodeId); err != nil {
			return 0, fuse.ToStatus(err)
		}
	}
	if fs.isPxarBacked(input.NodeId) && !fs.mutationMode {
		return 0, fuse.EROFS
	}

	fh := fs.handleForNode(input.NodeId, input.Fh)
	if fh == nil {
		// Lazy-open: materialization happened but no fd yet.
		rel := fs.nodePath(input.NodeId)
		fd, err := syscall.Open(fs.absPath(rel), syscall.O_WRONLY, 0)
		if err != nil {
			return 0, fuse.ToStatus(err)
		}
		fs.fhmu.Lock()
		id := fs.nextFh
		fs.nextFh++
		fs.handles[id] = &passFh{fd: fd, inode: input.NodeId}
		fs.fhmu.Unlock()
		fh = fs.handles[id]
	}

	n, err := syscall.Pwrite(fh.fd, data, int64(input.Offset))
	if err != nil {
		return 0, fuse.ToStatus(err)
	}
	return uint32(n), fuse.OK
}

func (fs *PassthroughFS) fsyncBacked(nodeID, fhID uint64) fuse.Status {
	if !fs.isBacked(nodeID) {
		return fuse.OK
	}
	fh := fs.handleForNode(nodeID, fhID)
	if fh == nil {
		return fuse.EBADF
	}
	if err := syscall.Fsync(fh.fd); err != nil {
		return fuse.ToStatus(err)
	}
	return fuse.OK
}

func (fs *PassthroughFS) Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status {
	return fs.fsyncBacked(input.NodeId, input.Fh)
}

func (fs *PassthroughFS) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	return fs.fsyncBacked(input.NodeId, input.Fh)
}

func (fs *PassthroughFS) Fallocate(cancel <-chan struct{}, input *fuse.FallocateIn) fuse.Status {
	if !fs.isBacked(input.NodeId) {
		if !fs.mutationMode {
			return fuse.EROFS
		}
		if _, err := fs.materializePxarFile(input.NodeId); err != nil {
			return fuse.ToStatus(err)
		}
	}
	if fs.isPxarBacked(input.NodeId) && !fs.mutationMode {
		return fuse.EROFS
	}

	fh := fs.handleForNode(input.NodeId, input.Fh)
	if fh == nil {
		rel := fs.nodePath(input.NodeId)
		fd, err := syscall.Open(fs.absPath(rel), syscall.O_WRONLY, 0)
		if err != nil {
			return fuse.ToStatus(err)
		}
		fs.fhmu.Lock()
		id := fs.nextFh
		fs.nextFh++
		fs.handles[id] = &passFh{fd: fd, inode: input.NodeId}
		fs.fhmu.Unlock()
		fh = fs.handles[id]
	}

	mode := uint32(0)
	if input.Mode&0x1 != 0 {
		mode |= unix.FALLOC_FL_KEEP_SIZE
	}

	if err := unix.Fallocate(fh.fd, mode, int64(input.Offset), int64(input.Length)); err != nil {
		return fuse.ToStatus(err)
	}
	return fuse.OK
}

func (fs *PassthroughFS) Release(cancel <-chan struct{}, input *fuse.ReleaseIn) {
	fs.fhmu.Lock()
	fh, ok := fs.handles[input.Fh]
	if ok {
		_ = syscall.Close(fh.fd)
		delete(fs.handles, input.Fh)
	}
	fs.fhmu.Unlock()

	if !fs.isBacked(input.NodeId) {
		fs.pxar.Release(cancel, input)
	}
}

func (fs *PassthroughFS) Lseek(cancel <-chan struct{}, in *fuse.LseekIn, out *fuse.LseekOut) fuse.Status {
	if !fs.isBacked(in.NodeId) {
		return fuse.ENOSYS
	}

	fh := fs.handleForNode(in.NodeId, in.Fh)
	if fh == nil {
		return fuse.EBADF
	}

	off, err := syscall.Seek(fh.fd, int64(in.Offset), int(in.Whence))
	if err != nil {
		return fuse.ToStatus(err)
	}
	out.Offset = uint64(off)
	return fuse.OK
}

// --- Readlink ---

func (fs *PassthroughFS) Readlink(cancel <-chan struct{}, header *fuse.InHeader) ([]byte, fuse.Status) {
	if fs.isBacked(header.NodeId) {
		rel := fs.nodePath(header.NodeId)
		target, err := os.Readlink(fs.absPath(rel))
		if err != nil {
			return nil, fuse.ToStatus(err)
		}
		return []byte(target), fuse.OK
	}
	return fs.pxar.Readlink(cancel, header)
}

// --- ReadDir / ReadDirPlus ---

func (fs *PassthroughFS) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	return fs.readDirImpl(cancel, input, out, false)
}

func (fs *PassthroughFS) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	return fs.readDirImpl(cancel, input, out, true)
}

func (fs *PassthroughFS) readDirImpl(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList, plus bool) fuse.Status {
	parentPath := fs.nodePath(input.NodeId)

	dirIno := input.NodeId
	if _, err := fs.pxar.ReadDirRaw(dirIno); err != nil && parentPath != "" {
		if recovered := fs.recoverPxarDirNode(parentPath); recovered != 0 {
			fs.setNode(recovered, parentPath, true)
			fs.mu.Lock()
			fs.pxarDir[recovered] = true
			fs.mu.Unlock()
			dirIno = recovered
		}
	}

	var pxarEntries []dirEntrySlim
	if pxarN, err := fs.pxar.ReadDirRaw(dirIno); err == nil {
		pxarEntries = pxarN
	}

	var backedEntries []dirEntrySlim
	localStats := make(map[uint64]syscall.Stat_t)
	if parentPath != "" {
		absParent := fs.absPath(parentPath)
		des, err := os.ReadDir(absParent)
		if err == nil && len(des) > 0 {
			backedEntries = make([]dirEntrySlim, 0, len(des))
			for _, de := range des {
				if de.Name() == TransactionsDir {
					continue
				}
				info, err := de.Info()
				if err != nil {
					continue
				}
				st := info.Sys().(*syscall.Stat_t)
				childPath := joinPath(parentPath, de.Name())
				mode := info.Mode()
				isDir := de.IsDir()
				ino, _ := fs.lookupOrAllocIno(childPath, isDir)
				fs.setNode(ino, childPath, true)

				backedEntries = append(backedEntries, dirEntrySlim{
					name:  de.Name(),
					inode: ino,
					mode:  uint32(mode),
				})
				localStats[ino] = *st
			}
		}
	}

	backedName := make(map[string]bool, len(backedEntries))
	for _, be := range backedEntries {
		backedName[be.name] = true
	}

	for i := range pxarEntries {
		pe := &pxarEntries[i]
		fs.pxar.RegisterSlimNode(pe, input.NodeId)
		childPath := joinPath(parentPath, pe.name)

		if fs.mutationMode && fs.isPathDeleted(childPath) {
			continue
		}

		if IsDirInode(pe.inode) {
			fs.ensureDirBacking(pe.inode, childPath)
		}
	}

	entries := make([]dirEntrySlim, 0, len(pxarEntries)+len(backedEntries))
	for _, pe := range pxarEntries {
		childPath := joinPath(parentPath, pe.name)
		if fs.mutationMode && fs.isPathDeleted(childPath) {
			continue
		}
		if !backedName[pe.name] {
			entries = append(entries, pe)
			if !IsDirInode(pe.inode) {
				fs.setNode(pe.inode, childPath, false)
			}
		} else if IsDirInode(pe.inode) {
			entries = append(entries, pe)
			fs.setNode(pe.inode, childPath, true)
			backedName[pe.name] = false
		}
	}
	for _, be := range backedEntries {
		if backedName[be.name] {
			entries = append(entries, be)
		}
	}

	if input.Offset == 0 {
		mode := fs.dirModeForNode(input.NodeId)
		if plus {
			eo := out.AddDirLookupEntry(fuse.DirEntry{Name: ".", Ino: input.NodeId, Mode: mode})
			if eo == nil {
				return fuse.OK
			}
			fs.fillEntryOutForNodeWithStats(input.NodeId, eo, localStats)
		} else {
			if !out.AddDirEntry(fuse.DirEntry{Name: ".", Ino: input.NodeId, Mode: mode}) {
				return fuse.OK
			}
		}
	}

	if input.Offset <= 1 {
		parentIno := RootInode
		parentMode := uint32(syscall.S_IFDIR | 0o555)

		if n := fs.getPxarNode(input.NodeId); n != nil {
			parentIno = n.parent
		}

		fs.pxar.mu.RLock()
		if pn, pok := fs.pxar.nodes[parentIno]; pok {
			parentMode = statMode(pn.mode)
		}
		fs.pxar.mu.RUnlock()

		if plus {
			eo := out.AddDirLookupEntry(fuse.DirEntry{Name: "..", Ino: parentIno, Mode: parentMode})
			if eo != nil {
				fs.fillEntryOutForNodeWithStats(parentIno, eo, localStats)
			}
		} else {
			out.AddDirEntry(fuse.DirEntry{Name: "..", Ino: parentIno, Mode: parentMode})
		}
	}

	start := max(int(input.Offset)-2, 0)
	for i := start; i < len(entries); i++ {
		de := fuse.DirEntry{
			Name: entries[i].name,
			Ino:  entries[i].inode,
			Mode: entries[i].mode,
		}
		if plus {
			eo := out.AddDirLookupEntry(de)
			if eo == nil {
				break
			}
			fs.fillEntryOutForNodeWithStats(entries[i].inode, eo, localStats)
		} else {
			if !out.AddDirEntry(de) {
				break
			}
		}
	}
	return fuse.OK
}

func (fs *PassthroughFS) dirModeForNode(ino uint64) uint32 {
	if fs.isBacked(ino) {
		rel := fs.nodePath(ino)
		var st syscall.Stat_t
		if err := syscall.Lstat(fs.absPath(rel), &st); err == nil {
			return uint32(st.Mode) | syscall.S_IFDIR
		}
	}
	if n := fs.getPxarNode(ino); n != nil {
		return statMode(n.mode)
	}
	return uint32(syscall.S_IFDIR | 0o555)
}

func (fs *PassthroughFS) fillEntryOutForNodeWithStats(ino uint64, out *fuse.EntryOut, localStats map[uint64]syscall.Stat_t) {
	if st, ok := localStats[ino]; ok {
		n := nodeFromStat(&st)
		n.inode = ino
		n.mtimeNanos = uint32(st.Mtim.Nsec)
		fillEntryOut(ino, n, out)
		return
	}
	if fs.isBacked(ino) {
		rel := fs.nodePath(ino)
		nd, err := fs.statBacked(rel)
		if err == nil {
			fillEntryOut(nd.inode, nd, out)
			return
		}
	}
	if n := fs.getPxarNode(ino); n != nil {
		fillEntryOut(ino, n, out)
	}
}

// --- Access ---

func (fs *PassthroughFS) Access(cancel <-chan struct{}, input *fuse.AccessIn) fuse.Status {
	if fs.isBacked(input.NodeId) {
		return fuse.OK
	}
	return fs.pxar.Access(cancel, input)
}

// --- xattr ---

func (fs *PassthroughFS) GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, dest []byte) (uint32, fuse.Status) {
	if fs.isBacked(header.NodeId) {
		rel := fs.nodePath(header.NodeId)
		sz, err := unix.Getxattr(fs.absPath(rel), attr, dest)
		if err != nil {
			return 0, fuse.ToStatus(err)
		}
		return uint32(sz), fuse.OK
	}
	rel := fs.nodePath(header.NodeId)
	if mo := fs.getOverlay(rel); mo != nil {
		if mo.xdel[attr] {
			return 0, fuse.Status(syscall.ENODATA)
		}
		if val, ok := mo.xadd[attr]; ok {
			if dest == nil {
				return uint32(len(val)), fuse.OK
			}
			if uint32(len(dest)) < uint32(len(val)) {
				return 0, fuse.Status(syscall.ERANGE)
			}
			return uint32(copy(dest, val)), fuse.OK
		}
	}
	return fs.pxar.GetXAttr(cancel, header, attr, dest)
}

func (fs *PassthroughFS) ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (uint32, fuse.Status) {
	if fs.isBacked(header.NodeId) {
		rel := fs.nodePath(header.NodeId)
		sz, err := unix.Listxattr(fs.absPath(rel), dest)
		if err != nil {
			return 0, fuse.ToStatus(err)
		}
		return uint32(sz), fuse.OK
	}

	// Merge pxar xattrs with overlay additions/removals.
	rel := fs.nodePath(header.NodeId)

	// Get base list from pxar.
	var baseNames []string
	{
		tmpBuf := make([]byte, 4096)
		sz, status := fs.pxar.ListXAttr(cancel, header, tmpBuf)
		if status != fuse.OK {
			if status == fuse.Status(syscall.ERANGE) {
				tmpBuf = make([]byte, 16384)
				sz, status = fs.pxar.ListXAttr(cancel, header, tmpBuf)
			}
			if status != fuse.OK {
				return 0, status
			}
		}
		for name := range bytes.SplitSeq(tmpBuf[:sz], []byte{0}) {
			if len(name) > 0 {
				baseNames = append(baseNames, string(name))
			}
		}
	}

	// Build merged list.
	names := make(map[string]bool)
	for _, n := range baseNames {
		names[n] = true
	}
	mo := fs.getOverlay(rel)
	if mo != nil {
		for name := range mo.xdel {
			delete(names, name)
		}
		for name := range mo.xadd {
			names[name] = true
		}
	}

	var total uint32
	for name := range names {
		total += uint32(len(name)) + 1
	}
	if dest == nil {
		return total, fuse.OK
	}
	if uint32(len(dest)) < total {
		return 0, fuse.Status(syscall.ERANGE)
	}
	pos := 0
	for name := range names {
		pos += copy(dest[pos:], name)
		dest[pos] = 0
		pos++
	}
	return uint32(pos), fuse.OK
}

func (fs *PassthroughFS) SetXAttr(cancel <-chan struct{}, input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	if isACLXattr(attr) {
		return fs.setACLXAttr(input, attr, data)
	}
	if !fs.mutationMode {
		return fuse.EROFS
	}

	rel := fs.nodePath(input.NodeId)

	if fs.isBacked(input.NodeId) {
		flags := 0
		if input.Flags&xattrCreate != 0 {
			flags = unix.XATTR_CREATE
		} else if input.Flags&xattrReplace != 0 {
			flags = unix.XATTR_REPLACE
		}
		if err := unix.Setxattr(fs.absPath(rel), attr, data, flags); err != nil {
			return fuse.ToStatus(err)
		}
	}

	// Record in overlay + txn log.
	if fs.txnLog != nil {
		_, _ = fs.txnLog.RecordSetXAttr(rel, attr, data)
	}
	if !fs.isBacked(input.NodeId) {
		mo := fs.getOrCreateOverlay(rel)
		mo.xadd[attr] = make([]byte, len(data))
		copy(mo.xadd[attr], data)
		delete(mo.xdel, attr)
	}
	return fuse.OK
}

func (fs *PassthroughFS) setACLXAttr(input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	if !fs.mutationMode {
		return fuse.EROFS
	}

	rel := fs.nodePath(input.NodeId)

	if fs.isBacked(input.NodeId) {
		flags := 0
		if input.Flags&xattrCreate != 0 {
			flags = unix.XATTR_CREATE
		} else if input.Flags&xattrReplace != 0 {
			flags = unix.XATTR_REPLACE
		}
		if err := unix.Setxattr(fs.absPath(rel), attr, data, flags); err != nil {
			return fuse.ToStatus(err)
		}
	}

	if fs.txnLog != nil {
		_, _ = fs.txnLog.RecordSetXAttr(rel, attr, data)
	}
	if !fs.isBacked(input.NodeId) {
		mo := fs.getOrCreateOverlay(rel)
		mo.xadd[attr] = make([]byte, len(data))
		copy(mo.xadd[attr], data)
		delete(mo.xdel, attr)
	}
	return fuse.OK
}

func (fs *PassthroughFS) RemoveXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string) fuse.Status {
	if isACLXattr(attr) {
		return fs.removeACLXAttr(header, attr)
	}
	if !fs.mutationMode {
		return fuse.EROFS
	}

	rel := fs.nodePath(header.NodeId)

	if fs.isBacked(header.NodeId) {
		if err := unix.Removexattr(fs.absPath(rel), attr); err != nil {
			return fuse.ToStatus(err)
		}
	}

	if fs.txnLog != nil {
		_, _ = fs.txnLog.RecordRemoveXAttr(rel, attr)
	}
	if !fs.isBacked(header.NodeId) {
		mo := fs.getOrCreateOverlay(rel)
		delete(mo.xadd, attr)
		mo.xdel[attr] = true
	}
	return fuse.OK
}

func (fs *PassthroughFS) removeACLXAttr(header *fuse.InHeader, attr string) fuse.Status {
	if !fs.mutationMode {
		return fuse.EROFS
	}

	rel := fs.nodePath(header.NodeId)

	if fs.isBacked(header.NodeId) {
		if err := unix.Removexattr(fs.absPath(rel), attr); err != nil {
			return fuse.ToStatus(err)
		}
	}

	if fs.txnLog != nil {
		_, _ = fs.txnLog.RecordRemoveXAttr(rel, attr)
	}
	if !fs.isBacked(header.NodeId) {
		mo := fs.getOrCreateOverlay(rel)
		delete(mo.xadd, attr)
		mo.xdel[attr] = true
	}
	return fuse.OK
}

// saveRootACLs reads all ACL-related xattrs from the backing dir root.
func (fs *PassthroughFS) saveRootACLs() map[string][]byte {
	var xattrs map[string][]byte

	// First call: get size.
	sz, err := unix.Llistxattr(fs.backingDir, nil)
	if err != nil || sz == 0 {
		return nil
	}

	// Second call: get names.
	nameBuf := make([]byte, sz)
	sz, err = unix.Llistxattr(fs.backingDir, nameBuf)
	if err != nil {
		return nil
	}

	valBuf := make([]byte, 4096)
	for name := range bytes.SplitSeq(nameBuf[:sz], []byte{0}) {
		if len(name) == 0 {
			continue
		}
		s := string(name)
		if !isACLXattr(s) {
			continue
		}
		for {
			sz, err := unix.Lgetxattr(fs.backingDir, s, valBuf)
			if err == unix.ERANGE {
				valBuf = make([]byte, len(valBuf)*2)
				continue
			}
			if err != nil {
				break
			}
			val := make([]byte, sz)
			copy(val, valBuf[:sz])
			if xattrs == nil {
				xattrs = make(map[string][]byte)
			}
			xattrs[s] = val
			break
		}
	}

	return xattrs
}

// restoreRootACLs writes saved ACL xattrs and ownership back to the backing
// dir root. Also applies the ACLConfig defaults.
func (fs *PassthroughFS) restoreRootACLs(xattrs map[string][]byte) {
	uid := fs.acl.OwnerUID
	gid := fs.acl.OwnerGID

	// Restore saved ACL xattrs (inheritance).
	for name, val := range xattrs {
		_ = unix.Setxattr(fs.backingDir, name, val, 0)
	}

	if uid != 0 || gid != 0 {
		_ = os.Chown(fs.backingDir, uid, gid)
	}
}

// applyACLOwnership applies the configured default owner/group to a newly
// created or materialized file in the backing dir.
func (fs *PassthroughFS) applyACLOwnership(absPath string, isDir bool) {
	uid := fs.acl.OwnerUID
	gid := fs.acl.OwnerGID
	if uid != 0 || gid != 0 {
		_ = os.Chown(absPath, uid, gid)
	}
}

// ForceACLOwnership walks the entire backing dir and forces owner/group
// on every file and directory. Called once at mount when force flags are set.
func (fs *PassthroughFS) ForceACLOwnership() {
	if !fs.acl.ForceOwner && !fs.acl.ForceGroup {
		return
	}
	uid := fs.acl.OwnerUID
	gid := fs.acl.OwnerGID
	if uid == 0 && gid == 0 {
		return
	}
	_ = filepath.Walk(fs.backingDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		_ = os.Chown(path, uid, gid)
		return nil
	})
}

// --- StatFs ---

func (fs *PassthroughFS) StatFs(cancel <-chan struct{}, header *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	var st unix.Statfs_t
	if err := unix.Statfs(fs.backingDir, &st); err != nil {
		return fuse.ToStatus(err)
	}
	out.Blocks = st.Blocks
	out.Bfree = st.Bfree
	out.Bavail = st.Bavail
	out.Files = st.Files
	out.Ffree = st.Ffree
	out.Bsize = uint32(st.Bsize)
	out.NameLen = 255
	out.Frsize = uint32(st.Bsize)
	return fuse.OK
}

// --- Statx ---

func (fs *PassthroughFS) Statx(cancel <-chan struct{}, input *fuse.StatxIn, out *fuse.StatxOut) fuse.Status {
	return fuse.ENOSYS
}

// --- Forget ---

func (fs *PassthroughFS) Forget(nodeID, nlookup uint64) {
	fs.mu.Lock()
	if fs.backed[nodeID] {
		if path, ok := fs.nodePaths[nodeID]; ok {
			delete(fs.pathToIno, path)
		}
		delete(fs.backed, nodeID)
		delete(fs.nodePaths, nodeID)
		delete(fs.pxarDir, nodeID)
	}
	fs.mu.Unlock()
	fs.pxar.Forget(nodeID, nlookup)
}

// --- unsupported ---

func (fs *PassthroughFS) CopyFileRange(cancel <-chan struct{}, input *fuse.CopyFileRangeIn) (uint32, fuse.Status) {
	return 0, fuse.ENOSYS
}

func (fs *PassthroughFS) Ioctl(cancel <-chan struct{}, input *fuse.IoctlIn, inbuf []byte, output *fuse.IoctlOut, outbuf []byte) fuse.Status {
	return fuse.ENOSYS
}

func (fs *PassthroughFS) GetLk(cancel <-chan struct{}, in *fuse.LkIn, out *fuse.LkOut) fuse.Status {
	return fuse.ENOSYS
}

func (fs *PassthroughFS) SetLk(cancel <-chan struct{}, in *fuse.LkIn) fuse.Status {
	return fuse.ENOSYS
}

func (fs *PassthroughFS) SetLkw(cancel <-chan struct{}, in *fuse.LkIn) fuse.Status {
	return fuse.ENOSYS
}

// --- recovery ---

func (fs *PassthroughFS) recoverPxarDirNode(relPath string) uint64 {
	if relPath == "" || relPath == "/" {
		return RootInode
	}

	parts := strings.Split(relPath, "/")
	curIno := RootInode

	for i := 1; i < len(parts); i++ {
		name := parts[i]
		if name == "" {
			continue
		}

		entries, err := fs.pxar.ReadDirRaw(curIno)
		if err != nil {
			return 0
		}

		found := false
		for _, e := range entries {
			if e.name == name {
				fs.pxar.RegisterSlimNode(&e, curIno)
				if IsDirInode(e.inode) {
					curIno = e.inode
					found = true
				} else {
					return e.inode
				}
				break
			}
		}
		if !found {
			return 0
		}
	}
	return curIno
}

// SetSnapshotRef sets the original snapshot identity for commit dedup.
func (fs *PassthroughFS) SetSnapshotRef(ref snapshotRef) {
	fs.mu.Lock()
	fs.origSnapshot = ref
	fs.mu.Unlock()
}

// snapshotGroupDir returns the local filesystem path for a backup group:
//
//	<pbsStore>/[ns/.../]<backup-type>/<backup-id>
func (fs *PassthroughFS) snapshotGroupDir(backupType, backupID, namespace string) string {
	parts := []string{fs.pbsStore}
	if namespace != "" {
		for comp := range strings.SplitSeq(namespace, "/") {
			if comp != "" {
				parts = append(parts, "ns", comp)
			}
		}
	}
	parts = append(parts, backupType, backupID)
	return filepath.Join(parts...)
}

// Close releases resources held by the filesystem.
func (fs *PassthroughFS) Close() {
	for _, d := range fs.mmapData {
		_ = munmap(d)
	}
	fs.mmapData = nil

	fs.fhmu.Lock()
	for _, fh := range fs.handles {
		_ = syscall.Close(fh.fd)
	}
	fs.handles = nil
	fs.fhmu.Unlock()

	if fs.txnLog != nil {
		_ = fs.txnLog.Close()
	}
}

// ResetState clears all passthrough inode/handle state after a pxar hotSwap.
func (fs *PassthroughFS) ResetState() {
	fs.fhmu.Lock()
	for _, fh := range fs.handles {
		_ = syscall.Close(fh.fd)
	}
	fs.handles = make(map[uint64]*passFh)
	fs.nextFh = 0
	fs.fhmu.Unlock()

	fs.mu.Lock()
	rootPath := fs.nodePaths[RootInode]
	rootBacked := fs.backed[RootInode]
	rootPxarDir := fs.pxarDir[RootInode]

	fs.nodePaths = make(map[uint64]string)
	fs.pathToIno = make(map[string]uint64)
	fs.backed = make(map[uint64]bool)
	fs.pxarDir = make(map[uint64]bool)
	fs.deletedPaths = make(map[string]bool)
	fs.metaOverlay = make(map[string]*metaOverride)

	if rootPath != "" {
		fs.nodePaths[RootInode] = rootPath
		fs.pathToIno[rootPath] = RootInode
	}
	fs.backed[RootInode] = rootBacked
	fs.pxarDir[RootInode] = rootPxarDir
	fs.mu.Unlock()

	fs.matMu.Lock()
	fs.materialize = nil
	fs.matMu.Unlock()

	if fs.txnLog != nil {
		_ = fs.txnLog.Clear()
	}
}

// parseOrigSnapshot extracts snapshot identity from a DIDX file path.
func ParseOrigSnapshot(pbsStore, ppxarDidx string) snapshotRef {
	rel := strings.TrimPrefix(ppxarDidx, pbsStore)
	rel = strings.TrimPrefix(rel, "/")
	parts := strings.Split(rel, "/")

	var ref snapshotRef
	if len(parts) >= 4 {
		filename := parts[len(parts)-1]
		ref.ArchiveName = strings.TrimSuffix(filename, ".didx")
		ref.ArchiveName = strings.TrimSuffix(ref.ArchiveName, ".mpxar")
		ref.ArchiveName = strings.TrimSuffix(ref.ArchiveName, ".ppxar")
		ref.ArchiveName = strings.TrimSuffix(ref.ArchiveName, ".pxar")

		_, _ = fmt.Sscanf(parts[len(parts)-2], "%d", &ref.BackupTime)
		ref.BackupID = parts[len(parts)-3]
		ref.BackupType = parts[len(parts)-4]
		if len(parts) > 4 {
			nsParts := parts[:len(parts)-4]
			var clean []string
			for i := 0; i < len(nsParts); i++ {
				if nsParts[i] == "ns" && i+1 < len(nsParts) {
					i++
					clean = append(clean, nsParts[i])
				}
			}
			ref.Namespace = strings.Join(clean, "/")
		}
	}
	if ref.BackupType == "" {
		ref.BackupType = "host"
	}
	if ref.ArchiveName == "" {
		ref.ArchiveName = ref.BackupID
	}
	return ref
}
