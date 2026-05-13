package main

import (
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

const backedInoBase uint64 = 1 << 60

// RENAME flags as defined in the Linux FUSE protocol (fuse_kernel.h).
const (
	renameNoReplace = 1 << 0
	renameExchange  = 1 << 1
)

// XATTR flags as defined in the Linux FUSE protocol.
const (
	xattrCreate  = 1
	xattrReplace = 2
)

// isACLXattr reports whether the xattr name is ACL-related.
// Covers Linux POSIX ACLs and Windows NT ACLs (SMB acl_xattr module).
func isACLXattr(attr string) bool {
	switch attr {
	case "system.posix_acl_access",
		"system.posix_acl_default",
		"security.NTACL",
		"security.XDACL",
		"security.ACL":
		return true
	}
	return strings.HasPrefix(attr, "system.posix_acl_") ||
		strings.HasPrefix(attr, "security.ntfs_") ||
		strings.HasPrefix(attr, "user.NTACL")
}

// passthroughFS implements fuse.RawFileSystem as an overlay:
//   - pxarFS provides the read-only lower layer
//   - backingDir provides the read-write upper layer
//
// Files from pxar are read-only. Files created/modified via the mount
// are written to backingDir. Listing merges both layers (backing wins).
//
// When mutationMode is enabled, pxar-backed entries can be mutated
// (renamed, deleted, written to). Mutations are recorded in a transaction
// log (txnLog) stored in a configurable directory. These transactions are
// applied during commit to build the new snapshot.
type passthroughFS struct {
	fuse.RawFileSystem
	nodePaths     map[uint64]string
	pathToIno     map[string]uint64 // reverse: relPath → inode for dedup
	pxar          *pxarFS
	handles       map[uint64]*passFh
	backed        map[uint64]bool // has a real path in backing dir
	pxarDir       map[uint64]bool // originated from pxar (directory)
	deletedPaths  map[string]bool // pxar paths deleted in mutation mode
	origSnapshot  snapshotRef
	pbsStore      string
	origPpxarDidx string
	backingDir    string
	nextBackIno   uint64
	nextFh        uint64
	mu            sync.RWMutex           // guards nodePaths, pathToIno, backed, pxarDir, deletedPaths, nextBackIno
	fhmu          sync.Mutex             // guards handles, nextFh
	matMu         sync.Mutex             // guards materialize map
	materialize   map[uint64]*sync.Mutex // per-inode serialization for materializePxarFile

	// Mutation mode: allows rename/delete/write of pxar-backed entries.
	// Transactions are recorded in txnLog and applied during commit.
	mutationMode bool
	txnLog       *TransactionLog
}

// joinPath builds a child path from a parent path and a name.
// Handles the root ("/") case without producing "//".
func joinPath(parent, name string) string {
	if parent == "/" {
		return "/" + name
	}
	return parent + "/" + name
}

type snapshotRef struct {
	BackupType  string
	BackupID    string
	Namespace   string
	ArchiveName string
	BackupTime  int64
}

// parseOrigSnapshot extracts snapshot identity from a DIDX file path.
// Path format: <store>/[ns/]<type>/<backup-id>/<backup-time>/<file>.ppxar.didx
func parseOrigSnapshot(pbsStore, ppxarDidx string) snapshotRef {
	rel := strings.TrimPrefix(ppxarDidx, pbsStore)
	rel = strings.TrimPrefix(rel, "/")
	parts := strings.Split(rel, "/")

	var ref snapshotRef
	if len(parts) >= 4 {
		// filename is last component: AKA---E.mpxar.didx or AKA---E.ppxar.didx
		filename := parts[len(parts)-1]
		// Strip .didx suffix, then .mpxar/.ppxar/.pxar to get the base archive name.
		// Example: AKA---E.mpxar.didx → AKA---E
		ref.ArchiveName = strings.TrimSuffix(filename, ".didx")
		ref.ArchiveName = strings.TrimSuffix(ref.ArchiveName, ".mpxar")
		ref.ArchiveName = strings.TrimSuffix(ref.ArchiveName, ".ppxar")
		ref.ArchiveName = strings.TrimSuffix(ref.ArchiveName, ".pxar")

		// backup-time is second-to-last
		_, _ = fmt.Sscanf(parts[len(parts)-2], "%d", &ref.BackupTime)
		// backup-id is third-to-last
		ref.BackupID = parts[len(parts)-3]
		// backup-type is fourth-to-last
		ref.BackupType = parts[len(parts)-4]
		// everything before that is namespace (with ns/ prefix per segment)
		// e.g. "ns/test/ns/sgprog" → namespace "test/sgprog"
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

type passFh struct {
	fd    int
	inode uint64
}

// --- initialization ---

func (fs *passthroughFS) Init(server *fuse.Server) {
	fs.RawFileSystem = fuse.NewDefaultRawFileSystem()
	fs.RawFileSystem.Init(server)
}

func (fs *passthroughFS) String() string {
	return "pxar-passthrough"
}

func (fs *passthroughFS) SetDebug(dbg bool) {}

// --- helpers ---

func (fs *passthroughFS) absPath(rel string) string {
	return filepath.Join(fs.backingDir, rel)
}

func (fs *passthroughFS) isBacked(ino uint64) bool {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.backed[ino]
}

func (fs *passthroughFS) nodePath(ino uint64) string {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.nodePaths[ino]
}

func (fs *passthroughFS) setNode(ino uint64, relPath string, backed bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	// If this path already has a different inode, clean up the old one.
	if oldIno, exists := fs.pathToIno[relPath]; exists && oldIno != ino {
		delete(fs.nodePaths, oldIno)
		delete(fs.backed, oldIno)
	}
	fs.nodePaths[ino] = relPath
	fs.pathToIno[relPath] = ino
	fs.backed[ino] = backed
}

func (fs *passthroughFS) allocBackedIno(isDir bool) uint64 {
	fs.mu.Lock()
	ino := fs.nextBackIno + backedInoBase
	fs.nextBackIno++
	fs.mu.Unlock()
	if !isDir {
		ino |= nonDirBit
	}
	return ino
}

// lookupOrAllocIno atomically returns the existing inode for relPath, or
// allocates a new one. This prevents concurrent goroutines from creating
// duplicate inodes for the same backing file.
func (fs *passthroughFS) lookupOrAllocIno(relPath string, isDir bool) (ino uint64, allocated bool) {
	fs.mu.Lock()
	if existing, ok := fs.pathToIno[relPath]; ok {
		fs.mu.Unlock()
		return existing, false
	}
	ino = fs.nextBackIno + backedInoBase
	fs.nextBackIno++
	if !isDir {
		ino |= nonDirBit
	}
	fs.pathToIno[relPath] = ino
	fs.nodePaths[ino] = relPath
	fs.mu.Unlock()
	return ino, true
}

// ensureBackingParent creates parent directories in backingDir for the
// given relative path so that the file itself can be created.
func (fs *passthroughFS) ensureBackingParent(rel string) error {
	parent := filepath.Dir(rel)
	if parent == "." || parent == "/" {
		return nil
	}
	return os.MkdirAll(fs.absPath(parent), 0o755)
}

// initPassthroughRoot initializes the root directory in the backing dir.
// Subdirectories are created lazily on first access (Lookup / ReadDir)
// via ensureDirBacking, rather than eagerly walking the entire pxar tree.
func (fs *passthroughFS) initPassthroughRoot() error {
	if err := os.MkdirAll(fs.backingDir, 0o755); err != nil {
		return err
	}
	fs.setNode(rootInode, "/", true)
	fs.mu.Lock()
	fs.pxarDir[rootInode] = true
	fs.mu.Unlock()
	return nil
}

// ensureDirBacking lazily creates a pxar-originated directory on disk in
// the backing filesystem. This is called when a directory is first accessed
// (via Lookup or ReadDir), avoiding the need to walk the entire archive tree
// at init time. MkdirAll ensures all ancestor directories exist as well.
func (fs *passthroughFS) ensureDirBacking(ino uint64, relPath string) {
	absPath := fs.absPath(relPath)
	if err := os.MkdirAll(absPath, 0o755); err != nil {
		return
	}
	fs.setNode(ino, relPath, true)
	fs.mu.Lock()
	fs.pxarDir[ino] = true
	fs.mu.Unlock()
}

// statBacked stat's a relative path in the backing dir and constructs
// backing node metadata. Returns nil if the file does not exist.
// Uses lookupOrAllocIno to avoid allocating duplicate inodes.
func (fs *passthroughFS) statBacked(rel string) (*node, error) {
	var st syscall.Stat_t
	if err := syscall.Lstat(fs.absPath(rel), &st); err != nil {
		return nil, err
	}
	mode := uint64(st.Mode)
	ino, _ := fs.lookupOrAllocIno(rel, mode&syscall.S_IFDIR != 0)
	nd := &node{
		inode:     ino,
		parent:    rootInode, // filled by caller if needed
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

// handleForNode returns the passFh for an open file handle, or nil.
func (fs *passthroughFS) handleForNode(nodeID, fhID uint64) *passFh {
	fs.fhmu.Lock()
	defer fs.fhmu.Unlock()
	fh, ok := fs.handles[fhID]
	if !ok || fh.inode != nodeID {
		return nil
	}
	return fh
}

// registerFh opens a backed file and returns a handle ID.
func (fs *passthroughFS) registerFh(rel string, nodeID uint64, flags int) (uint64, error) {
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

// getPxarNode safely reads a node from the pxar node cache.
// Returns a heap-allocated copy — use only when pointer is needed for FUSE APIs.
func (fs *passthroughFS) getPxarNode(ino uint64) *node {
	fs.pxar.mu.RLock()
	defer fs.pxar.mu.RUnlock()
	n, ok := fs.pxar.nodes[ino]
	if !ok {
		return nil
	}
	return &n
}

// nodeFromStat creates a minimal node from a syscall.Stat_t for use with
// fillEntryOut / fillAttrOut. The inode field is set by the caller.
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

// isPxarBacked returns true if the node (by inode) originated from the pxar
// archive. Pxar-backed nodes reject ALL mutating FS operations, including
// xattr changes — the archive is fully immutable.
func (fs *passthroughFS) isPxarBacked(ino uint64) bool {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.pxarDir[ino]
}

// isPxarChild checks whether any pxar entry (file, dir, symlink, etc.) with
// the given name exists under the parent directory. Used to prevent
// deletion/rename of archive entries.
func (fs *passthroughFS) isPxarChild(parentIno uint64, name string) bool {
	entries, err := fs.pxar.readDirRaw(parentIno)
	if err != nil {
		// Node may have been evicted — try recovery via path
		parentPath := fs.nodePath(parentIno)
		if parentPath != "" {
			if recovered := fs.recoverPxarDirNode(parentPath); recovered != 0 {
				fs.setNode(recovered, parentPath, true)
				fs.mu.Lock()
				fs.pxarDir[recovered] = true
				fs.mu.Unlock()
				if entries2, err2 := fs.pxar.readDirRaw(recovered); err2 == nil {
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

// isPathDeleted checks whether a path has been marked as deleted
// (via unlink/rmdir in mutation mode).
func (fs *passthroughFS) isPathDeleted(relPath string) bool {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.deletedPaths[relPath]
}

// markPathDeleted records a path as deleted and records a DELETE transaction.
// It is idempotent: if the path is already deleted, no duplicate transaction
// is recorded.
func (fs *passthroughFS) markPathDeleted(relPath string) {
	fs.mu.Lock()
	if fs.deletedPaths[relPath] {
		fs.mu.Unlock()
		return // already deleted
	}
	fs.deletedPaths[relPath] = true
	fs.mu.Unlock()
	if fs.txnLog != nil {
		_, _ = fs.txnLog.Record(TxnDelete, relPath)
	}
}

// unDeletePath removes a path from the deleted set, allowing it to
// appear in Lookup/ReadDir again. Called when a new file/dir is created
// over a previously-deleted pxar path.
func (fs *passthroughFS) unDeletePath(relPath string) {
	fs.mu.Lock()
	delete(fs.deletedPaths, relPath)
	fs.mu.Unlock()
}

// materializePxarFile copies a pxar-backed file to the backing directory
// so it can be modified. The node is then marked as backed.
// Returns the relative path and nil on success.
// Concurrent calls for the same inode are serialized to prevent data corruption
// from O_TRUNC racing with concurrent writes.
func (fs *passthroughFS) materializePxarFile(ino uint64) (string, error) {
	// Serialize per-inode to prevent concurrent materialization.
	inoMu := fs.getInoMu(ino)
	inoMu.Lock()
	defer inoMu.Unlock()

	return fs.materializePxarFileLocked(ino)
}

// getInoMu returns a per-inode mutex used to serialize materialization.
func (fs *passthroughFS) getInoMu(ino uint64) *sync.Mutex {
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

func (fs *passthroughFS) materializePxarFileLocked(ino uint64) (string, error) {
	relPath := fs.nodePath(ino)
	if relPath == "" {
		return "", syscall.ENOENT
	}

	// Check if already materialized
	if fs.isBacked(ino) {
		abs := fs.absPath(relPath)
		if _, err := os.Lstat(abs); err == nil {
			return relPath, nil
		}
	}

	// Read the pxar entry
	fs.pxar.mu.RLock()
	n, ok := fs.pxar.nodes[ino]
	fs.pxar.mu.RUnlock()
	if !ok {
		return "", syscall.ENOENT
	}

	if n.isDir {
		// Directories are already materialized via ensureDirBacking
		return relPath, nil
	}

	// Create parent directories
	if err := fs.ensureBackingParent(relPath); err != nil {
		return "", err
	}

	abs := fs.absPath(relPath)

	if n.isSymlink {
		// Read symlink target from pxar
		fs.pxar.readerMu.Lock()
		entry, err := fs.pxar.readEntryForNode(&n)
		fs.pxar.readerMu.Unlock()
		if err != nil {
			return "", err
		}
		if err := syscall.Symlink(entry.LinkTarget, abs); err != nil {
			return "", err
		}
	} else if n.isReg {
		// Copy file content from pxar to backing dir
		fs.pxar.readerMu.Lock()
		entry, err := fs.pxar.readEntryForNode(&n)
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

		if _, err := io.Copy(f, rc); err != nil {
			return "", err
		}

		// Preserve original permissions
		if err := os.Chmod(abs, os.FileMode(statMode(n.mode)&0o7777)); err != nil {
			return "", err
		}
	}

	// Mark as backed
	ino2, _ := fs.lookupOrAllocIno(relPath, n.isDir)
	fs.setNode(ino2, relPath, true)

	// Record MODIFY transaction
	if fs.txnLog != nil {
		_, _ = fs.txnLog.Record(TxnModify, relPath)
	}

	return relPath, nil
}

// materializePxarDir ensures a pxar directory is materialized in the
// backing directory and marked as backed.
func (fs *passthroughFS) materializePxarDir(ino uint64, relPath string) {
	fs.ensureDirBacking(ino, relPath)
}

// --- Lookup ---

func (fs *passthroughFS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	parentPath := fs.nodePath(header.NodeId)
	childPath := joinPath(parentPath, name)

	// In mutation mode, hide entries that have been deleted.
	if fs.mutationMode && fs.isPathDeleted(childPath) {
		return fuse.ENOENT
	}

	// Check backing dir first (upper layer takes priority)
	backedNode, _ := fs.statBacked(childPath)

	// For directories, prefer the pxar inode so that readDirRaw works.
	// A backed directory created by ensureBackingParent (empty) should
	// not shadow the pxar directory with actual content.
	if backedNode != nil && backedNode.isDir {
		// Try pxar lookup for the same name
		if pxarSt := fs.pxar.Lookup(cancel, header, name, out); pxarSt == fuse.OK {
			// Found pxar directory — use its inode but mark as backed
			// so file creation still works in this directory.
			fs.setNode(out.NodeId, childPath, true)
			fs.mu.Lock()
			fs.pxarDir[out.NodeId] = true
			fs.mu.Unlock()
			return fuse.OK
		}
		// pxar.Lookup failed — the parent node may have been evicted.
		// Try to recover by walking the pxar tree from root.
		if pxarIno := fs.recoverPxarDirNode(childPath); pxarIno != 0 {
			fs.setNode(pxarIno, childPath, true)
			fs.mu.Lock()
			fs.pxarDir[pxarIno] = true
			fs.mu.Unlock()
			// Use the recovered pxar inode
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

	// Delegate to pxar
	st := fs.pxar.Lookup(cancel, header, name, out)
	if st == fuse.OK {
		if out.Mode&syscall.S_IFDIR != 0 {
			// Lazily create the backing directory on first access
			// so SMB's acl_xattr can store/retrieve ACLs.
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

// --- GetAttr / SetAttr ---

func (fs *passthroughFS) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
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
	return fs.pxar.GetAttr(cancel, input, out)
}

func (fs *passthroughFS) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {
	if !fs.isBacked(input.NodeId) {
		if !fs.mutationMode {
			return fuse.EROFS
		}
		// In mutation mode: materialize the pxar file so we can setattr on it.
		if _, err := fs.materializePxarFile(input.NodeId); err != nil {
			return fuse.ToStatus(err)
		}
	}
	// Pxar-backed non-directories are immutable in standard mode.
	if fs.isPxarBacked(input.NodeId) && !fs.mutationMode {
		n := fs.getPxarNode(input.NodeId)
		if n == nil || !n.isDir {
			return fuse.EPERM
		}
	}

	// Record SETATTR transaction if in mutation mode and this is a pxar-backed entry.
	if fs.mutationMode && fs.isPxarBacked(input.NodeId) && fs.txnLog != nil {
		rel := fs.nodePath(input.NodeId)
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
		if hasAttr {
			_, _ = fs.txnLog.RecordSetAttr(rel, attrs)
		}
	}

	rel := fs.nodePath(input.NodeId)
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

	return fs.GetAttr(cancel, &fuse.GetAttrIn{InHeader: input.InHeader}, out)
}

// --- Create / Mkdir / Mknod / Symlink ---

func (fs *passthroughFS) Create(cancel <-chan struct{}, input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	parentPath := fs.nodePath(input.NodeId)
	childPath := joinPath(parentPath, name)

	// Un-delete the path if it was previously a deleted pxar entry.
	if fs.mutationMode {
		fs.unDeletePath(childPath)
	}

	if err := fs.ensureBackingParent(childPath); err != nil {
		return fuse.ToStatus(err)
	}

	abs := fs.absPath(childPath)

	// Try atomic create first.
	flags := int(input.Flags) | os.O_CREATE | os.O_EXCL
	fd, err := syscall.Open(abs, flags, uint32(input.Mode&0o777))
	if err == nil {
		// We won the O_EXCL race. This inode is ours.
		ino, _ := fs.lookupOrAllocIno(childPath, false)
		fs.setNode(ino, childPath, true)
		return fs.finishCreate(ino, fd, out)
	}

	// O_EXCL failed — file exists. Open without O_EXCL and reuse inode.
	ino, _ := fs.lookupOrAllocIno(childPath, false)
	fs.setNode(ino, childPath, true)
	fd, err = syscall.Open(abs, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		return fuse.ToStatus(err)
	}
	return fs.finishCreate(ino, fd, out)
}

// finishCreate populates the CreateOut response and registers a file handle.
func (fs *passthroughFS) finishCreate(ino uint64, fd int, out *fuse.CreateOut) fuse.Status {
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

func (fs *passthroughFS) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) fuse.Status {
	parentPath := fs.nodePath(input.NodeId)
	childPath := joinPath(parentPath, name)

	if fs.mutationMode {
		fs.unDeletePath(childPath)
	}

	if err := fs.ensureBackingParent(childPath); err != nil {
		return fuse.ToStatus(err)
	}

	abs := fs.absPath(childPath)
	if err := syscall.Mkdir(abs, input.Mode&0o777); err != nil {
		if err == syscall.EEXIST {
			// Created by another goroutine; reuse existing inode.
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
		return fuse.ToStatus(err)
	}

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

func (fs *passthroughFS) Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) fuse.Status {
	parentPath := fs.nodePath(input.NodeId)
	childPath := joinPath(parentPath, name)

	if fs.mutationMode {
		fs.unDeletePath(childPath)
	}

	if err := fs.ensureBackingParent(childPath); err != nil {
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
		return fuse.ToStatus(err)
	}

	ino := fs.allocBackedIno(isDir)
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

func (fs *passthroughFS) Symlink(cancel <-chan struct{}, header *fuse.InHeader, target string, linkName string, out *fuse.EntryOut) fuse.Status {
	parentPath := fs.nodePath(header.NodeId)
	childPath := joinPath(parentPath, linkName)

	if fs.mutationMode {
		fs.unDeletePath(childPath)
	}

	if err := fs.ensureBackingParent(childPath); err != nil {
		return fuse.ToStatus(err)
	}

	abs := fs.absPath(childPath)
	if err := syscall.Symlink(target, abs); err != nil {
		if err == syscall.EEXIST {
			// Created by another goroutine; reuse existing inode.
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

// --- Unlink / Rmdir ---

// removeEntry is the shared implementation for Unlink and Rmdir.
func (fs *passthroughFS) removeEntry(parentIno uint64, name string, removeFn func(string) error) fuse.Status {
	parentPath := fs.nodePath(parentIno)
	childPath := joinPath(parentPath, name)

	isPxar := fs.isPxarChild(parentIno, name)

	if isPxar && !fs.mutationMode {
		return fuse.EPERM
	}

	if isPxar && fs.mutationMode {
		// In mutation mode: record deletion and mark path as deleted.
		// The pxar entry still exists in the archive but is hidden.
		fs.markPathDeleted(childPath)

		// If the entry was materialized in the backing dir, remove it.
		abs := fs.absPath(childPath)
		if _, err := os.Lstat(abs); err == nil {
			_ = removeFn(abs)
		}

		// Clean up inode mappings.
		fs.mu.Lock()
		if ino, ok := fs.pathToIno[childPath]; ok {
			delete(fs.nodePaths, ino)
			delete(fs.backed, ino)
			delete(fs.pxarDir, ino)
			delete(fs.pathToIno, childPath)
		}
		fs.mu.Unlock()

		return fuse.OK
	}

	// Non-pxar entry: standard behavior
	abs := fs.absPath(childPath)

	if _, err := os.Lstat(abs); err != nil {
		return fuse.EROFS
	}
	if err := removeFn(abs); err != nil {
		return fuse.ToStatus(err)
	}

	// Clean up inode mappings for the removed entry.
	fs.mu.Lock()
	if ino, ok := fs.pathToIno[childPath]; ok {
		delete(fs.nodePaths, ino)
		delete(fs.backed, ino)
		delete(fs.pxarDir, ino)
		delete(fs.pathToIno, childPath)
	}
	fs.mu.Unlock()

	return fuse.OK
}

func (fs *passthroughFS) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	return fs.removeEntry(header.NodeId, name, syscall.Unlink)
}

func (fs *passthroughFS) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	return fs.removeEntry(header.NodeId, name, syscall.Rmdir)
}

// renamePaths updates the internal path→inode mappings after a successful
// filesystem rename. It moves the inode from oldPath to newPath.
func (fs *passthroughFS) renamePaths(oldPath, newPath string) {
	fs.mu.Lock()
	ino, ok := fs.pathToIno[oldPath]
	if ok {
		// Save the old destination inode BEFORE overwriting.
		oldDstIno, hadDst := fs.pathToIno[newPath]

		delete(fs.pathToIno, oldPath)
		fs.pathToIno[newPath] = ino
		fs.nodePaths[ino] = newPath

		// If the destination had an existing file that was overwritten by rename,
		// its inode is now stale — clean it up.
		if hadDst && oldDstIno != ino {
			delete(fs.nodePaths, oldDstIno)
			delete(fs.backed, oldDstIno)
			delete(fs.pxarDir, oldDstIno)
		}
	}
	fs.mu.Unlock()
}

// --- Rename ---

func (fs *passthroughFS) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName string, newName string) fuse.Status {
	oldIsPxar := fs.isPxarChild(input.NodeId, oldName)
	newIsPxar := fs.isPxarChild(input.Newdir, newName)

	if !fs.mutationMode {
		// Standard mode: pxar entries cannot be renamed/moved.
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

	// Un-delete the destination path if it was previously deleted.
	// This handles rename-to-deleted-path correctly.
	if fs.mutationMode {
		fs.unDeletePath(newPath)
	}

	if fs.mutationMode {
		if oldIsPxar {
			// Renaming a pxar entry: materialize it first, then rename.
			// Find the pxar inode for the old entry.
			if entries, err := fs.pxar.readDirRaw(input.NodeId); err == nil {
				for _, e := range entries {
					if e.name == oldName {
						fs.pxar.registerSlimNode(&e, input.NodeId)
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

		if newIsPxar {
			// Overwriting a pxar destination: mark it as deleted.
			fs.markPathDeleted(newPath)
		}
	}

	oldAbs := fs.absPath(oldPath)

	// Source must exist in backing dir
	if _, err := os.Lstat(oldAbs); err != nil {
		return fuse.EROFS
	}

	// Ensure target parent exists
	if err := fs.ensureBackingParent(newPath); err != nil {
		return fuse.ToStatus(err)
	}

	newAbs := fs.absPath(newPath)

	if input.Flags&renameNoReplace != 0 {
		if _, err := os.Lstat(newAbs); err == nil {
			return fuse.Status(syscall.EEXIST)
		}
		if err := os.Rename(oldAbs, newAbs); err != nil {
			return fuse.ToStatus(err)
		}
		fs.renamePaths(oldPath, newPath)

		// Record RENAME transaction for pxar entries
		if oldIsPxar && fs.txnLog != nil {
			_, _ = fs.txnLog.RecordRename(oldPath, newPath)
		}
		return fuse.OK
	}

	if input.Flags&renameExchange != 0 {
		if err := unix.Renameat2(unix.AT_FDCWD, oldAbs, unix.AT_FDCWD, newAbs, unix.RENAME_EXCHANGE); err != nil {
			return fuse.ToStatus(err)
		}
		fs.renamePaths(oldPath, newPath)

		if oldIsPxar && fs.txnLog != nil {
			_, _ = fs.txnLog.RecordRename(oldPath, newPath)
		}
		return fuse.OK
	}

	// Default: rename with potential overwrite of existing backing file.
	if err := os.Rename(oldAbs, newAbs); err != nil {
		return fuse.ToStatus(err)
	}
	fs.renamePaths(oldPath, newPath)

	// Record RENAME transaction for pxar entries
	if oldIsPxar && fs.txnLog != nil {
		_, _ = fs.txnLog.RecordRename(oldPath, newPath)
	}
	return fuse.OK
}

// --- Link ---

func (fs *passthroughFS) Link(cancel <-chan struct{}, input *fuse.LinkIn, name string, out *fuse.EntryOut) fuse.Status {
	return fuse.ENOSYS
}

// --- Open / Read / Write / Flush / Fsync / Release ---

func (fs *passthroughFS) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	if !fs.isBacked(input.NodeId) {
		// Not backed — check if this is a write open in mutation mode.
		if fs.mutationMode && input.Flags&(uint32(syscall.O_WRONLY|syscall.O_RDWR)) != 0 {
			// Materialize the pxar file so it can be opened for writing.
			if _, err := fs.materializePxarFile(input.NodeId); err != nil {
				return fuse.ToStatus(err)
			}
			// Fall through to backed open below.
		} else {
			return fs.pxar.Open(cancel, input, out)
		}
	}

	// Pxar-backed entries are read-only in standard mode.
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

func (fs *passthroughFS) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
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

func (fs *passthroughFS) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (uint32, fuse.Status) {
	if !fs.isBacked(input.NodeId) {
		if !fs.mutationMode {
			return 0, fuse.EROFS
		}
		// In mutation mode: materialize the pxar file on first write.
		// This should already be done via Open(), but handle edge cases.
		if _, err := fs.materializePxarFile(input.NodeId); err != nil {
			return 0, fuse.ToStatus(err)
		}
	}
	if fs.isPxarBacked(input.NodeId) && !fs.mutationMode {
		return 0, fuse.EROFS
	}

	fh := fs.handleForNode(input.NodeId, input.Fh)
	if fh == nil {
		return 0, fuse.EBADF
	}

	n, err := syscall.Pwrite(fh.fd, data, int64(input.Offset))
	if err != nil {
		return 0, fuse.ToStatus(err)
	}
	return uint32(n), fuse.OK
}

func (fs *passthroughFS) fsyncBacked(nodeID, fhID uint64) fuse.Status {
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

func (fs *passthroughFS) Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status {
	return fs.fsyncBacked(input.NodeId, input.Fh)
}

func (fs *passthroughFS) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	return fs.fsyncBacked(input.NodeId, input.Fh)
}

func (fs *passthroughFS) Fallocate(cancel <-chan struct{}, input *fuse.FallocateIn) fuse.Status {
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
		return fuse.EBADF
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

func (fs *passthroughFS) Release(cancel <-chan struct{}, input *fuse.ReleaseIn) {
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

func (fs *passthroughFS) Lseek(cancel <-chan struct{}, in *fuse.LseekIn, out *fuse.LseekOut) fuse.Status {
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

func (fs *passthroughFS) Readlink(cancel <-chan struct{}, header *fuse.InHeader) ([]byte, fuse.Status) {
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

// --- OpenDir / ReadDir / ReadDirPlus ---

func (fs *passthroughFS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	return fuse.OK
}

func (fs *passthroughFS) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	return fs.readDirImpl(cancel, input, out, false)
}

func (fs *passthroughFS) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	return fs.readDirImpl(cancel, input, out, true)
}

func (fs *passthroughFS) readDirImpl(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList, plus bool) fuse.Status {
	parentPath := fs.nodePath(input.NodeId)

	// Resolve the actual pxar inode to use for reading directory entries.
	// input.NodeId may be a backed inode (from allocBackedIno) that has no
	// pxar node registered. In that case, recover the pxar inode from the
	// archive by walking from root.
	dirIno := input.NodeId
	if _, err := fs.pxar.readDirRaw(dirIno); err != nil && parentPath != "" {
		if recovered := fs.recoverPxarDirNode(parentPath); recovered != 0 {
			fs.setNode(recovered, parentPath, true)
			fs.mu.Lock()
			fs.pxarDir[recovered] = true
			fs.mu.Unlock()
			dirIno = recovered
		}
	}

	// Read pxar entries using the resolved inode.
	var pxarEntries []dirEntrySlim
	if pxarN, err := fs.pxar.readDirRaw(dirIno); err == nil {
		pxarEntries = pxarN
	}

	// Read backing dir entries using lookupOrAllocIno for inode stability.
	var backedEntries []dirEntrySlim
	localStats := make(map[uint64]syscall.Stat_t) // per-call, no lock needed
	if parentPath != "" {
		absParent := fs.absPath(parentPath)
		des, err := os.ReadDir(absParent)
		if err == nil && len(des) > 0 {
			backedEntries = make([]dirEntrySlim, 0, len(des))
			for _, de := range des {
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

	// Merge: backed entries shadow pxar entries
	backedName := make(map[string]bool, len(backedEntries))
	for _, be := range backedEntries {
		backedName[be.name] = true
	}

	// Register pxar nodes so fillEntryOutForNode can find them.
	// For directories, lazily create them in the backing filesystem.
	for i := range pxarEntries {
		pe := &pxarEntries[i]
		fs.pxar.registerSlimNode(pe, input.NodeId)
		childPath := joinPath(parentPath, pe.name)

		// In mutation mode, skip entries that have been deleted.
		if fs.mutationMode && fs.isPathDeleted(childPath) {
			continue
		}

		if isDirInode(pe.inode) {
			// Lazily create the backing directory on first ReadDir
			// so SMB's acl_xattr can store/retrieve ACLs.
			fs.ensureDirBacking(pe.inode, childPath)
		}
	}

	entries := make([]dirEntrySlim, 0, len(pxarEntries)+len(backedEntries))
	for _, pe := range pxarEntries {
		childPath := joinPath(parentPath, pe.name)
		// Skip deleted entries in mutation mode
		if fs.mutationMode && fs.isPathDeleted(childPath) {
			continue
		}
		if !backedName[pe.name] {
			entries = append(entries, pe)
			if !isDirInode(pe.inode) {
				fs.setNode(pe.inode, childPath, false)
			}
		} else if isDirInode(pe.inode) {
			// Pxar directory shadowed by a backed directory (e.g. from a
			// previous lazy creation). Keep the pxar inode so readDirRaw
			// works, but mark as backed so file creation works.
			entries = append(entries, pe)
			fs.setNode(pe.inode, childPath, true)
			backedName[pe.name] = false // suppress the backed version
		}
	}
	for _, be := range backedEntries {
		if backedName[be.name] { // false = suppressed (pxar dir took precedence)
			entries = append(entries, be)
		}
	}

	// "." entry
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

	// ".." entry
	if input.Offset <= 1 {
		parentIno := rootInode
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

	// Child entries
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

func (fs *passthroughFS) dirModeForNode(ino uint64) uint32 {
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

// fillEntryOutForNodeWithStats uses a per-call local stats map instead of a
// shared field, eliminating the need for any locking during concurrent ReadDir calls.
func (fs *passthroughFS) fillEntryOutForNodeWithStats(ino uint64, out *fuse.EntryOut, localStats map[uint64]syscall.Stat_t) {
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

func (fs *passthroughFS) ReleaseDir(input *fuse.ReleaseIn) {}
func (fs *passthroughFS) FsyncDir(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	return fuse.OK
}

// --- Access ---

func (fs *passthroughFS) Access(cancel <-chan struct{}, input *fuse.AccessIn) fuse.Status {
	if fs.isBacked(input.NodeId) {
		return fuse.OK
	}
	return fs.pxar.Access(cancel, input)
}

// resetState clears all passthrough inode/handle state after a pxar hotSwap.
// This must be called after pxar.hotSwap() and after the backing dir has
// been cleaned up and recreated. It preserves only the root inode mapping.
func (fs *passthroughFS) resetState() {
	// Close any open file handles
	fs.fhmu.Lock()
	for _, fh := range fs.handles {
		_ = syscall.Close(fh.fd)
	}
	fs.handles = make(map[uint64]*passFh)
	fs.nextFh = 0
	fs.fhmu.Unlock()

	// Reset all inode maps, preserving root
	fs.mu.Lock()
	rootPath := fs.nodePaths[rootInode]
	rootBacked := fs.backed[rootInode]
	rootPxarDir := fs.pxarDir[rootInode]

	fs.nodePaths = make(map[uint64]string)
	fs.pathToIno = make(map[string]uint64)
	fs.backed = make(map[uint64]bool)
	fs.pxarDir = make(map[uint64]bool)
	fs.deletedPaths = make(map[string]bool)

	// Re-register root
	if rootPath != "" {
		fs.nodePaths[rootInode] = rootPath
		fs.pathToIno[rootPath] = rootInode
	}
	fs.backed[rootInode] = rootBacked
	fs.pxarDir[rootInode] = rootPxarDir
	fs.mu.Unlock()

	// Clear per-inode materialization locks
	fs.matMu.Lock()
	fs.materialize = nil
	fs.matMu.Unlock()

	// Clear the transaction log if present.
	if fs.txnLog != nil {
		_ = fs.txnLog.Clear()
	}
}

// --- xattr ---

// recoverPxarDirNode attempts to recover the pxar inode for a directory
// whose node was evicted from the pxar node cache. It walks up the path
// to find an ancestor still in cache, then re-reads directory entries
// down to the target directory, re-registering all nodes along the way.
// Returns the recovered pxar inode, or 0 if recovery fails.
func (fs *passthroughFS) recoverPxarDirNode(relPath string) uint64 {
	if relPath == "" || relPath == "/" {
		return rootInode
	}

	// Build the path components: ["", "2022-PROJECTS", "USGS__22.PA"]
	parts := strings.Split(relPath, "/")

	// Find the deepest ancestor still registered in pxar.nodes.
	// Start from root (always in cache after hotSwap or init) and walk down.
	curIno := rootInode
	for i := 1; i < len(parts); i++ {
		name := parts[i]
		if name == "" {
			continue
		}

		// Try to read this level's directory entries
		entries, err := fs.pxar.readDirRaw(curIno)
		if err != nil {
			return 0 // can't read parent
		}

		found := false
		for _, e := range entries {
			if e.name == name {
				fs.pxar.registerSlimNode(&e, curIno)
				if isDirInode(e.inode) {
					curIno = e.inode
					found = true
				} else {
					return e.inode // it's a file, not a dir
				}
				break
			}
		}
		if !found {
			return 0 // name not found in parent
		}
	}
	return curIno
}

func (fs *passthroughFS) GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, dest []byte) (uint32, fuse.Status) {
	if fs.isBacked(header.NodeId) {
		rel := fs.nodePath(header.NodeId)
		sz, err := unix.Getxattr(fs.absPath(rel), attr, dest)
		if err != nil {
			return 0, fuse.ToStatus(err)
		}
		return uint32(sz), fuse.OK
	}

	return fs.pxar.GetXAttr(cancel, header, attr, dest)
}

func (fs *passthroughFS) ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (uint32, fuse.Status) {
	if fs.isBacked(header.NodeId) {
		rel := fs.nodePath(header.NodeId)
		sz, err := unix.Listxattr(fs.absPath(rel), dest)
		if err != nil {
			return 0, fuse.ToStatus(err)
		}
		return uint32(sz), fuse.OK
	}

	return fs.pxar.ListXAttr(cancel, header, dest)
}

func (fs *passthroughFS) SetXAttr(cancel <-chan struct{}, input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	if isACLXattr(attr) {
		return fs.setACLXAttr(input, attr, data)
	}

	if !fs.isBacked(input.NodeId) {
		if !fs.mutationMode {
			return fuse.EROFS
		}
		// In mutation mode: materialize the pxar file so xattr can be set.
		if _, err := fs.materializePxarFile(input.NodeId); err != nil {
			return fuse.ToStatus(err)
		}
	}
	// Pxar-backed entries are immutable in standard mode.
	if fs.isPxarBacked(input.NodeId) && !fs.mutationMode {
		return fuse.EPERM
	}

	flags := 0
	if input.Flags&xattrCreate != 0 {
		flags = unix.XATTR_CREATE
	} else if input.Flags&xattrReplace != 0 {
		flags = unix.XATTR_REPLACE
	}

	rel := fs.nodePath(input.NodeId)
	if err := unix.Setxattr(fs.absPath(rel), attr, data, flags); err != nil {
		return fuse.ToStatus(err)
	}
	return fuse.OK
}

// setACLXAttr handles ACL xattr writes with per-node-type semantics:
//   - Transparent-backed files/dirs: apply normally to the backing filesystem.
//   - Pxar-backed directories: apply to the materialized directory in the
//     backing overlay so the ACL is visible in the mount.
//   - Pxar-backed files: accept silently (return OK) without persisting.
func (fs *passthroughFS) setACLXAttr(input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	if !fs.isBacked(input.NodeId) {
		// Pxar-backed file (no real path): accept the ACL change silently.
		return fuse.OK
	}

	// Transparent-backed or pxar-backed directory (both have real paths):
	// apply the ACL to the backing filesystem.
	flags := 0
	if input.Flags&xattrCreate != 0 {
		flags = unix.XATTR_CREATE
	} else if input.Flags&xattrReplace != 0 {
		flags = unix.XATTR_REPLACE
	}

	rel := fs.nodePath(input.NodeId)
	if err := unix.Setxattr(fs.absPath(rel), attr, data, flags); err != nil {
		return fuse.ToStatus(err)
	}
	return fuse.OK
}

func (fs *passthroughFS) RemoveXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string) fuse.Status {
	if isACLXattr(attr) {
		return fs.removeACLXAttr(header, attr)
	}

	if !fs.isBacked(header.NodeId) {
		if !fs.mutationMode {
			return fuse.EROFS
		}
		// In mutation mode: materialize the pxar file so xattr can be removed.
		if _, err := fs.materializePxarFile(header.NodeId); err != nil {
			return fuse.ToStatus(err)
		}
	}
	// Pxar-backed entries are immutable in standard mode.
	if fs.isPxarBacked(header.NodeId) && !fs.mutationMode {
		return fuse.EPERM
	}

	rel := fs.nodePath(header.NodeId)
	if err := unix.Removexattr(fs.absPath(rel), attr); err != nil {
		return fuse.ToStatus(err)
	}
	return fuse.OK
}

// removeACLXAttr handles ACL xattr removals with the same semantics as
// setACLXAttr: accept for pxar-backed files, apply for everything else
// that has a real backing path.
func (fs *passthroughFS) removeACLXAttr(header *fuse.InHeader, attr string) fuse.Status {
	if !fs.isBacked(header.NodeId) {
		// Pxar-backed file: accept silently.
		return fuse.OK
	}

	rel := fs.nodePath(header.NodeId)
	if err := unix.Removexattr(fs.absPath(rel), attr); err != nil {
		return fuse.ToStatus(err)
	}
	return fuse.OK
}

// --- StatFs ---

func (fs *passthroughFS) StatFs(cancel <-chan struct{}, header *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
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

func (fs *passthroughFS) Statx(cancel <-chan struct{}, input *fuse.StatxIn, out *fuse.StatxOut) fuse.Status {
	return fuse.ENOSYS
}

// --- Forget ---

func (fs *passthroughFS) Forget(nodeID, nlookup uint64) {
	fs.mu.Lock()
	if fs.backed[nodeID] {
		if path, ok := fs.nodePaths[nodeID]; ok {
			delete(fs.pathToIno, path)
		}
		delete(fs.backed, nodeID)
		delete(fs.nodePaths, nodeID)
	}
	fs.mu.Unlock()
	fs.pxar.Forget(nodeID, nlookup)
}

// --- unsupported ---

func (fs *passthroughFS) CopyFileRange(cancel <-chan struct{}, input *fuse.CopyFileRangeIn) (uint32, fuse.Status) {
	return 0, fuse.ENOSYS
}

func (fs *passthroughFS) Ioctl(cancel <-chan struct{}, input *fuse.IoctlIn, inbuf []byte, output *fuse.IoctlOut, outbuf []byte) fuse.Status {
	return fuse.ENOSYS
}

func (fs *passthroughFS) GetLk(cancel <-chan struct{}, in *fuse.LkIn, out *fuse.LkOut) fuse.Status {
	return fuse.ENOSYS
}

func (fs *passthroughFS) SetLk(cancel <-chan struct{}, in *fuse.LkIn) fuse.Status {
	return fuse.ENOSYS
}

func (fs *passthroughFS) SetLkw(cancel <-chan struct{}, in *fuse.LkIn) fuse.Status {
	return fuse.ENOSYS
}
