package pxarmount

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pbs-plus/pbs-plus/internal/log"
	pxar "github.com/pbs-plus/pxar"
	"github.com/puzpuzpuz/xsync/v4"
	"golang.org/x/sys/unix"
)

// Applied to the journal in Flush (file close) or SetAttr
// (whichever runs first), serialized by per-inode lock.
type pendingMeta struct {
	size    uint64
	mtimeNs int64
	ctimeNs int64
}

//   - PxarFS provides the immutable lower layer
//   - Journal provides the SQLite-backed inode graph for the overlay
//
// The journal uses a graph model (nodes + edges) making rename O(1):
//
//	Walk edges from root. If a component is whiteout → ENOENT.
//	If an edge is found → use the journal node (authoritative).
//	If no edge → fall back to pxar at the node's redirect_to path.
type MutableFS struct {
	fuse.RawFileSystem

	pxar       *PxarFS
	journal    *Journal
	mutableDir string

	// Inode allocation for pxar-only entries.
	nextIno atomic.Uint64

	// File handle management  -  lock-free via xsync.Map since every
	// Read/Write/Flush/Fsync calls getFh. nextFh uses atomic for
	handles *xsync.Map[uint64, *passFh]
	nextFh  atomic.Uint64

	// Per-inode writer locks.
	inoLocks *xsync.Map[uint64, *sync.Mutex]

	mmapData [][]byte

	origSnapshot  snapshotRef
	pbsStore      string
	origPpxarDidx string

	acl     ACLConfig
	verbose bool

	// Tracks inodes with deferred Write metadata not yet flushed to
	// the journal. Keyed by FUSE inode (input.NodeId).
	dirtyMeta *xsync.Map[uint64, pendingMeta]

	// Freeze mechanism: blocks FUSE mutations during commit.
	freezeMu   sync.Mutex
	freezeCond *sync.Cond
	frozen     bool

	// Inode ↔ path bidirectional mapping.
	// Per-instance to prevent cross-mount corruption  -  analogous to
	// ext4's per-superblock inode cache. Uses xsync.Map for lock-free
	inoLookup  *xsync.Map[string, uint64]
	pathLookup *xsync.Map[uint64, string]

	// Per-path ensureNode serialization  -  prevents duplicate journal
	// nodes when concurrent FUSE ops (e.g. setfacl -R) materialize
	// the same pxar entry simultaneously.
	ensureLocks *xsync.Map[string, *sync.Mutex]
}

// NewMutableFS creates a layered filesystem with an immutable pxar base and a mutable overlay.
func NewMutableFS(pxar *PxarFS, journal *Journal, mutableDir string) *MutableFS {
	fs := &MutableFS{
		pxar:        pxar,
		journal:     journal,
		mutableDir:  mutableDir,
		handles:     xsync.NewMap[uint64, *passFh](),
		inoLocks:    xsync.NewMap[uint64, *sync.Mutex](),
		inoLookup:   xsync.NewMap[string, uint64](),
		pathLookup:  xsync.NewMap[uint64, string](),
		ensureLocks: xsync.NewMap[string, *sync.Mutex](),
		dirtyMeta:   xsync.NewMap[uint64, pendingMeta](),
		nextIno:     atomic.Uint64{},
	}
	fs.nextIno.Store(1)
	fs.freezeCond = sync.NewCond(&fs.freezeMu)
	return fs
}

func (fs *MutableFS) SetSnapshotRef(ref snapshotRef) { fs.origSnapshot = ref }
func (fs *MutableFS) SetACLConfig(cfg ACLConfig)     { fs.acl = cfg }

// applyACL overrides the UID/GID and mode on a ResolvedEntry when the ACL
// config specifies a default owner, group, or mask.
func (fs *MutableFS) applyACL(re *ResolvedEntry) {
	if fs.acl.OwnerUID != 0 {
		re.UID = uint32(fs.acl.OwnerUID)
	}
	if fs.acl.OwnerGID != 0 {
		re.GID = uint32(fs.acl.OwnerGID)
	}

	// When ACL entries are present, the mode's group bits represent the ACL
	if fs.acl.HasACLs() {
		for _, e := range fs.acl.ACLEntries {
			if e.Tag == ACLMask {
				re.Mode = (re.Mode &^ 0070) | (uint32(e.Perm) << 3)
				break
			}
		}
	}
}
func (fs *MutableFS) SetVerbose(v bool) { fs.verbose = v }

func (fs *MutableFS) debugf(format string, args ...any) {
	if fs.verbose {
		fmt.Fprintf(os.Stderr, "  "+format+"\n", args...)
	}
}

func (fs *MutableFS) logNonFatal(op, path string, err error) {
	if fs.verbose {
		fmt.Fprintf(os.Stderr, "  [nonfatal] %s %s: %v\n", op, path, err)
	}
}

func (fs *MutableFS) SetStorePaths(pbsStore, ppxarDidx string) {
	fs.pbsStore = pbsStore
	fs.origPpxarDidx = ppxarDidx
}

func (fs *MutableFS) InitMutableRoot() error {
	return os.MkdirAll(fs.mutableDir, 0o755)
}

// ReconcileMutableDir removes orphan disk entries not tracked by journal nodes.
// Called on startup to clean up after unclean shutdowns  -  analogous to
// ext4's orphan inode cleanup during journal recovery (ext4_orphan_cleanup).
// A file is an orphan if:
//   - No journal node exists for its path, OR
//   - The journal node exists but HasData is false
//
// Directories are kept (they may be parents of tracked files and are cheap).
func (fs *MutableFS) ReconcileMutableDir() error {
	updated := false
	err := filepath.Walk(fs.mutableDir, func(absPath string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}

		relPath, rerr := filepath.Rel(fs.mutableDir, absPath)
		if rerr != nil {
			return nil
		}

		if relPath == "." || relPath == JournalDir || strings.HasPrefix(relPath, JournalDir+string(filepath.Separator)) {
			return nil
		}

		if info.IsDir() {
			return nil
		}

		fusePath := "/" + filepath.ToSlash(relPath)

		nodeID, _, _, _, rerr := fs.journal.ResolvePath(fusePath)
		if rerr != nil {
			return nil
		}

		if nodeID == 0 {
			if err := os.Remove(absPath); err != nil {
				fs.logNonFatal("reconcile-remove", fusePath, err)
			}
			return nil
		}

		node, nerr := fs.journal.GetNode(nodeID)
		if nerr != nil || node == nil {
			return nil
		}

		if !node.HasData {
			if err := os.Remove(absPath); err != nil {
				fs.logNonFatal("reconcile-remove", fusePath, err)
			}
			return nil
		}

		stat := info.Sys().(*syscall.Stat_t)
		if uint64(stat.Size) != node.Size || stat.Mtim.Nano() != node.MtimeNs {
			node.Size = uint64(info.Size())
			node.MtimeNs = info.ModTime().UnixNano()
			node.CtimeNs = info.ModTime().UnixNano()
			if err := fs.journal.UpdateNode(node); err != nil {
				log.Error(err, "")
			}
			updated = true
		}

		return nil
	})
	if err != nil {
		return err
	}
	if updated {
		return fs.journal.Sync()
	}
	return nil
}

func (fs *MutableFS) Init(server *fuse.Server) {
	fs.RawFileSystem = fuse.NewDefaultRawFileSystem()
	fs.RawFileSystem.Init(server)
}

func (fs *MutableFS) String() string    { return "pxar-mutable" }
func (fs *MutableFS) SetDebug(dbg bool) {}

// waitIfFrozen blocks until the filesystem is no longer frozen for commit.
// All mutation FUSE ops must call this first to ensure consistency.
func (fs *MutableFS) waitIfFrozen() {
	fs.freezeMu.Lock()
	for fs.frozen {
		fs.freezeCond.Wait()
	}
	fs.freezeMu.Unlock()
}

func (fs *MutableFS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	fs.debugf("Lookup: parent=%d name=%q", header.NodeId, name)
	if name == JournalDir {
		return fuse.ENOENT
	}

	parentPath := fs.inodeToPath(header.NodeId)
	childPath := joinPath(parentPath, name)

	re, status := fs.resolve(childPath)
	if status != fuse.OK {
		fs.debugf("Lookup: resolve(%q)=%s", childPath, status)
		return status
	}

	ino := fs.pathToIno(childPath, re.IsDir)
	fillResolvedEntryOut(ino, re, out)
	return fuse.OK
}

func (fs *MutableFS) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	path := fs.inodeToPath(input.NodeId)
	fs.debugf("GetAttr: ino=%d path=%q", input.NodeId, path)
	if path == "" && input.NodeId != RootInode {
		fs.debugf("GetAttr: ENOENT (no path for ino %d)", input.NodeId)
		return fuse.ENOENT
	}
	if path == "" {
		path = "/"
	}

	re, status := fs.resolve(path)
	if status != fuse.OK {
		fs.debugf("GetAttr: resolve(%q) failed: %s", path, status)
		return status
	}

	// stale journal value (journal is only updated on Flush/Close).
	if re.DataIsMut && !re.IsDir {
		if st, err := os.Stat(fs.mutablePath(path)); err == nil {
			re.Size = uint64(st.Size())
			re.MtimeNs = st.ModTime().UnixNano()
			re.CtimeNs = re.MtimeNs
		}
	}

	fillResolvedAttrOut(re, out)
	fs.debugf("GetAttr: ok mode=0%o isDir=%v", out.Mode, re.IsDir)
	return fuse.OK
}

func (fs *MutableFS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	return fuse.OK
}
func (fs *MutableFS) ReleaseDir(input *fuse.ReleaseIn) {}
func (fs *MutableFS) FsyncDir(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	return fuse.OK
}

func (fs *MutableFS) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	fs.debugf("ReadDir: ino=%d offset=%d", input.NodeId, input.Offset)
	return fs.readDirImpl(input, out, false)
}

func (fs *MutableFS) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	return fs.readDirImpl(input, out, true)
}

func (fs *MutableFS) readDirImpl(input *fuse.ReadIn, out *fuse.DirEntryList, plus bool) fuse.Status {
	parentPath := fs.inodeToPath(input.NodeId)
	if parentPath == "" && input.NodeId != RootInode {
		return fuse.ENOENT
	}
	if parentPath == "" {
		parentPath = "/"
	}

	// Resolve the parent to find its journal node and pxar source.
	re, status := fs.resolve(parentPath)
	if status != fuse.OK && status != fuse.ENOENT {
		return status
	}

	var parentNodeID int64
	var pxarDirPath string
	isOpaque := false

	if re != nil {
		if re.Node != nil {
			parentNodeID = re.Node.ID
			isOpaque = re.Node.Opaque
			if re.Node.RedirectTo != "" {
				pxarDirPath = re.Node.RedirectTo
			} else {
				pxarDirPath = parentPath
			}
		} else if re.PxarNode != nil {
			pxarDirPath = parentPath
		}
	}
	if pxarDirPath == "" && parentPath == "/" {
		pxarDirPath = "/"
	}

	var pxarEntries []dirEntrySlim
	if !isOpaque && pxarDirPath != "" {
		pxarNode := fs.findPxarNode(pxarDirPath)
		if pxarNode != nil && pxarNode.isDir {
			var rerr error
			pxarEntries, rerr = fs.pxar.ReadDirRaw(pxarNode.inode)
			if rerr != nil {
				fs.debugf("ReadDir: pxar readdir %q err: %v", pxarDirPath, rerr)
			}
		}
	}

	edgeNames := make(map[string]int64)
	whiteoutNames := make(map[string]bool)
	if parentNodeID != 0 {
		var edges []GraphEdge
		var wos []string
		if e, err := fs.journal.ListEdges(parentNodeID); err != nil {
			fs.debugf("ReadDir: list edges %d err: %v", parentNodeID, err)
		} else {
			edges = e
			for _, e := range edges {
				edgeNames[e.Name] = e.ChildID
			}
		}
		if w, err := fs.journal.ListWhiteouts(parentNodeID); err != nil {
			fs.debugf("ReadDir: list whiteouts %d err: %v", parentNodeID, err)
		} else {
			wos = w
			for _, w := range wos {
				whiteoutNames[w] = true
			}
		}
	}

	type mergedEntry struct {
		name  string
		ino   uint64
		mode  uint32
		isDir bool
	}
	var merged []mergedEntry

	for _, pe := range pxarEntries {
		if whiteoutNames[pe.name] {
			continue
		}
		if _, ok := edgeNames[pe.name]; ok {
			continue
		}
		childPath := joinPath(parentPath, pe.name)
		ino := fs.pathToIno(childPath, pe.isDir)
		merged = append(merged, mergedEntry{
			name: pe.name, ino: ino, mode: pe.mode, isDir: pe.isDir,
		})
	}

	// Go map iteration is randomized; without sorting, a multi-call
	// readdir (small buffer) would see different entry order on each
	// call, causing duplicates and missing entries via offset resume.
	edgeNamesSorted := make([]string, 0, len(edgeNames))
	for name := range edgeNames {
		edgeNamesSorted = append(edgeNamesSorted, name)
	}
	sort.Strings(edgeNamesSorted)

	for _, name := range edgeNamesSorted {
		nodeID := edgeNames[name]
		// Edges take priority over whiteouts  -  if there's a journal node,
		// it's always visible.
		node, err := fs.journal.GetNode(nodeID)
		if err != nil {
			log.Error(err, "")
		}
		if node == nil {
			continue
		}
		childPath := joinPath(parentPath, name)
		isDir := node.Kind == NodeDir
		ino := fs.pathToIno(childPath, isDir)
		merged = append(merged, mergedEntry{
			name: name, ino: ino, mode: node.Mode, isDir: isDir,
		})
	}

	if input.Offset == 0 {
		dirMode := fs.dirModeForPath(parentPath)
		if plus {
			eo := out.AddDirLookupEntry(fuse.DirEntry{Name: ".", Ino: input.NodeId, Mode: dirMode})
			if eo != nil {
				fs.fillEntryOutForPath(parentPath, eo)
			}
		} else {
			out.AddDirEntry(fuse.DirEntry{Name: ".", Ino: input.NodeId, Mode: dirMode})
		}
	}

	if input.Offset <= 1 {
		parentIno, parentMode := fs.getParentInfo(parentPath)
		if plus {
			eo := out.AddDirLookupEntry(fuse.DirEntry{Name: "..", Ino: parentIno, Mode: parentMode})
			if eo != nil {
				pp := filepath.Dir(parentPath)
				if pp == "." {
					pp = "/"
				}
				fs.fillEntryOutForPath(pp, eo)
			}
		} else {
			out.AddDirEntry(fuse.DirEntry{Name: "..", Ino: parentIno, Mode: parentMode})
		}
	}

	start := max(int(input.Offset)-2, 0)
	for i := start; i < len(merged); i++ {
		de := fuse.DirEntry{Name: merged[i].name, Ino: merged[i].ino, Mode: merged[i].mode}
		if plus {
			eo := out.AddDirLookupEntry(de)
			if eo == nil {
				break
			}
			fs.fillEntryOutForPath(joinPath(parentPath, merged[i].name), eo)
		} else {
			if !out.AddDirEntry(de) {
				break
			}
		}
	}
	return fuse.OK
}

func (fs *MutableFS) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	path := fs.inodeToPath(input.NodeId)
	fs.debugf("Open: ino=%d path=%q flags=0x%x", input.NodeId, path, input.Flags)
	if path == "" {
		return fuse.ENOENT
	}

	re, status := fs.resolve(path)
	if status != fuse.OK {
		fs.debugf("Open: resolve(%q) failed: %s", path, status)
		return status
	}

	flags := int(input.Flags) & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)
	isWrite := flags&(os.O_WRONLY|os.O_RDWR) != 0

	if isWrite && !re.DataIsMut {
		if err := fs.copyUp(re); err != nil {
			fs.debugf("Open: copyUp failed: %v", err)
			return fuse.ToStatus(err)
		}
		re.DataIsMut = true
	}

	if re.DataIsMut {
		abs := fs.mutablePath(path)
		fd, err := syscall.Open(abs, flags, 0)
		if err != nil {
			fs.debugf("Open: syscall.Open(%q) failed: %v", abs, err)
			return fuse.ToStatus(err)
		}
		fhID := fs.registerFh(path, fd)
		out.Fh = fhID
		out.OpenFlags = fuse.FOPEN_KEEP_CACHE
		fs.debugf("Open: mutable fh=%d", fhID)
		return fuse.OK
	}

	out.Fh = 0
	out.OpenFlags = fuse.FOPEN_KEEP_CACHE
	fs.debugf("Open: pxar passthrough")
	return fuse.OK
}

func (fs *MutableFS) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	path := fs.inodeToPath(input.NodeId)
	fs.debugf("Read: ino=%d fh=%d path=%q off=%d sz=%d", input.NodeId, input.Fh, path, input.Offset, len(buf))
	if path == "" {
		return nil, fuse.ENOENT
	}

	re, status := fs.resolve(path)
	if status != fuse.OK {
		fs.debugf("Read: resolve(%q) failed: %s", path, status)
		return nil, status
	}

	if re.DataIsMut {
		fh := fs.getFh(input.Fh)
		if fh == nil {
			fs.debugf("Read: EBADF fh=%d", input.Fh)
			return nil, fuse.EBADF
		}
		n, err := syscall.Pread(fh.fd, buf, int64(input.Offset))
		if err != nil {
			fs.debugf("Read: pread err: %v", err)
			return nil, fuse.ToStatus(err)
		}
		if n == 0 {
			return fuse.ReadResultData(nil), fuse.OK
		}
		return fuse.ReadResultData(buf[:n]), fuse.OK
	}

	// Delegate to pxar using its native inode.
	if re.PxarNode == nil {
		fs.debugf("Read: no pxar node for %q, re=%+v", path, re)
		return nil, fuse.EIO
	}
	pxarInput := *input
	pxarInput.NodeId = re.PxarNode.inode
	result, status := fs.pxar.Read(cancel, &pxarInput, buf)
	fs.debugf("Read: pxar delegate ino=%d status=%s", pxarInput.NodeId, status)
	return result, status
}

func (fs *MutableFS) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (uint32, fuse.Status) {
	fs.waitIfFrozen()
	path := fs.inodeToPath(input.NodeId)
	if path == "" {
		return 0, fuse.ENOENT
	}

	re, status := fs.resolve(path)
	if status != fuse.OK {
		return 0, status
	}
	if !re.DataIsMut {
		if err := fs.copyUp(re); err != nil {
			return 0, fuse.ToStatus(err)
		}
	}

	fh := fs.getFh(input.Fh)
	// already released), open an anonymous fd and close it after the
	// write. Registering it would leak since the kernel won't send
	closeAfterWrite := false
	if fh == nil {
		abs := fs.mutablePath(path)
		fd, err := syscall.Open(abs, os.O_WRONLY, 0)
		if err != nil {
			return 0, fuse.ToStatus(err)
		}
		fh = &passFh{fd: fd}
		closeAfterWrite = true
	}

	n, err := syscall.Pwrite(fh.fd, data, int64(input.Offset))
	if closeAfterWrite {
		if err := syscall.Close(fh.fd); err != nil {
			log.Error(err, "")
		}
	}
	if err != nil {
		return 0, fuse.ToStatus(err)
	}

	// Track pending metadata for deferred journal sync in Flush.
	// Avoids an fsync per 128 KB FUSE write chunk which kills throughput.
	newSize := uint64(int64(input.Offset) + int64(n))
	now := time.Now().UnixNano()
	fs.dirtyMeta.Compute(input.NodeId, func(old pendingMeta, exists bool) (pendingMeta, xsync.ComputeOp) {
		s := newSize
		if exists && old.size > s {
			s = old.size
		}
		return pendingMeta{size: s, mtimeNs: now, ctimeNs: now}, xsync.UpdateOp
	})

	return uint32(n), fuse.OK
}

func (fs *MutableFS) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {
	fs.waitIfFrozen()
	path := fs.inodeToPath(input.NodeId)
	if path == "" {
		return fuse.ENOENT
	}

	inoMu := fs.getInoLock(input.NodeId)
	inoMu.Lock()
	defer inoMu.Unlock()

	re, status := fs.resolve(path)
	if status != fuse.OK {
		return status
	}

	if v, ok := input.GetMode(); ok {
		re.Mode = v
	}
	if v, ok := input.GetUID(); ok {
		re.UID = v
	}
	if v, ok := input.GetGID(); ok {
		re.GID = v
	}
	sizeChanged := false
	if v, ok := input.GetSize(); ok {
		re.Size = v
		sizeChanged = true
		if re.DataIsMut {
			if err := os.Truncate(fs.mutablePath(path), int64(v)); err != nil {
				fs.logNonFatal("truncate", path, err)
			}
		}
	}
	if a, ok := input.GetATime(); ok {
		re.CtimeNs = a.UnixNano()
	}
	mtimeSet := false
	if m, ok := input.GetMTime(); ok {
		re.MtimeNs = m.UnixNano()
		mtimeSet = true
	}

	if re.DataIsMut {
		abs := fs.mutablePath(path)
		if m, ok := input.GetMode(); ok {
			if err := unix.Chmod(abs, m); err != nil {
				fs.logNonFatal("chmod", path, err)
			}
		}
		uid, gid := -1, -1
		if u, ok := input.GetUID(); ok {
			uid = int(u)
		}
		if g, ok := input.GetGID(); ok {
			gid = int(g)
		}
		if uid != -1 || gid != -1 {
			if err := unix.Lchown(abs, uid, gid); err != nil {
				fs.logNonFatal("lchown", path, err)
			}
		}
		if atime, aok := input.GetATime(); aok {
			if mtime, mok := input.GetMTime(); mok {
				tv := []unix.Timeval{
					{Sec: atime.Unix(), Usec: int64(atime.Nanosecond() / 1000)},
					{Sec: mtime.Unix(), Usec: int64(mtime.Nanosecond() / 1000)},
				}
				if err := unix.Lutimes(abs, tv); err != nil {
					fs.logNonFatal("lutimes", path, err)
				}
			}
		}
	}

	// Consume pending write metadata to prevent Flush from overwriting
	// our journal write with stale Write data.
	if meta, ok := fs.dirtyMeta.LoadAndDelete(input.NodeId); ok {
		if !sizeChanged && meta.size > re.Size {
			re.Size = meta.size
		}
		if !mtimeSet {
			re.MtimeNs = meta.mtimeNs
		}
	}

	fs.ensureNode(re)
	if re.Node != nil {
		re.Node.Mode = re.Mode
		re.Node.UID = re.UID
		re.Node.GID = re.GID
		re.Node.Size = re.Size
		re.Node.MtimeNs = re.MtimeNs
		re.Node.CtimeNs = re.CtimeNs
		re.Node.HasData = re.DataIsMut
		if err := fs.journal.UpdateNode(re.Node); err != nil {
			return fuse.EIO
		}
	}

	return fs.GetAttr(cancel, &fuse.GetAttrIn{InHeader: input.InHeader}, out)
}

func (fs *MutableFS) Create(cancel <-chan struct{}, input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	fs.waitIfFrozen()
	fs.debugf("Create: parent=%d name=%q", input.NodeId, name)
	parentPath := fs.inodeToPath(input.NodeId)
	childPath := joinPath(parentPath, name)

	abs := fs.mutablePath(childPath)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return fuse.ToStatus(err)
	}

	fd, err := syscall.Open(abs, int(input.Flags)|os.O_CREATE|os.O_EXCL, input.Mode&0o777)
	if err != nil {
		return fuse.ToStatus(err)
	}

	ino := fs.pathToIno(childPath, false)

	now := time.Now().UnixNano()
	node := &GraphNode{
		Kind:    NodeFile,
		Mode:    uint32(syscall.S_IFREG) | input.Mode&0o777,
		UID:     input.Uid,
		GID:     input.Gid,
		Size:    0,
		MtimeNs: now,
		CtimeNs: now,
		HasData: true,
	}

	parentID := fs.resolveParentNodeID(parentPath)
	shadowPxar := fs.hasPxarEntry(childPath)

	// Atomically create node + edge + optional whiteout.
	nodeID, err := fs.journal.CreateNodeEdgeAndWhiteout(parentID, name, node, shadowPxar)
	if err != nil {
		if cerr := syscall.Close(fd); cerr != nil {
			fs.logNonFatal("close-fd-cleanup", "fd", cerr)
		}
		if rerr := os.Remove(abs); rerr != nil {
			fs.logNonFatal("remove-cleanup", abs, rerr)
		}
		fs.unmapInode(childPath)
		return fuse.EIO
	}
	node.ID = nodeID

	fs.applyACLOwnership(abs)

	fhID := fs.registerFh(childPath, fd)

	out.NodeId = ino
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	out.Fh = fhID
	out.OpenFlags = fuse.FOPEN_KEEP_CACHE
	fillAttrFromNode(&out.Attr, node)
	out.Ino = ino
	return fuse.OK
}

func (fs *MutableFS) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) fuse.Status {
	fs.waitIfFrozen()
	parentPath := fs.inodeToPath(input.NodeId)
	childPath := joinPath(parentPath, name)

	abs := fs.mutablePath(childPath)
	if err := syscall.Mkdir(abs, input.Mode&0o777); err != nil {
		return fuse.ToStatus(err)
	}

	fs.applyACLOwnership(abs)

	hasPxar := fs.hasPxarEntry(childPath)
	now := time.Now().UnixNano()
	node := &GraphNode{
		Kind:    NodeDir,
		Mode:    input.Mode&0o777 | syscall.S_IFDIR,
		UID:     input.Uid,
		GID:     input.Gid,
		Size:    0,
		MtimeNs: now,
		CtimeNs: now,
		HasData: false,
		Opaque:  hasPxar, // hide pxar children if shadowing
	}
	if hasPxar {
		node.RedirectTo = childPath // retain pxar source for metadata
	}

	parentID := fs.resolveParentNodeID(parentPath)

	// Atomically create node + edge + whiteout.
	nodeID, err := fs.journal.CreateNodeEdgeAndWhiteout(parentID, name, node, hasPxar)
	if err != nil {
		if err := os.Remove(abs); err != nil && !os.IsNotExist(err) {
			log.Error(err, "")
		}
		return fuse.EIO
	}
	node.ID = nodeID

	ino := fs.pathToIno(childPath, true)

	out.NodeId = ino
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	fillAttrFromNode(&out.Attr, node)
	out.Ino = ino
	return fuse.OK
}

func (fs *MutableFS) Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) fuse.Status {
	fs.waitIfFrozen()
	parentPath := fs.inodeToPath(input.NodeId)
	childPath := joinPath(parentPath, name)

	abs := fs.mutablePath(childPath)
	if err := syscall.Mknod(abs, input.Mode, int(input.Rdev)); err != nil {
		return fuse.ToStatus(err)
	}

	fs.applyACLOwnership(abs)

	hasPxar := fs.hasPxarEntry(childPath)
	now := time.Now().UnixNano()
	node := &GraphNode{
		Kind:    NodeFile,
		Mode:    input.Mode,
		UID:     input.Uid,
		GID:     input.Gid,
		Size:    0,
		MtimeNs: now,
		CtimeNs: now,
		HasData: true,
	}
	if hasPxar {
		node.RedirectTo = childPath
	}

	parentID := fs.resolveParentNodeID(parentPath)

	// Atomically create node + edge + whiteout.
	nodeID, err := fs.journal.CreateNodeEdgeAndWhiteout(parentID, name, node, hasPxar)
	if err != nil {
		if err := os.Remove(abs); err != nil && !os.IsNotExist(err) {
			log.Error(err, "")
		}
		return fuse.EIO
	}
	node.ID = nodeID

	ino := fs.pathToIno(childPath, false)

	out.NodeId = ino
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	fillAttrFromNode(&out.Attr, node)
	out.Ino = ino
	return fuse.OK
}

func (fs *MutableFS) Symlink(cancel <-chan struct{}, header *fuse.InHeader, target string, linkName string, out *fuse.EntryOut) fuse.Status {
	fs.waitIfFrozen()
	parentPath := fs.inodeToPath(header.NodeId)
	childPath := joinPath(parentPath, linkName)

	abs := fs.mutablePath(childPath)
	if err := syscall.Symlink(target, abs); err != nil {
		return fuse.ToStatus(err)
	}

	fs.applyACLOwnership(abs)

	hasPxar := fs.hasPxarEntry(childPath)
	now := time.Now().UnixNano()
	node := &GraphNode{
		Kind:       NodeSymlink,
		Mode:       uint32(syscall.S_IFLNK | 0o777),
		UID:        header.Uid,
		GID:        header.Gid,
		Size:       0,
		MtimeNs:    now,
		CtimeNs:    now,
		HasData:    true,
		SymlinkTgt: target,
	}
	if hasPxar {
		node.RedirectTo = childPath
	}

	parentID := fs.resolveParentNodeID(parentPath)

	// Atomically create node + edge + whiteout.
	nodeID, err := fs.journal.CreateNodeEdgeAndWhiteout(parentID, linkName, node, hasPxar)
	if err != nil {
		if err := os.Remove(abs); err != nil && !os.IsNotExist(err) {
			log.Error(err, "")
		}
		return fuse.EIO
	}
	node.ID = nodeID

	ino := fs.pathToIno(childPath, false)

	out.NodeId = ino
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	fillAttrFromNode(&out.Attr, node)
	out.Ino = ino
	return fuse.OK
}

func (fs *MutableFS) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	fs.waitIfFrozen()
	parentPath := fs.inodeToPath(header.NodeId)
	childPath := joinPath(parentPath, name)

	re, status := fs.resolve(childPath)
	if status != fuse.OK {
		return status
	}

	// Journal-first for destructive ops.
	parentID := fs.resolveParentNodeID(parentPath)

	if re.Node != nil {
		// Atomically remove edge + node + add whiteout if pxar counterpart exists.
		needsWhiteout := re.PxarNode != nil || fs.hasPxarEntry(childPath)
		if err := fs.journal.DeleteEdgeAndNode(parentID, name, re.Node.ID, needsWhiteout); err != nil {
			return fuse.EIO
		}
	} else if re.PxarNode != nil {
		// Pure pxar deletion: just add whiteout.
		if err := fs.journal.AddWhiteout(parentID, name); err != nil {
			return fuse.EIO
		}
	}

	if re.DataIsMut {
		if err := os.Remove(fs.mutablePath(childPath)); err != nil {
			fs.logNonFatal("remove", childPath, err)
		}
	}

	fs.unmapInode(childPath)
	return fuse.OK
}

func (fs *MutableFS) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	fs.waitIfFrozen()
	parentPath := fs.inodeToPath(header.NodeId)
	childPath := joinPath(parentPath, name)

	re, status := fs.resolve(childPath)
	if status != fuse.OK {
		return status
	}
	if !re.IsDir {
		return fuse.ENOTDIR
	}

	parentNodeID := fs.resolveParentNodeID(childPath)
	if parentNodeID != 0 {
		edges, err := fs.journal.ListEdges(parentNodeID)
		if err != nil {
			log.Error(err, "")
		}
		whiteouts, err := fs.journal.ListWhiteouts(parentNodeID)
		if err != nil {
			log.Error(err, "")
		}
		if len(edges) > 0 || len(whiteouts) > 0 {
			return fuse.Status(syscall.ENOTEMPTY)
		}
	}

	// Also check pxar children if not opaque.
	if re.Node == nil || !re.Node.Opaque {
		pxarDirPath := childPath
		if re.Node != nil && re.Node.RedirectTo != "" {
			pxarDirPath = re.Node.RedirectTo
		}
		if pxarNode := fs.findPxarNode(pxarDirPath); pxarNode != nil {
			entries, err := fs.pxar.ReadDirRaw(pxarNode.inode)
			if err != nil {
				log.Error(err, "")
			}
			if len(entries) > 0 {
				return fuse.Status(syscall.ENOTEMPTY)
			}
		}
	}

	return fs.Unlink(cancel, header, name)
}

func (fs *MutableFS) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName string, newName string) fuse.Status {
	fs.waitIfFrozen()
	oldParentPath := fs.inodeToPath(input.NodeId)
	newParentPath := fs.inodeToPath(input.Newdir)
	oldPath := joinPath(oldParentPath, oldName)
	newPath := joinPath(newParentPath, newName)

	oldRE, oldStatus := fs.resolve(oldPath)
	if oldStatus != fuse.OK {
		return oldStatus
	}

	oldParentID := fs.resolveParentNodeID(oldParentPath)
	newParentID := fs.resolveParentNodeID(newParentPath)

	destHasPXar := fs.hasPxarEntry(newPath)
	destRE, _ := fs.resolve(newPath)
	var destNodeID int64
	if destRE != nil && destRE.Node != nil {
		destNodeID = destRE.Node.ID
	}

	// All journal mutations happen in a single SQLite transaction so a
	// crash at any point leaves the journal in a consistent state.
	if oldRE.Node != nil {
		// Source has a journal node: atomically move edge, replace dest, add whiteouts.
		whiteoutOld := oldRE.Node.RedirectTo != ""
		if err := fs.journal.MoveEdgeAndWhiteout(
			oldParentID, oldName, newParentID, newName,
			destNodeID, whiteoutOld, destHasPXar); err != nil {
			return fuse.EIO
		}
	} else {
		// Source is pxar-only: create journal node at destination, whiteout old.
		now := time.Now().UnixNano()
		node := &GraphNode{
			Kind:       nodeKindFromPxar(oldRE.PxarNode),
			Mode:       statMode(oldRE.PxarNode.mode),
			UID:        oldRE.PxarNode.uid,
			GID:        oldRE.PxarNode.gid,
			Size:       oldRE.PxarNode.fileSize,
			MtimeNs:    oldRE.PxarNode.mtimeSecs*1e9 + int64(oldRE.PxarNode.mtimeNanos),
			CtimeNs:    now,
			HasData:    false,
			RedirectTo: oldPath,
			SymlinkTgt: oldRE.SymlinkTgt,
		}
		// Use compound operation: delete dest edge+node if needed, create node+edge+whiteouts.
		if destNodeID != 0 {
			if err := fs.journal.DeleteEdgeAndNode(newParentID, newName, destNodeID, false); err != nil {
				return fuse.EIO
			}
		}
		nodeID, err := fs.journal.CreateNodeEdgeAndWhiteout(newParentID, newName, node, false)
		if err != nil {
			return fuse.EIO
		}
		node.ID = nodeID

		// Whiteout old location.
		if err := fs.journal.AddWhiteout(oldParentID, oldName); err != nil {
			return fuse.EIO
		}
		if destHasPXar {
			if err := fs.journal.AddWhiteout(newParentID, newName); err != nil {
				fs.logNonFatal("add-whiteout", newName, err)
			}
		}
		oldRE.Node = node
	}

	fs.unmapInode(newPath)
	ino := fs.pathToIno(oldPath, oldRE.IsDir)
	fs.unmapInode(oldPath)
	fs.mapInode(ino, newPath)

	if oldRE.IsDir {
		fs.remapPathPrefix(oldPath, newPath)
	}

	// If we crash here, the journal is consistent  -  disk files are redundant

	// Remove destination mutable data (journal already points away from it).
	if destRE != nil && destRE.DataIsMut {
		if err := os.Remove(fs.mutablePath(newPath)); err != nil {
			fs.logNonFatal("remove-dest", newPath, err)
		}
	}

	if oldRE.DataIsMut || oldRE.IsDir {
		oldAbs := fs.mutablePath(oldPath)
		if _, err := os.Stat(oldAbs); err == nil {
			newAbs := fs.mutablePath(newPath)
			if err := os.MkdirAll(filepath.Dir(newAbs), 0o755); err != nil {
				return fuse.ToStatus(err)
			}
			if err := os.Rename(oldAbs, newAbs); err != nil {
				// If copy also fails, the journal edge is still correct
				// and ReconcileMutableDir will clean up on next startup.
				fs.logNonFatal("rename-disk", oldPath, err)
				if !oldRE.IsDir {
					if copyErr := copyRegularFile(oldAbs, newAbs); copyErr != nil {
						fs.logNonFatal("copy-fallback", newPath, copyErr)
					}
				}
			}
		}
	}

	return fuse.OK
}

func (fs *MutableFS) Readlink(cancel <-chan struct{}, header *fuse.InHeader) ([]byte, fuse.Status) {
	path := fs.inodeToPath(header.NodeId)
	if path == "" {
		return nil, fuse.ENOENT
	}

	re, status := fs.resolve(path)
	if status != fuse.OK {
		return nil, status
	}

	if re.SymlinkTgt != "" {
		return []byte(re.SymlinkTgt), fuse.OK
	}
	if re.PxarNode != nil && re.PxarNode.isSymlink {
		pxarHeader := *header
		pxarHeader.NodeId = re.PxarNode.inode
		return fs.pxar.Readlink(cancel, &pxarHeader)
	}
	return nil, fuse.EINVAL
}

func (fs *MutableFS) GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, dest []byte) (uint32, fuse.Status) {
	path := fs.inodeToPath(header.NodeId)
	if path == "" {
		return 0, fuse.ENOENT
	}

	re, _ := fs.resolve(path)
	if status, handled := fs.resolveCheck(path, re); !handled {
		return 0, status
	}

	// Priority: passthrough → journal → default ACL → pxar.

	if re.DataIsMut {
		abs := fs.mutablePath(path)
		sz, xerr := unix.Getxattr(abs, attr, dest)
		if xerr == nil {
			return uint32(sz), fuse.OK
		}
	}

	// 2. Journal.
	if re.Node != nil {
		val, err := fs.journal.GetXAttr(re.Node.ID, attr)
		if err != nil {
			return 0, fuse.EIO
		}
		if val != nil {
			return xattrValue(val, dest)
		}
	}

	// 3. Default ACL (virtual, no write).
	switch attr {
	case "system.posix_acl_access":
		if fs.acl.HasACLs() {
			return xattrValue(MarshalACL(fs.acl.ACLEntries), dest)
		}
	case "system.posix_acl_default":
		if re.IsDir && len(fs.acl.DefaultACLEntries) > 0 {
			return xattrValue(MarshalACL(fs.acl.DefaultACLEntries), dest)
		}
		return 0, fuse.Status(syscall.ENODATA)
	}

	// 4. Pxar archive.
	if re.PxarNode != nil {
		pxarHeader := *header
		pxarHeader.NodeId = re.PxarNode.inode
		return fs.pxar.GetXAttr(cancel, &pxarHeader, attr, dest)
	}

	return 0, fuse.Status(syscall.ENODATA)
}

func (fs *MutableFS) ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (uint32, fuse.Status) {
	path := fs.inodeToPath(header.NodeId)
	if path == "" {
		return 0, fuse.ENOENT
	}

	re, _ := fs.resolve(path)
	if status, handled := fs.resolveCheck(path, re); !handled {
		return 0, status
	}

	nameSet := make(map[string]bool)

	if re.DataIsMut {
		abs := fs.mutablePath(path)
		sz, xerr := unix.Listxattr(abs, nil)
		if xerr == nil && sz > 0 {
			buf := make([]byte, sz)
			if sz, xerr = unix.Listxattr(abs, buf); xerr == nil {
				start := 0
				for i := 0; i <= sz; i++ {
					if i == sz || buf[i] == 0 {
						if i > start {
							nameSet[string(buf[start:i])] = true
						}
						start = i + 1
					}
				}
			}
		}
	}

	// 2. Journal xattrs.
	if re.Node != nil {
		names, err := fs.journal.ListXAttrs(re.Node.ID)
		if err != nil {
			log.Error(err, "")
		}
		for _, n := range names {
			nameSet[n] = true
		}
	}

	// 3. Default ACL xattr names (virtual).
	if fs.acl.HasACLs() {
		nameSet["system.posix_acl_access"] = true
		if re.IsDir && len(fs.acl.DefaultACLEntries) > 0 {
			nameSet["system.posix_acl_default"] = true
		}
	}

	// 4. Pxar xattrs.
	if re.PxarNode != nil {
		pxarHeader := *header
		pxarHeader.NodeId = re.PxarNode.inode
		pxarSz, pxarStatus := fs.pxar.ListXAttr(cancel, &pxarHeader, nil)
		if pxarStatus == fuse.OK && pxarSz > 0 {
			buf := make([]byte, pxarSz)
			sz, status := fs.pxar.ListXAttr(cancel, &pxarHeader, buf)
			if status == fuse.OK {
				start := 0
				for i := 0; i <= int(sz); i++ {
					if i == int(sz) || buf[i] == 0 {
						if i > start {
							nameSet[string(buf[start:i])] = true
						}
						start = i + 1
					}
				}
			}
		}
	}

	var total uint32
	for n := range nameSet {
		total += uint32(len(n)) + 1
	}
	if dest == nil {
		return total, fuse.OK
	}
	if uint32(len(dest)) < total {
		return 0, fuse.Status(syscall.ERANGE)
	}
	pos := 0
	for n := range nameSet {
		pos += copy(dest[pos:], n)
		dest[pos] = 0
		pos++
	}
	return uint32(pos), fuse.OK
}

func (fs *MutableFS) SetXAttr(cancel <-chan struct{}, input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	fs.waitIfFrozen()
	path := fs.inodeToPath(input.NodeId)
	if path == "" {
		return fuse.ENOENT
	}

	re, status := fs.resolve(path)
	if status != fuse.OK {
		return status
	}
	fs.ensureNode(re)

	if re.Node == nil {
		return fuse.EIO
	}

	if err := fs.journal.SetXAttr(re.Node.ID, attr, data); err != nil {
		return fuse.EIO
	}

	if re.DataIsMut {
		abs := fs.mutablePath(path)
		flags := 0
		if input.Flags&XattrCreate != 0 {
			flags = unix.XATTR_CREATE
		} else if input.Flags&XattrReplace != 0 {
			flags = unix.XATTR_REPLACE
		}
		if err := unix.Setxattr(abs, attr, data, flags); err != nil {
			fs.logNonFatal("setxattr", attr, err)
		}
	}

	return fuse.OK
}

func (fs *MutableFS) RemoveXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string) fuse.Status {
	fs.waitIfFrozen()
	path := fs.inodeToPath(header.NodeId)
	if path == "" {
		return fuse.ENOENT
	}

	re, _ := fs.resolve(path)
	if re == nil || re.Node == nil {
		return fuse.OK
	}

	if err := fs.journal.RemoveXAttr(re.Node.ID, attr); err != nil {
		return fuse.EIO
	}

	if re.DataIsMut {
		if err := unix.Removexattr(fs.mutablePath(path), attr); err != nil {
			fs.logNonFatal("removexattr", attr, err)
		}
	}

	return fuse.OK
}

func (fs *MutableFS) Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status {
	// Sync dirty node metadata to journal on close.
	// inoLock serializes with concurrent SetAttr on the same inode so
	// neither overwrites the other's journal write.
	inoMu := fs.getInoLock(input.NodeId)
	inoMu.Lock()
	if meta, ok := fs.dirtyMeta.LoadAndDelete(input.NodeId); ok {
		path := fs.inodeToPath(input.NodeId)
		if path != "" {
			if re, status := fs.resolve(path); status == fuse.OK && re.Node != nil {
				if meta.size > re.Node.Size {
					re.Node.Size = meta.size
				}
				re.Node.MtimeNs = meta.mtimeNs
				re.Node.CtimeNs = meta.ctimeNs
				if err := fs.journal.UpdateNode(re.Node); err != nil {
					log.Error(err, "")
				}
			}
		}
	}
	inoMu.Unlock()
	if input.Fh == 0 {
		return fuse.OK // pxar passthrough, no fd to sync
	}
	return fs.fsyncInternal(input.NodeId, input.Fh)
}

func (fs *MutableFS) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	// Sync journal so metadata durability matches data durability.
	if err := fs.journal.Sync(); err != nil {
		log.Error(err, "")
	}
	if input.Fh == 0 {
		return fuse.OK
	}
	return fs.fsyncInternal(input.NodeId, input.Fh)
}

func (fs *MutableFS) fsyncInternal(_, fhID uint64) fuse.Status {
	fh := fs.getFh(fhID)
	if fh == nil {
		return fuse.EBADF
	}
	if err := syscall.Fsync(fh.fd); err != nil {
		return fuse.ToStatus(err)
	}
	return fuse.OK
}

func (fs *MutableFS) Release(cancel <-chan struct{}, input *fuse.ReleaseIn) {
	if input.Fh == 0 {
		return // pxar passthrough, no fd to close
	}
	if fh, ok := fs.handles.LoadAndDelete(input.Fh); ok {
		if err := syscall.Close(fh.fd); err != nil {
			fs.logNonFatal("close-fd", "fd", err)
		}
	}
	// Clean up per-inode lock  -  operations that need it will
	fs.inoLocks.Delete(input.NodeId)
}

// Forget is called by the FUSE kernel when it evicts an inode from its
// dentry cache. We clean up per-inode and per-path synchronization
// state that would otherwise leak indefinitely.
// NOTE: We don't delete inoLookup/pathLookup here because there may
// still be open file handles (Read/Write/Flush/Release) that need the
// inode→path mapping. Those are cleaned up in unmapInode (Unlink,
func (fs *MutableFS) Forget(nodeID, nlookup uint64) {
	fs.inoLocks.Delete(nodeID)
	if path, ok := fs.pathLookup.Load(nodeID); ok {
		fs.ensureLocks.Delete(path)
	}
	fs.dirtyMeta.Delete(nodeID)
}

func (fs *MutableFS) Link(cancel <-chan struct{}, input *fuse.LinkIn, name string, out *fuse.EntryOut) fuse.Status {
	fs.waitIfFrozen()
	return fuse.ENOSYS
}
func (fs *MutableFS) CopyFileRange(cancel <-chan struct{}, input *fuse.CopyFileRangeIn) (uint32, fuse.Status) {
	return 0, fuse.ENOSYS
}
func (fs *MutableFS) Ioctl(cancel <-chan struct{}, input *fuse.IoctlIn, inbuf []byte, output *fuse.IoctlOut, outbuf []byte) fuse.Status {
	return fuse.ENOSYS
}
func (fs *MutableFS) GetLk(cancel <-chan struct{}, in *fuse.LkIn, out *fuse.LkOut) fuse.Status {
	return fuse.ENOSYS
}
func (fs *MutableFS) SetLk(cancel <-chan struct{}, in *fuse.LkIn) fuse.Status {
	return fuse.ENOSYS
}
func (fs *MutableFS) SetLkw(cancel <-chan struct{}, in *fuse.LkIn) fuse.Status {
	return fuse.ENOSYS
}

func (fs *MutableFS) Lseek(cancel <-chan struct{}, in *fuse.LseekIn, out *fuse.LseekOut) fuse.Status {
	fh := fs.getFh(in.Fh)
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

func (fs *MutableFS) StatFs(cancel <-chan struct{}, header *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	var st unix.Statfs_t
	if err := unix.Statfs(fs.mutableDir, &st); err != nil {
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

func (fs *MutableFS) Statx(cancel <-chan struct{}, input *fuse.StatxIn, out *fuse.StatxOut) fuse.Status {
	return fuse.ENOSYS
}
func (fs *MutableFS) Access(cancel <-chan struct{}, input *fuse.AccessIn) fuse.Status {
	return fuse.OK
}

func (fs *MutableFS) copyUp(re *ResolvedEntry) error {
	inoMu := fs.getInoLock(re.Inode)
	inoMu.Lock()
	defer inoMu.Unlock()

	if re.DataIsMut {
		return nil
	}

	// Ensure we have a journal node for this path.
	fs.ensureNode(re)
	if re.Node == nil {
		return fmt.Errorf("copyUp: could not create node for %q", re.Path)
	}

	abs := fs.mutablePath(re.Path)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return err
	}

	if re.PxarNode != nil {
		if re.PxarNode.isReg {
			if err := fs.copyUpRegularFile(re.Path, re.PxarNode); err != nil {
				return err
			}
		}
		if re.PxarNode.isSymlink {
			entry, err := fs.pxar.GetPxarEntry(re.PxarNode.inode)
			if err != nil {
				return err
			}
			if err := syscall.Symlink(entry.LinkTarget, abs); err != nil {
				return err
			}
		}
	}

	if err := fs.journal.SetHasData(re.Node.ID); err != nil {
		return fmt.Errorf("journal set has_data: %w", err)
	}
	re.DataIsMut = true
	re.Node.HasData = true

	fs.applyACLOwnership(abs)
	return nil
}

func (fs *MutableFS) copyUpRegularFile(path string, n *node) error {
	abs := fs.mutablePath(path)
	f, err := os.OpenFile(abs, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	entry, err := fs.pxar.GetPxarEntry(n.inode)
	if err != nil {
		return err
	}

	rc, err := fs.pxar.Reader().ReadFileContentReader(entry)
	if err != nil {
		return err
	}
	defer func() {
		if err := rc.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	bufp := copyBufPool.Get().(*[]byte)
	defer copyBufPool.Put(bufp)
	if _, err := io.CopyBuffer(f, rc, *bufp); err != nil {
		return err
	}

	mode := os.FileMode(statMode(n.mode) & 0o7777)
	if err := os.Chmod(abs, mode); err != nil {
		fs.logNonFatal("chmod", abs, err)
	}

	// Preserve extended attributes and file capabilities from the pxar
	// for write silently drops all xattrs and fcaps.
	applyPxarXattrsToFile(abs, entry)

	return nil
}

// applyPxarXattrsToFile sets extended attributes and file capabilities
// from a pxar entry onto a real file. Errors are logged but not fatal  -
func applyPxarXattrsToFile(abs string, entry *pxar.Entry) {
	for _, xa := range entry.Metadata.XAttrs {
		name := xa.Name()
		if len(name) == 0 {
			continue
		}
		// Skip ACL and fcaps  -  those are handled separately via the
		if isACLXattr(name) || isFcapsXattr(name) {
			continue
		}
		if err := unix.Lsetxattr(abs, string(name), xa.Value(), 0); err != nil {
			if !isIgnorableXattrErr(err) {
				fmt.Fprintf(os.Stderr, "  [nonfatal] copyUp xattr %q on %q: %v\n", string(name), abs, err)
			}
		}
	}
	if len(entry.Metadata.FCaps) > 0 {
		if err := unix.Lsetxattr(abs, "security.capability", entry.Metadata.FCaps, 0); err != nil {
			if !isIgnorableXattrErr(err) {
				fmt.Fprintf(os.Stderr, "  [nonfatal] copyUp fcaps on %q: %v\n", abs, err)
			}
		}
	}
}

func isACLXattr(name []byte) bool {
	return bytesEq(name, "system.posix_acl_access") || bytesEq(name, "system.posix_acl_default")
}

func isFcapsXattr(name []byte) bool {
	return bytesEq(name, "security.capability")
}

// isIgnorableXattrErr reports whether an xattr error can be safely ignored
func isIgnorableXattrErr(err error) bool {
	return errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.ENODATA) || errors.Is(err, unix.EOPNOTSUPP)
}

// Used as a fallback when os.Rename fails during Rename operations
// (e.g. cross-device rename).
func copyRegularFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() {
		if err := in.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		if err := out.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	bufp := copyBufPool.Get().(*[]byte)
	defer copyBufPool.Put(bufp)
	_, err = io.CopyBuffer(out, in, *bufp)
	return err
}

// resolve looks up a path using the inode graph, falling back to pxar.
func (fs *MutableFS) resolveRoot() (*ResolvedEntry, fuse.Status) {
	n, err := fs.journal.GetNode(1)
	fs.debugf("resolveRoot: GetNode(1) err=%v node=%+v", err, n)
	if err != nil {
		return nil, fuse.EIO
	}
	if n != nil {
		return fs.resolveFromNode("/", n)
	}
	// Fallback to pure pxar root
	pxarNode := fs.findPxarNode("/")
	if pxarNode == nil {
		return nil, fuse.ENOENT
	}
	re := &ResolvedEntry{
		Path:      "/",
		PxarNode:  pxarNode,
		DataIsMut: false,
		IsDir:     true,
		Mode:      statMode(pxarNode.mode),
		UID:       pxarNode.uid,
		GID:       pxarNode.gid,
		Size:      pxarNode.fileSize,
		MtimeNs:   pxarNode.mtimeSecs*1e9 + int64(pxarNode.mtimeNanos),
		CtimeNs:   pxarNode.mtimeSecs*1e9 + int64(pxarNode.mtimeNanos),
	}
	re.Inode = fs.pathToIno("/", true)
	fs.applyACL(re)
	return re, fuse.OK
}

func (fs *MutableFS) resolve(path string) (*ResolvedEntry, fuse.Status) {
	// Root is always a pxar-only directory.
	if path == "/" || path == "" {
		return fs.resolveRoot()
	}
	nodeID, pxarPath, _, _, err := fs.journal.ResolvePath(path)
	if err != nil {
		fs.debugf("resolve(%q) ResolvePath err: %v", path, err)
		return nil, fuse.EIO
	}

	// Whiteout detected.
	if nodeID == 0 && pxarPath == "" {
		return nil, fuse.ENOENT
	}

	if nodeID != 0 {
		node, err := fs.journal.GetNode(nodeID)
		if err != nil {
			return nil, fuse.EIO
		}
		if node == nil {
			return nil, fuse.ENOENT
		}
		return fs.resolveFromNode(path, node)
	}

	// Fell off graph  -  check pxar.
	pxarNode := fs.findPxarNode(pxarPath)
	if pxarNode == nil {
		return nil, fuse.ENOENT
	}

	re := &ResolvedEntry{
		Path:      path,
		PxarNode:  pxarNode,
		DataIsMut: false,
		IsDir:     pxarNode.isDir,
		Mode:      statMode(pxarNode.mode),
		UID:       pxarNode.uid,
		GID:       pxarNode.gid,
		Size:      pxarNode.fileSize,
		MtimeNs:   pxarNode.mtimeSecs*1e9 + int64(pxarNode.mtimeNanos),
		CtimeNs:   pxarNode.mtimeSecs*1e9 + int64(pxarNode.mtimeNanos),
	}
	// Use cached xattr-derived times if already resolved; otherwise fall
	// back to Stat.Mtime. Full resolution is deferred to individual
	// GetAttr calls so that bulk readdir never triggers O(N) archive reads.
	if pxarNode.timesResolved {
		aNs, mNs := fs.pxar.ResolvedTimes(pxarNode)
		re.AtimeNs = aNs
		re.MtimeNs = mNs
		re.CtimeNs = mNs
	} else {
		re.AtimeNs = re.MtimeNs
	}
	re.Inode = fs.pathToIno(path, re.IsDir)
	fs.applyACL(re)
	return re, fuse.OK
}

// resolveCheck is a helper for xattr ops that returns (status, ok) where
func (fs *MutableFS) resolveCheck(_ string, re *ResolvedEntry) (fuse.Status, bool) {
	if re == nil {
		return fuse.ENOENT, false
	}
	return fuse.OK, true
}

func (fs *MutableFS) resolveFromNode(path string, n *GraphNode) (*ResolvedEntry, fuse.Status) {
	re := &ResolvedEntry{
		Path:       path,
		Node:       n,
		DataIsMut:  n.HasData,
		IsDir:      n.Kind == NodeDir,
		Mode:       n.Mode,
		UID:        n.UID,
		GID:        n.GID,
		Size:       n.Size,
		MtimeNs:    n.MtimeNs,
		CtimeNs:    n.CtimeNs,
		SymlinkTgt: n.SymlinkTgt,
	}

	// Ensure mode has file type bits.
	re.Mode = ensureModeType(re.Mode, n.Kind)

	// If the node has a redirect, check pxar for data.
	if n.RedirectTo != "" && !n.HasData {
		pxarNode := fs.findPxarNode(n.RedirectTo)
		re.PxarNode = pxarNode
		// Use pxar metadata if node fields are zero.
		if pxarNode != nil {
			if re.Size == 0 {
				re.Size = pxarNode.fileSize
			}
			if re.UID == 0 && re.GID == 0 {
				re.UID = pxarNode.uid
				re.GID = pxarNode.gid
			}
		}
	}

	re.Inode = fs.pathToIno(path, re.IsDir)
	fs.applyACL(re)
	return re, fuse.OK
}

func (fs *MutableFS) findPxarNode(path string) *node {
	if path == "/" {
		return fs.pxar.GetNode(RootInode)
	}

	curIno := RootInode
	parts := splitPath(path)

	for i, name := range parts {
		if name == "" {
			continue
		}
		entries, err := fs.pxar.ReadDirRaw(curIno)
		if err != nil {
			return nil
		}
		found := false
		for _, e := range entries {
			if e.name == name {
				// root inode is cached and subdirectories appear empty.
				n := fs.pxar.RegisterSlimNode(&e, curIno)
				if i == len(parts)-1 {
					return n
				}
				curIno = e.inode
				found = true
				break
			}
		}
		if !found {
			return nil
		}
	}
	return nil
}

func (fs *MutableFS) hasPxarEntry(path string) bool {
	return fs.findPxarNode(path) != nil
}

// resolveParentNodeID ensures a journal node exists for the parent path
// and returns its node ID. Creates pxar-derived nodes as needed.
func (fs *MutableFS) resolveParentNodeID(parentPath string) int64 {
	if parentPath == "" || parentPath == "/" {
		return 1
	}

	re, status := fs.resolve(parentPath)
	if status != fuse.OK {
		return 1 // fallback to root
	}

	fs.ensureNode(re)
	if re.Node != nil {
		return re.Node.ID
	}
	return 1
}

// ensureNode ensures a journal node+edge exists for a resolved entry.
// For pxar-only entries, it creates a node with redirect_to and an edge
// under the parent. For journal entries, it's a no-op.
// Per-path locking prevents duplicate nodes when concurrent FUSE ops
// (e.g. setfacl -R) materialize the same pxar entry simultaneously  -
// analogous to ext4's inode_lock preventing concurrent inode initialization.
func (fs *MutableFS) ensureNode(re *ResolvedEntry) {
	if re.Node != nil {
		return
	}

	// Acquire per-path lock to serialize concurrent ensureNode for the
	// same path. Without this, two concurrent setfacl threads could both
	// resolve the same path, see Node==nil, and create duplicate nodes.
	val, _ := fs.ensureLocks.LoadOrStore(re.Path, &sync.Mutex{})
	pathMu := val
	pathMu.Lock()
	defer pathMu.Unlock()

	// Double-check after acquiring lock  -  another goroutine may have
	if re.Node != nil {
		return
	}
	// Re-resolve: the other goroutine's node is visible via the journal.
	re2, status := fs.resolve(re.Path)
	if status == fuse.OK && re2.Node != nil {
		re.Node = re2.Node
		return
	}

	now := time.Now().UnixNano()
	node := &GraphNode{}
	kind := NodeFile
	if re.IsDir {
		kind = NodeDir
	}
	if re.SymlinkTgt != "" {
		kind = NodeSymlink
	}
	node.Kind = kind
	node.Mode = re.Mode
	node.UID = re.UID
	node.GID = re.GID
	node.Size = re.Size
	node.MtimeNs = re.MtimeNs
	node.CtimeNs = now
	node.HasData = re.DataIsMut
	node.SymlinkTgt = re.SymlinkTgt
	if re.PxarNode != nil {
		node.RedirectTo = re.Path
	}

	nodeID, err := fs.journal.EnsureNodePath(re.Path, node, false)
	if err != nil {
		fs.debugf("ensureNode: EnsureNodePath(%q) failed: %v", re.Path, err)
		return
	}
	node.ID = nodeID
	re.Node = node
}

func (fs *MutableFS) allocInode(isDir bool) uint64 {
	ino := fs.nextIno.Add(1)
	if !isDir {
		ino |= NonDirBit
	}
	return ino
}

func (fs *MutableFS) pathToIno(path string, isDir bool) uint64 {
	if ino, ok := fs.inoLookup.Load(path); ok {
		return ino
	}

	ino := fs.allocInode(isDir)
	fs.mapInode(ino, path)
	return ino
}

func (fs *MutableFS) mapInode(ino uint64, path string) {
	fs.inoLookup.Store(path, ino)
	fs.pathLookup.Store(ino, path)
}

func (fs *MutableFS) unmapInode(path string) {
	if ino, ok := fs.inoLookup.LoadAndDelete(path); ok {
		fs.pathLookup.Delete(ino)
	}
}

func (fs *MutableFS) remapPathPrefix(oldPrefix, newPrefix string) {
	fs.inoLookup.Range(func(p string, ino uint64) bool {
		if p == oldPrefix || strings.HasPrefix(p, oldPrefix+"/") {
			newPath := newPrefix + p[len(oldPrefix):]
			fs.inoLookup.Store(newPath, ino)
			fs.pathLookup.Store(ino, newPath)
			fs.inoLookup.Delete(p)
		}
		return true
	})
}

func (fs *MutableFS) inodeToPath(ino uint64) string {
	path, _ := fs.pathLookup.Load(ino)
	return path
}

func (fs *MutableFS) registerFh(_ string, fd int) uint64 {
	id := fs.nextFh.Add(1)
	fs.handles.Store(id, &passFh{fd: fd})
	return id
}

func (fs *MutableFS) getFh(id uint64) *passFh {
	val, _ := fs.handles.Load(id)
	return val
}

func (fs *MutableFS) getInoLock(ino uint64) *sync.Mutex {
	val, _ := fs.inoLocks.LoadOrStore(ino, &sync.Mutex{})
	return val
}

func (fs *MutableFS) mutablePath(relPath string) string {
	return filepath.Join(fs.mutableDir, relPath)
}

func (fs *MutableFS) dirModeForPath(path string) uint32 {
	re, status := fs.resolve(path)
	if status != fuse.OK {
		return uint32(syscall.S_IFDIR | 0o555)
	}
	return re.Mode | syscall.S_IFDIR
}

func (fs *MutableFS) fillEntryOutForPath(path string, out *fuse.EntryOut) {
	re, status := fs.resolve(path)
	if status != fuse.OK {
		return
	}
	fillResolvedEntryOut(re.Inode, re, out)
}

func (fs *MutableFS) getParentInfo(path string) (parentIno uint64, parentMode uint32) {
	parentDir := filepath.Dir(path)
	if parentDir == "." {
		parentDir = "/"
	}
	parentIno = fs.pathToIno(parentDir, true)

	re, status := fs.resolve(parentDir)
	if status == fuse.OK {
		parentMode = re.Mode | syscall.S_IFDIR
	} else {
		parentMode = uint32(syscall.S_IFDIR | 0o555)
	}
	return
}

func (fs *MutableFS) applyACLOwnership(absPath string) {
	uid := fs.acl.OwnerUID
	gid := fs.acl.OwnerGID
	if uid != 0 || gid != 0 {
		if err := os.Chown(absPath, uid, gid); err != nil {
			fs.logNonFatal("chown", absPath, err)
		}
	}
}

// resetAfterCommit clears in-memory state that became stale after a
// successful commit (journal cleared, pxar reader swapped).
func (fs *MutableFS) resetAfterCommit() {
	fs.dirtyMeta = xsync.NewMap[uint64, pendingMeta]()
}

func (fs *MutableFS) Close() {
	for _, d := range fs.mmapData {
		if err := munmap(d); err != nil {
			fs.logNonFatal("munmap", "data", err)
		}
	}
	fs.mmapData = nil

	fs.handles.Range(func(_ uint64, fh *passFh) bool {
		if err := syscall.Close(fh.fd); err != nil {
			fs.logNonFatal("close-fd", "fd", err)
		}
		return true
	})
	fs.handles = nil
}

func fillResolvedEntryOut(ino uint64, re *ResolvedEntry, out *fuse.EntryOut) {
	out.NodeId = ino
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	out.AttrValidNsec = uint32(time.Second)

	a := &out.Attr
	a.Ino = ino
	a.Size = re.Size
	a.Blocks = (re.Size + 511) / 512
	sec := re.MtimeNs / 1_000_000_000
	nsec := uint32(re.MtimeNs % 1_000_000_000)
	a.Atime = uint64(sec)
	a.Mtime = uint64(sec)
	a.Ctime = uint64(sec)
	a.Atimensec = nsec
	a.Mtimensec = nsec
	a.Ctimensec = nsec
	a.Mode = re.Mode
	if re.IsDir {
		a.Nlink = 2
	} else {
		a.Nlink = 1
	}
	a.Uid = re.UID
	a.Gid = re.GID
	a.Blksize = 4096
}

func fillResolvedAttrOut(re *ResolvedEntry, out *fuse.AttrOut) {
	out.AttrValid = 1
	out.AttrValidNsec = uint32(time.Second)

	a := &out.Attr
	a.Ino = re.Inode
	a.Size = re.Size
	a.Blocks = (re.Size + 511) / 512
	// Atime/Mtime mirror restore's precedence (xattr-derived where the pxar
	// entry carries lastaccesstime/lastwritetime, else Stat.Mtime). Ctime is
	// kernel-owned under restore; report mtime as the closest stable value.
	atimeNs := re.AtimeNs
	if atimeNs == 0 {
		atimeNs = re.MtimeNs
	}
	sec := atimeNs / 1_000_000_000
	nsec := uint32(atimeNs % 1_000_000_000)
	a.Atime = uint64(sec)
	a.Atimensec = nsec
	msec := re.MtimeNs / 1_000_000_000
	mnsec := uint32(re.MtimeNs % 1_000_000_000)
	a.Mtime = uint64(msec)
	a.Mtimensec = mnsec
	a.Ctime = uint64(msec)
	a.Ctimensec = mnsec
	a.Mode = re.Mode
	if re.IsDir {
		a.Nlink = 2
	} else {
		a.Nlink = 1
	}
	a.Uid = re.UID
	a.Gid = re.GID
	a.Blksize = 4096
}

func fillAttrFromNode(attr *fuse.Attr, n *GraphNode) {
	attr.Size = n.Size
	attr.Blocks = (n.Size + 511) / 512
	sec := n.MtimeNs / 1_000_000_000
	nsec := uint32(n.MtimeNs % 1_000_000_000)
	attr.Atime = uint64(sec)
	attr.Mtime = uint64(sec)
	attr.Ctime = uint64(sec)
	attr.Atimensec = nsec
	attr.Mtimensec = nsec
	attr.Ctimensec = nsec
	attr.Mode = ensureModeType(n.Mode, n.Kind)
	if n.Kind == NodeDir {
		attr.Nlink = 2
	} else {
		attr.Nlink = 1
	}
	attr.Uid = n.UID
	attr.Gid = n.GID
	attr.Blksize = 4096
}

func munmap(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return syscall.Munmap(data)
}

func mmapFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if fi.Size() == 0 {
		return nil, nil
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		if _, err2 := f.Seek(0, io.SeekStart); err2 != nil {
			return nil, err
		}
		return io.ReadAll(f)
	}
	return data, nil
}
