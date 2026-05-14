package pxarmount

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
)

// MutableFS implements fuse.RawFileSystem as a layered filesystem:
//   - PxarFS provides the immutable lower layer
//   - Journal provides the SQLite-backed inode graph for the overlay
//   - mutableDir stores file data for copied-up/modified files
//
// The journal uses a graph model (nodes + edges) making rename O(1):
// only the edge row is updated; no descendants are touched.
//
// Resolution:
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
	nextIno uint64
	inoMu   sync.Mutex

	// File handle management.
	handles map[uint64]*passFh
	nextFh  uint64
	fhMu    sync.Mutex

	// Per-inode writer locks.
	inoLocks   map[uint64]*sync.Mutex
	inoLocksMu sync.Mutex

	// mmap'd DIDX data.
	mmapData [][]byte

	origSnapshot  snapshotRef
	pbsStore      string
	origPpxarDidx string

	acl     ACLConfig
	verbose bool
}

func NewMutableFS(pxar *PxarFS, journal *Journal, mutableDir string) *MutableFS {
	return &MutableFS{
		pxar:       pxar,
		journal:    journal,
		mutableDir: mutableDir,
		handles:    make(map[uint64]*passFh),
		inoLocks:   make(map[uint64]*sync.Mutex),
		nextFh:     1,
		nextIno:    2, // 1 is RootInode
	}
}

func (fs *MutableFS) SetSnapshotRef(ref snapshotRef) { fs.origSnapshot = ref }
func (fs *MutableFS) SetACLConfig(cfg ACLConfig)     { fs.acl = cfg }
func (fs *MutableFS) SetVerbose(v bool)              { fs.verbose = v }

func (fs *MutableFS) debugf(format string, args ...any) {
	if fs.verbose {
		fmt.Fprintf(os.Stderr, "  "+format+"\n", args...)
	}
}

func (fs *MutableFS) SetStorePaths(pbsStore, ppxarDidx string) {
	fs.pbsStore = pbsStore
	fs.origPpxarDidx = ppxarDidx
}

// InitMutableRoot ensures the mutable root directory exists.
func (fs *MutableFS) InitMutableRoot() error {
	return os.MkdirAll(fs.mutableDir, 0o755)
}

// ReconcileMutableDir removes orphan disk entries not tracked by journal nodes.
func (fs *MutableFS) ReconcileMutableDir() error {
	// With the graph model, orphan detection means: files on disk whose
	// reconstructed path doesn't match any tracked node. For now we
	// trust the journal — committed nodes are the source of truth.
	// On restart, any disk files under tracked paths are valid.
	return nil
}

// --- FUSE interface ---

func (fs *MutableFS) Init(server *fuse.Server) {
	fs.RawFileSystem = fuse.NewDefaultRawFileSystem()
	fs.RawFileSystem.Init(server)
}

func (fs *MutableFS) String() string    { return "pxar-mutable" }
func (fs *MutableFS) SetDebug(dbg bool) {}

// Lookup resolves a name in a directory.
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

// GetAttr returns attributes.
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

	fillResolvedAttrOut(re, out)
	fs.debugf("GetAttr: ok mode=0%o isDir=%v", out.Attr.Mode, re.IsDir)
	return fuse.OK
}

func (fs *MutableFS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	return fuse.OK
}
func (fs *MutableFS) ReleaseDir(input *fuse.ReleaseIn) {}
func (fs *MutableFS) FsyncDir(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	return fuse.OK
}

// ReadDir merges immutable (pxar) and mutable (journal edges) entries.
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

	// Get pxar entries (unless opaque).
	var pxarEntries []dirEntrySlim
	if !isOpaque && pxarDirPath != "" {
		pxarNode := fs.findPxarNode(pxarDirPath)
		if pxarNode != nil && pxarNode.isDir {
			pxarEntries, _ = fs.pxar.ReadDirRaw(pxarNode.inode)
		}
	}

	// Get journal edges for this parent.
	edgeNames := make(map[string]int64) // name → child node ID
	whiteoutNames := make(map[string]bool)
	if parentNodeID != 0 {
		edges, _ := fs.journal.ListEdges(parentNodeID)
		for _, e := range edges {
			edgeNames[e.Name] = e.ChildID
		}
		wos, _ := fs.journal.ListWhiteouts(parentNodeID)
		for _, w := range wos {
			whiteoutNames[w] = true
		}
	}

	// Merge: pxar entries minus whiteouts/edges, plus journal edges.
	type mergedEntry struct {
		name  string
		ino   uint64
		mode  uint32
		isDir bool
	}
	var merged []mergedEntry

	pxarParentIno := uint64(0)
	if pxarDirPath != "" {
		if pn := fs.findPxarNode(pxarDirPath); pn != nil {
			pxarParentIno = pn.inode
		}
	}

	for _, pe := range pxarEntries {
		if whiteoutNames[pe.name] {
			continue
		}
		if _, ok := edgeNames[pe.name]; ok {
			continue
		}
		if pxarParentIno != 0 {
			fs.pxar.RegisterSlimNode(&pe, pxarParentIno)
		}
		childPath := joinPath(parentPath, pe.name)
		ino := fs.pathToIno(childPath, pe.isDir)
		merged = append(merged, mergedEntry{
			name: pe.name, ino: ino, mode: pe.mode, isDir: pe.isDir,
		})
	}

	for name, nodeID := range edgeNames {
		// Edges take priority over whiteouts — if there's a journal node,
		// it's always visible.
		node, _ := fs.journal.GetNode(nodeID)
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

	// Emit entries.
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

// Open opens a file. For writes, triggers copy-up.
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

// Read reads data from the appropriate source.
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

// Write writes data. Triggers copy-up if needed.
func (fs *MutableFS) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (uint32, fuse.Status) {
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
	if fh == nil {
		abs := fs.mutablePath(path)
		fd, err := syscall.Open(abs, os.O_WRONLY, 0)
		if err != nil {
			return 0, fuse.ToStatus(err)
		}
		fhID := fs.registerFh(path, fd)
		fh = fs.getFh(fhID)
		if fh == nil {
			return 0, fuse.EBADF
		}
	}

	n, err := syscall.Pwrite(fh.fd, data, int64(input.Offset))
	if err != nil {
		return 0, fuse.ToStatus(err)
	}

	// Update journal node metadata after write.
	now := time.Now().UnixNano()
	re.Size = uint64(int64(input.Offset) + int64(n))
	re.MtimeNs = now
	re.CtimeNs = now
	if re.Node != nil {
		re.Node.Size = re.Size
		re.Node.MtimeNs = now
		re.Node.CtimeNs = now
		if err := fs.journal.UpdateNode(re.Node); err != nil {
			return 0, fuse.EIO
		}
	}

	return uint32(n), fuse.OK
}

// SetAttr applies metadata changes.
func (fs *MutableFS) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {
	path := fs.inodeToPath(input.NodeId)
	if path == "" {
		return fuse.ENOENT
	}

	re, status := fs.resolve(path)
	if status != fuse.OK {
		return status
	}

	if v, ok := input.GetMode(); ok {
		re.Mode = uint32(v)
	}
	if v, ok := input.GetUID(); ok {
		re.UID = uint32(v)
	}
	if v, ok := input.GetGID(); ok {
		re.GID = uint32(v)
	}
	if v, ok := input.GetSize(); ok {
		re.Size = v
		if re.DataIsMut {
			_ = os.Truncate(fs.mutablePath(path), int64(v))
		}
	}
	if a, ok := input.GetATime(); ok {
		re.CtimeNs = a.UnixNano()
	}
	if m, ok := input.GetMTime(); ok {
		re.MtimeNs = m.UnixNano()
	}

	// Apply to mutable data if present.
	if re.DataIsMut {
		abs := fs.mutablePath(path)
		if m, ok := input.GetMode(); ok {
			_ = unix.Chmod(abs, m)
		}
		uid, gid := -1, -1
		if u, ok := input.GetUID(); ok {
			uid = int(u)
		}
		if g, ok := input.GetGID(); ok {
			gid = int(g)
		}
		if uid != -1 || gid != -1 {
			_ = unix.Lchown(abs, uid, gid)
		}
		if atime, aok := input.GetATime(); aok {
			if mtime, mok := input.GetMTime(); mok {
				tv := []unix.Timeval{
					{Sec: atime.Unix(), Usec: int64(atime.Nanosecond() / 1000)},
					{Sec: mtime.Unix(), Usec: int64(mtime.Nanosecond() / 1000)},
				}
				_ = unix.Lutimes(abs, tv)
			}
		}
	}

	// Update journal node.
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

// Create creates a new file.
func (fs *MutableFS) Create(cancel <-chan struct{}, input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	parentPath := fs.inodeToPath(input.NodeId)
	childPath := joinPath(parentPath, name)

	abs := fs.mutablePath(childPath)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return fuse.ToStatus(err)
	}

	fd, err := syscall.Open(abs, int(input.Flags)|os.O_CREATE|os.O_EXCL, uint32(input.Mode&0o777))
	if err != nil {
		return fuse.ToStatus(err)
	}

	ino := fs.pathToIno(childPath, false)

	now := time.Now().UnixNano()
	node := &GraphNode{
		Kind:    NodeFile,
		Mode:    uint32(syscall.S_IFREG) | uint32(input.Mode&0o777),
		UID:     input.Uid,
		GID:     input.Gid,
		Size:    0,
		MtimeNs: now,
		CtimeNs: now,
		HasData: true,
	}

	// Get parent node ID for the edge.
	parentID := fs.resolveParentNodeID(parentPath)

	nodeID, err := fs.journal.CreateNode(node)
	if err != nil {
		syscall.Close(fd)
		os.Remove(abs)
		fs.unmapInode(childPath)
		return fuse.EIO
	}
	node.ID = nodeID

	// Create edge.
	if err := fs.journal.CreateEdge(parentID, name, nodeID); err != nil {
		syscall.Close(fd)
		os.Remove(abs)
		fs.journal.DeleteNode(nodeID)
		fs.unmapInode(childPath)
		return fuse.EIO
	}

	// If shadowing a pxar entry, add whiteout.
	if fs.hasPxarEntry(childPath) {
		if err := fs.journal.AddWhiteout(parentID, name); err != nil {
			// Not fatal — edge already created.
		}
	}

	fs.applyACLOwnership(abs, false)

	fhID := fs.registerFh(childPath, fd)

	out.NodeId = ino
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	out.Fh = fhID
	out.OpenFlags = fuse.FOPEN_KEEP_CACHE
	fillAttrFromNode(&out.Attr, node)
	out.Attr.Ino = ino
	return fuse.OK
}

// Mkdir creates a directory.
func (fs *MutableFS) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) fuse.Status {
	parentPath := fs.inodeToPath(input.NodeId)
	childPath := joinPath(parentPath, name)

	abs := fs.mutablePath(childPath)
	if err := syscall.Mkdir(abs, input.Mode&0o777); err != nil {
		return fuse.ToStatus(err)
	}

	fs.applyACLOwnership(abs, true)

	hasPxar := fs.hasPxarEntry(childPath)
	now := time.Now().UnixNano()
	node := &GraphNode{
		Kind:    NodeDir,
		Mode:    uint32(input.Mode&0o777) | syscall.S_IFDIR,
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

	nodeID, err := fs.journal.CreateNode(node)
	if err != nil {
		os.Remove(abs)
		return fuse.EIO
	}
	node.ID = nodeID

	if err := fs.journal.CreateEdge(parentID, name, nodeID); err != nil {
		os.Remove(abs)
		fs.journal.DeleteNode(nodeID)
		return fuse.EIO
	}

	// If shadowing pxar, add whiteout to hide the pxar name.
	if hasPxar {
		_ = fs.journal.AddWhiteout(parentID, name)
	}

	ino := fs.pathToIno(childPath, true)

	out.NodeId = ino
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	fillAttrFromNode(&out.Attr, node)
	out.Attr.Ino = ino
	return fuse.OK
}

// Mknod creates a device/special node.
func (fs *MutableFS) Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) fuse.Status {
	parentPath := fs.inodeToPath(input.NodeId)
	childPath := joinPath(parentPath, name)

	abs := fs.mutablePath(childPath)
	if err := syscall.Mknod(abs, input.Mode, int(input.Rdev)); err != nil {
		return fuse.ToStatus(err)
	}

	fs.applyACLOwnership(abs, false)

	hasPxar := fs.hasPxarEntry(childPath)
	now := time.Now().UnixNano()
	node := &GraphNode{
		Kind:    NodeFile,
		Mode:    uint32(input.Mode),
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

	nodeID, err := fs.journal.CreateNode(node)
	if err != nil {
		os.Remove(abs)
		return fuse.EIO
	}
	node.ID = nodeID

	if err := fs.journal.CreateEdge(parentID, name, nodeID); err != nil {
		os.Remove(abs)
		fs.journal.DeleteNode(nodeID)
		return fuse.EIO
	}

	if hasPxar {
		_ = fs.journal.AddWhiteout(parentID, name)
	}

	ino := fs.pathToIno(childPath, false)

	out.NodeId = ino
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	fillAttrFromNode(&out.Attr, node)
	out.Attr.Ino = ino
	return fuse.OK
}

// Symlink creates a symlink.
func (fs *MutableFS) Symlink(cancel <-chan struct{}, header *fuse.InHeader, target string, linkName string, out *fuse.EntryOut) fuse.Status {
	parentPath := fs.inodeToPath(header.NodeId)
	childPath := joinPath(parentPath, linkName)

	abs := fs.mutablePath(childPath)
	if err := syscall.Symlink(target, abs); err != nil {
		return fuse.ToStatus(err)
	}

	fs.applyACLOwnership(abs, false)

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

	nodeID, err := fs.journal.CreateNode(node)
	if err != nil {
		os.Remove(abs)
		return fuse.EIO
	}
	node.ID = nodeID

	if err := fs.journal.CreateEdge(parentID, linkName, nodeID); err != nil {
		os.Remove(abs)
		fs.journal.DeleteNode(nodeID)
		return fuse.EIO
	}

	if hasPxar {
		_ = fs.journal.AddWhiteout(parentID, linkName)
	}

	ino := fs.pathToIno(childPath, false)

	out.NodeId = ino
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	fillAttrFromNode(&out.Attr, node)
	out.Attr.Ino = ino
	return fuse.OK
}

// Unlink removes a file.
func (fs *MutableFS) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	parentPath := fs.inodeToPath(header.NodeId)
	childPath := joinPath(parentPath, name)

	re, status := fs.resolve(childPath)
	if status != fuse.OK {
		return status
	}

	// Journal-first for destructive ops.
	parentID := fs.resolveParentNodeID(parentPath)

	if re.Node != nil {
		// Delete the edge. If pxar counterpart exists, add whiteout.
		if err := fs.journal.DeleteEdge(parentID, name); err != nil {
			return fuse.EIO
		}
		if re.PxarNode != nil || fs.hasPxarEntry(childPath) {
			if err := fs.journal.AddWhiteout(parentID, name); err != nil {
				return fuse.EIO
			}
		}
		// Delete the node (cascade removes xattrs).
		_ = fs.journal.DeleteNode(re.Node.ID)
	} else if re.PxarNode != nil {
		// Pure pxar deletion: just add whiteout.
		if err := fs.journal.AddWhiteout(parentID, name); err != nil {
			return fuse.EIO
		}
	}

	// Remove mutable data.
	if re.DataIsMut {
		_ = os.Remove(fs.mutablePath(childPath))
	}

	fs.unmapInode(childPath)
	return fuse.OK
}

// Rmdir removes a directory.
func (fs *MutableFS) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	return fs.Unlink(cancel, header, name)
}

// Rename moves/renames a file or directory. O(1) — updates one edge row.
func (fs *MutableFS) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName string, newName string) fuse.Status {
	oldParentPath := fs.inodeToPath(input.NodeId)
	newParentPath := fs.inodeToPath(input.Newdir)
	oldPath := joinPath(oldParentPath, oldName)
	newPath := joinPath(newParentPath, newName)

	oldRE, oldStatus := fs.resolve(oldPath)
	if oldStatus != fuse.OK {
		return oldStatus
	}

	// Get parent node IDs.
	oldParentID := fs.resolveParentNodeID(oldParentPath)
	newParentID := fs.resolveParentNodeID(newParentPath)

	// Move mutable disk data if present.
	if oldRE.DataIsMut {
		oldAbs := fs.mutablePath(oldPath)
		if _, err := os.Stat(oldAbs); err == nil {
			newAbs := fs.mutablePath(newPath)
			if err := os.MkdirAll(filepath.Dir(newAbs), 0o755); err != nil {
				return fuse.ToStatus(err)
			}
			if err := os.Rename(oldAbs, newAbs); err != nil {
				return fuse.ToStatus(err)
			}
		}
	}

	// Dest has a pxar entry → whiteout it.
	destHasPXar := fs.hasPxarEntry(newPath)

	if oldRE.Node != nil {
		// Source has a journal node: move the edge.
		if err := fs.journal.MoveEdge(oldParentID, oldName, newParentID, newName); err != nil {
			if oldRE.DataIsMut {
				_ = os.Rename(fs.mutablePath(newPath), fs.mutablePath(oldPath))
			}
			return fuse.EIO
		}
	} else {
		// Source is pxar-only: create a journal node + edge, whiteout old location.
		now := time.Now().UnixNano()
		node := &GraphNode{
			Kind:       nodeKindFromPxar(oldRE.PxarNode),
			Mode:       statMode(oldRE.PxarNode.mode),
			UID:        oldRE.PxarNode.uid,
			GID:        oldRE.PxarNode.gid,
			Size:       oldRE.PxarNode.fileSize,
			MtimeNs:    int64(oldRE.PxarNode.mtimeSecs)*1e9 + int64(oldRE.PxarNode.mtimeNanos),
			CtimeNs:    now,
			HasData:    false,
			RedirectTo: oldPath, // pxar source path
			SymlinkTgt: oldRE.SymlinkTgt,
		}

		nodeID, err := fs.journal.CreateNode(node)
		if err != nil {
			return fuse.EIO
		}
		node.ID = nodeID

		if err := fs.journal.CreateEdge(newParentID, newName, nodeID); err != nil {
			fs.journal.DeleteNode(nodeID)
			return fuse.EIO
		}

		// Whiteout old location.
		if err := fs.journal.AddWhiteout(oldParentID, oldName); err != nil {
			return fuse.EIO
		}
	}

	// Whiteout dest if it had a pxar entry and we're not moving back
	// to a location where the pxar data is the source.
	if destHasPXar {
		// If source is a journal node with RedirectTo matching the dest path,
		// it's moving back home — remove any existing whiteout at dest.
		if oldRE.Node != nil && oldRE.Node.RedirectTo == newPath {
			_ = fs.journal.RemoveWhiteout(newParentID, newName)
		} else {
			_ = fs.journal.AddWhiteout(newParentID, newName)
		}
	} else {
		// No pxar at dest — make sure there's no stale whiteout.
		_ = fs.journal.RemoveWhiteout(newParentID, newName)
	}

	// Remove whiteout at old location if it was purely a journal move
	// (the old location might have had a pxar entry that we whiteout'd before).
	// Only keep the old whiteout if there's actually a pxar file there to hide.
	if oldRE.Node != nil && !fs.hasPxarEntry(oldPath) {
		_ = fs.journal.RemoveWhiteout(oldParentID, oldName)
	}

	// Update inode mapping.
	ino := fs.pathToIno(oldPath, oldRE.IsDir)
	fs.unmapInode(oldPath)
	fs.mapInode(ino, newPath)

	return fuse.OK
}

// nodeKindFromPxar returns the journal node kind for a pxar node.
func nodeKindFromPxar(n *node) uint8 {
	if n.isDir {
		return NodeDir
	}
	if n.isSymlink {
		return NodeSymlink
	}
	return NodeFile
}

// Readlink resolves and returns the symlink target.
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

// --- xattr ---

func (fs *MutableFS) GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, dest []byte) (uint32, fuse.Status) {
	path := fs.inodeToPath(header.NodeId)
	if path == "" {
		return 0, fuse.ENOENT
	}

	re, _ := fs.resolve(path)
	if status, handled := fs.resolveCheck(path, re); !handled {
		return 0, status
	}

	// Check journal xattrs first (if we have a node).
	if re.Node != nil {
		val, err := fs.journal.GetXAttr(re.Node.ID, attr)
		if err != nil {
			return 0, fuse.EIO
		}
		if val != nil {
			return xattrValue(val, dest)
		}
	}

	// Check mutable data xattrs.
	if re.DataIsMut {
		abs := fs.mutablePath(path)
		sz, xerr := unix.Getxattr(abs, attr, dest)
		if xerr == nil {
			return uint32(sz), fuse.OK
		}
	}

	// Fall back to pxar.
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

	// Journal xattrs.
	if re.Node != nil {
		names, _ := fs.journal.ListXAttrs(re.Node.ID)
		for _, n := range names {
			nameSet[n] = true
		}
	}

	// Pxar xattrs.
	if re.PxarNode != nil {
		pxarHeader := *header
		pxarHeader.NodeId = re.PxarNode.inode
		sz, _ := fs.pxar.ListXAttr(cancel, &pxarHeader, nil)
		_ = sz
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

	// Also apply to mutable data.
	if re.DataIsMut {
		abs := fs.mutablePath(path)
		flags := 0
		if input.Flags&XattrCreate != 0 {
			flags = unix.XATTR_CREATE
		} else if input.Flags&XattrReplace != 0 {
			flags = unix.XATTR_REPLACE
		}
		_ = unix.Setxattr(abs, attr, data, flags)
	}

	return fuse.OK
}

func (fs *MutableFS) RemoveXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string) fuse.Status {
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
		_ = unix.Removexattr(fs.mutablePath(path), attr)
	}

	return fuse.OK
}

// --- file handle lifecycle ---

func (fs *MutableFS) Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status {
	if input.Fh == 0 {
		return fuse.OK // pxar passthrough, no fd to sync
	}
	return fs.fsyncInternal(input.NodeId, input.Fh)
}

func (fs *MutableFS) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	if input.Fh == 0 {
		return fuse.OK
	}
	return fs.fsyncInternal(input.NodeId, input.Fh)
}

func (fs *MutableFS) fsyncInternal(nodeID, fhID uint64) fuse.Status {
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
	fs.fhMu.Lock()
	if fh, ok := fs.handles[input.Fh]; ok {
		_ = syscall.Close(fh.fd)
		delete(fs.handles, input.Fh)
	}
	fs.fhMu.Unlock()
}

// --- unsupported ops ---

func (fs *MutableFS) Link(cancel <-chan struct{}, input *fuse.LinkIn, name string, out *fuse.EntryOut) fuse.Status {
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

// --- copy-up ---

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
		return fmt.Errorf("copyUp: could not create node for %s", re.Path)
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

	fs.applyACLOwnership(abs, re.IsDir)
	return nil
}

func (fs *MutableFS) copyUpRegularFile(path string, n *node) error {
	abs := fs.mutablePath(path)
	f, err := os.OpenFile(abs, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	entry, err := fs.pxar.GetPxarEntry(n.inode)
	if err != nil {
		return err
	}

	rc, err := fs.pxar.Reader().ReadFileContentReader(entry)
	if err != nil {
		return err
	}
	defer rc.Close()

	bufp := copyBufPool.Get().(*[]byte)
	defer copyBufPool.Put(bufp)
	if _, err := io.CopyBuffer(f, rc, *bufp); err != nil {
		return err
	}

	mode := os.FileMode(statMode(n.mode) & 0o7777)
	_ = os.Chmod(abs, mode)
	return nil
}

// --- resolution ---

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
		MtimeNs:   int64(pxarNode.mtimeSecs)*1e9 + int64(pxarNode.mtimeNanos),
		CtimeNs:   int64(pxarNode.mtimeSecs)*1e9 + int64(pxarNode.mtimeNanos),
	}
	re.Inode = fs.pathToIno("/", true)
	return re, fuse.OK
}

func (fs *MutableFS) resolve(path string) (*ResolvedEntry, fuse.Status) {
	// Root is always a pxar-only directory.
	if path == "/" || path == "" {
		return fs.resolveRoot()
	}
	nodeID, pxarPath, fellOffAt, remaining, err := fs.journal.ResolvePath(path)
	if err != nil {
		fs.debugf("resolve(%q) ResolvePath err: %v", path, err)
		return nil, fuse.EIO
	}

	// Whiteout detected.
	if nodeID == 0 && pxarPath == "" {
		return nil, fuse.ENOENT
	}

	// Full graph match.
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

	// Fell off graph — check for pxar entry at the resolved pxar path.
	// First check if any part of the remaining path hits a pxar whiteout.
	if fellOffAt != 0 {
		parts := stringsSplit(remaining, "/")
		_ = parts
	}

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
		MtimeNs:   int64(pxarNode.mtimeSecs)*1e9 + int64(pxarNode.mtimeNanos),
		CtimeNs:   int64(pxarNode.mtimeSecs)*1e9 + int64(pxarNode.mtimeNanos),
	}
	re.Inode = fs.pathToIno(path, re.IsDir)
	return re, fuse.OK
}

func stringsSplit(s, sep string) []string {
	parts := []string{}
	for i := 0; i < len(s); {
		j := i
		for j < len(s) && s[j] != '/' {
			j++
		}
		if j > i {
			parts = append(parts, s[i:j])
		}
		i = j + 1
	}
	return parts
}

// resolveCheck is a helper for xattr ops that returns (status, ok) where
// ok=false means the caller should return status immediately.
func (fs *MutableFS) resolveCheck(path string, re *ResolvedEntry) (fuse.Status, bool) {
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
				fs.pxar.RegisterSlimNode(&e, curIno)
				if i == len(parts)-1 {
					return fs.pxar.GetNode(e.inode)
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
		return 1 // root
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
func (fs *MutableFS) ensureNode(re *ResolvedEntry) {
	if re.Node != nil {
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

	nodeID, err := fs.journal.CreateNode(node)
	if err != nil {
		return
	}
	node.ID = nodeID

	// Create edges for any parent nodes that don't already have them.
	// Walk path from root, creating edges and intermediate nodes as needed.
	parts := splitPath(re.Path)
	curParentID := int64(1) // root

	for i, name := range parts {
		if name == "" {
			continue
		}
		childID, err := fs.journal.LookupEdge(curParentID, name)
		if err != nil {
			return
		}
		if childID != 0 {
			curParentID = childID
			continue
		}

		if i == len(parts)-1 {
			// This is the target — use our node.
			if err := fs.journal.CreateEdge(curParentID, name, nodeID); err != nil {
				return
			}
			// Add whiteout if shadowing pxar.
			if re.PxarNode != nil {
				_ = fs.journal.AddWhiteout(curParentID, name)
			}
		} else {
			// Intermediate directory — create inline.
			var intermediatePath strings.Builder
			intermediatePath.WriteString("/" + parts[0])
			for j := 1; j <= i; j++ {
				intermediatePath.WriteString("/" + parts[j])
			}
			intermediate := &GraphNode{
				Kind:       NodeDir,
				Mode:       syscall.S_IFDIR | 0o755,
				UID:        node.UID,
				GID:        node.GID,
				Size:       0,
				MtimeNs:    now,
				CtimeNs:    now,
				RedirectTo: intermediatePath.String(),
			}
			midID, err := fs.journal.CreateNode(intermediate)
			if err != nil {
				return
			}
			intermediate.ID = midID
			if err := fs.journal.CreateEdge(curParentID, name, midID); err != nil {
				return
			}
			curParentID = midID
		}

		// Check pxar whiteouts.
		isWO, _ := fs.journal.IsWhiteout(curParentID, name)
		if isWO {
			_ = fs.journal.RemoveWhiteout(curParentID, name)
		}
	}

	re.Node = node
}

// --- inode management ---

var (
	pathToIno = make(map[string]uint64)
	inoToPath = make(map[uint64]string)
	pathInoMu sync.RWMutex
)

func (fs *MutableFS) allocInode(isDir bool) uint64 {
	fs.inoMu.Lock()
	ino := fs.nextIno
	fs.nextIno++
	fs.inoMu.Unlock()
	if !isDir {
		ino |= NonDirBit
	}
	return ino
}

func (fs *MutableFS) pathToIno(path string, isDir bool) uint64 {
	pathInoMu.RLock()
	if ino, ok := pathToIno[path]; ok {
		pathInoMu.RUnlock()
		return ino
	}
	pathInoMu.RUnlock()

	ino := fs.allocInode(isDir)
	fs.mapInode(ino, path)
	return ino
}

func (fs *MutableFS) mapInode(ino uint64, path string) {
	pathInoMu.Lock()
	pathToIno[path] = ino
	inoToPath[ino] = path
	pathInoMu.Unlock()
}

func (fs *MutableFS) unmapInode(path string) {
	pathInoMu.Lock()
	if ino, ok := pathToIno[path]; ok {
		delete(inoToPath, ino)
		delete(pathToIno, path)
	}
	pathInoMu.Unlock()
}

func (fs *MutableFS) inodeToPath(ino uint64) string {
	pathInoMu.RLock()
	defer pathInoMu.RUnlock()
	return inoToPath[ino]
}

// --- file handle management ---

func (fs *MutableFS) registerFh(path string, fd int) uint64 {
	fs.fhMu.Lock()
	id := fs.nextFh
	fs.nextFh++
	fs.handles[id] = &passFh{fd: fd, path: path}
	fs.fhMu.Unlock()
	return id
}

func (fs *MutableFS) getFh(id uint64) *passFh {
	fs.fhMu.Lock()
	defer fs.fhMu.Unlock()
	return fs.handles[id]
}

// --- per-inode writer lock ---

func (fs *MutableFS) getInoLock(ino uint64) *sync.Mutex {
	fs.inoLocksMu.Lock()
	defer fs.inoLocksMu.Unlock()
	if m, ok := fs.inoLocks[ino]; ok {
		return m
	}
	m := &sync.Mutex{}
	fs.inoLocks[ino] = m
	return m
}

// --- helpers ---

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

func (fs *MutableFS) applyACLOwnership(absPath string, isDir bool) {
	_ = isDir
	uid := fs.acl.OwnerUID
	gid := fs.acl.OwnerGID
	if uid != 0 || gid != 0 {
		_ = os.Chown(absPath, uid, gid)
	}
}

func (fs *MutableFS) ForceACLOwnership() {
	if !fs.acl.ForceOwner && !fs.acl.ForceGroup {
		return
	}
	uid := fs.acl.OwnerUID
	gid := fs.acl.OwnerGID
	if uid == 0 && gid == 0 {
		return
	}
	_ = filepath.Walk(fs.mutableDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		_ = os.Chown(path, uid, gid)
		return nil
	})
}

func (fs *MutableFS) Close() {
	for _, d := range fs.mmapData {
		_ = munmap(d)
	}
	fs.mmapData = nil

	fs.fhMu.Lock()
	for _, fh := range fs.handles {
		_ = syscall.Close(fh.fd)
	}
	fs.handles = nil
	fs.fhMu.Unlock()
}

// --- fill helpers ---

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

// splitPath splits a path into components.
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
	defer f.Close()

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
