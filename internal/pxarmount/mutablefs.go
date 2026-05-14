package pxarmount

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
)

// MutableFS implements fuse.RawFileSystem as a layered filesystem:
//   - PxarFS provides the immutable lower layer
//   - Journal provides the authoritative metadata record
//   - mutableDir stores file data for copied-up/modified files
//
// Resolution logic (lookup):
//
//	je = journal.get(path)
//	if je.state == whiteout: ENOENT
//	if je exists:
//	    meta = je (overrides everything)
//	    data_source = mutable if je.has_data else immutable
//	else:
//	    meta = immutable path or ENOENT
//	    data_source = immutable
type MutableFS struct {
	fuse.RawFileSystem

	pxar       *PxarFS
	journal    *Journal
	mutableDir string // backing directory for mutable file data

	// Inode allocation for new/mutable entries.
	nextIno uint64
	inoMu   sync.Mutex

	// File handle management.
	handles map[uint64]*passFh
	nextFh  uint64
	fhMu    sync.Mutex

	// Per-inode writer locks for write/truncate serialization.
	inoLocks   map[uint64]*sync.Mutex
	inoLocksMu sync.Mutex

	// mmap'd DIDX data to release on close.
	mmapData [][]byte

	// snapshot ref for commit.
	origSnapshot  snapshotRef
	pbsStore      string
	origPpxarDidx string

	acl     ACLConfig
	verbose bool
}

// NewMutableFS creates a new MutableFS.
func NewMutableFS(pxar *PxarFS, journal *Journal, mutableDir string) *MutableFS {
	fs := &MutableFS{
		pxar:       pxar,
		journal:    journal,
		mutableDir: mutableDir,
		handles:    make(map[uint64]*passFh),
		inoLocks:   make(map[uint64]*sync.Mutex),
		nextFh:     1, // 0 reserved for lazy-open sentinel
		nextIno:    2, // 1 is RootInode
	}
	return fs
}

// SetSnapshotRef sets the snapshot identity for commit.
func (fs *MutableFS) SetSnapshotRef(ref snapshotRef) {
	fs.origSnapshot = ref
}

// SetACLConfig sets the ACL ownership config.
func (fs *MutableFS) SetACLConfig(cfg ACLConfig) {
	fs.acl = cfg
}

// SetStorePaths sets PBS store and DIDX paths for commit.
func (fs *MutableFS) SetStorePaths(pbsStore, ppxarDidx string) {
	fs.pbsStore = pbsStore
	fs.origPpxarDidx = ppxarDidx
}

// SetVerbose enables diagnostic output.
func (fs *MutableFS) SetVerbose(v bool) {
	fs.verbose = v
}

// InitMutableRoot ensures the mutable root directory exists.
func (fs *MutableFS) InitMutableRoot() error {
	return os.MkdirAll(fs.mutableDir, 0o755)
}

// ReconcileMutableDir removes orphan disk entries not tracked by the journal.
// Called on startup to ensure disk and journal stay in sync.
func (fs *MutableFS) ReconcileMutableDir() error {
	entries, err := os.ReadDir(fs.mutableDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		if e.Name() == JournalDir {
			continue
		}
		relPath := "/" + e.Name()
		je, jerr := fs.journal.GetEntry(relPath)
		if jerr != nil {
			continue
		}
		if je == nil {
			// Orphan: exists on disk but not in journal.
			os.RemoveAll(filepath.Join(fs.mutableDir, relPath))
			if fs.verbose {
				fmt.Fprintf(os.Stderr, "  reconciled orphan: %s\n", relPath)
			}
		}
	}
	// Recurse into subdirectories.
	return fs.reconcileDir("")
}

func (fs *MutableFS) reconcileDir(relDir string) error {
	absDir := filepath.Join(fs.mutableDir, relDir)
	entries, err := os.ReadDir(absDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		if e.Name() == JournalDir && relDir == "" {
			continue
		}
		relPath := joinPath(relDir, e.Name())
		if relDir == "" || relDir == "/" {
			relPath = "/" + e.Name()
		}
		je, jerr := fs.journal.GetEntry(relPath)
		if jerr != nil {
			continue
		}
		if je == nil {
			os.RemoveAll(filepath.Join(fs.mutableDir, relPath))
			if fs.verbose {
				fmt.Fprintf(os.Stderr, "  reconciled orphan: %s\n", relPath)
			}
			continue
		}
		if e.IsDir() {
			if err := fs.reconcileDir(relPath); err != nil {
				return err
			}
		}
	}
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
	if name == JournalDir {
		return fuse.ENOENT
	}

	parentPath := fs.inodeToPath(header.NodeId)
	childPath := joinPath(parentPath, name)

	re, status := fs.resolve(childPath)
	if status != fuse.OK {
		return status
	}

	ino := fs.pathToIno(childPath, re.IsDir)
	fillResolvedEntryOut(ino, re, out)
	return fuse.OK
}

// GetAttr returns attributes for a node, resolving via journal → pxar.
func (fs *MutableFS) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	path := fs.inodeToPath(input.NodeId)
	if path == "" {
		return fuse.ENOENT
	}

	re, status := fs.resolve(path)
	if status != fuse.OK {
		return status
	}

	fillResolvedAttrOut(re, out)
	return fuse.OK
}

// OpenDir is a no-op for the mutable layer.
func (fs *MutableFS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	return fuse.OK
}

// ReleaseDir is a no-op.
func (fs *MutableFS) ReleaseDir(input *fuse.ReleaseIn) {}

// FsyncDir is a no-op.
func (fs *MutableFS) FsyncDir(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	return fuse.OK
}

// ReadDir merges immutable (pxar) and mutable (journal) directory entries,
// subtracting whiteouts and respecting opaque flags.
func (fs *MutableFS) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	return fs.readDirImpl(input, out, false)
}

// ReadDirPlus merges and includes entry attributes.
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

	// Check if this directory is opaque: if so, don't merge pxar children.
	isOpaque, _ := fs.journal.IsOpaque(parentPath)

	// Get pxar entries (immutable layer).
	// Resolve the mutable inode to the pxar inode for this path.
	var pxarEntries []dirEntrySlim
	if !isOpaque {
		pxarNode := fs.findPxarNode(parentPath)
		if pxarNode != nil && pxarNode.isDir {
			pxarEntries, _ = fs.pxar.ReadDirRaw(pxarNode.inode)
		}
	}

	// Get journal entries (mutable layer).
	journalEntries, _ := fs.journal.ListDir(parentPath)

	// Build name sets for dedup.
	whiteouts := make(map[string]bool)
	journalNames := make(map[string]*JournalEntry)
	for i := range journalEntries {
		je := &journalEntries[i]
		child := filepath.Base(je.Path)
		if je.State == StateWhiteout {
			whiteouts[child] = true
		} else {
			journalNames[child] = je
		}
	}

	// Merge: pxar entries minus whiteouts, plus journal entries.
	type mergedEntry struct {
		name  string
		ino   uint64
		mode  uint32
		isDir bool
	}
	var merged []mergedEntry

	// Determine the pxar parent inode for RegisterSlimNode.
	pxarParent := fs.findPxarNode(parentPath)
	pxarParentIno := uint64(0)
	if pxarParent != nil {
		pxarParentIno = pxarParent.inode
	}

	for _, pe := range pxarEntries {
		if whiteouts[pe.name] {
			continue
		}
		if _, overridden := journalNames[pe.name]; overridden {
			continue
		}
		if pxarParentIno != 0 {
			fs.pxar.RegisterSlimNode(&pe, pxarParentIno)
		}
		merged = append(merged, mergedEntry{
			name:  pe.name,
			ino:   pe.inode,
			mode:  pe.mode,
			isDir: pe.isDir,
		})
	}

	for name, je := range journalNames {
		if whiteouts[name] {
			continue
		}
		ino := fs.pathToIno(je.Path, je.Mode&syscall.S_IFDIR != 0)
		merged = append(merged, mergedEntry{
			name:  name,
			ino:   ino,
			mode:  je.Mode,
			isDir: je.Mode&syscall.S_IFDIR != 0,
		})
	}

	// Emit entries.
	if input.Offset == 0 {
		dirMode := fs.dirModeForPath(parentPath)
		if plus {
			eo := out.AddDirLookupEntry(fuse.DirEntry{Name: ".", Ino: input.NodeId, Mode: dirMode})
			if eo == nil {
				return fuse.OK
			}
			fs.fillEntryOutForPath(parentPath, eo)
		} else {
			if !out.AddDirEntry(fuse.DirEntry{Name: ".", Ino: input.NodeId, Mode: dirMode}) {
				return fuse.OK
			}
		}
	}

	if input.Offset <= 1 {
		parentIno, parentMode := fs.getParentInfo(parentPath)
		if plus {
			eo := out.AddDirLookupEntry(fuse.DirEntry{Name: "..", Ino: parentIno, Mode: parentMode})
			if eo != nil {
				parentPathDir := filepath.Dir(parentPath)
				if parentPathDir == "." {
					parentPathDir = "/"
				}
				fs.fillEntryOutForPath(parentPathDir, eo)
			}
		} else {
			out.AddDirEntry(fuse.DirEntry{Name: "..", Ino: parentIno, Mode: parentMode})
		}
	}

	start := max(int(input.Offset)-2, 0)
	for i := start; i < len(merged); i++ {
		de := fuse.DirEntry{
			Name: merged[i].name,
			Ino:  merged[i].ino,
			Mode: merged[i].mode,
		}
		if plus {
			eo := out.AddDirLookupEntry(de)
			if eo == nil {
				break
			}
			childPath := joinPath(parentPath, merged[i].name)
			fs.fillEntryOutForPath(childPath, eo)
		} else {
			if !out.AddDirEntry(de) {
				break
			}
		}
	}
	return fuse.OK
}

// Open opens a file.
// O_RDONLY → opens from data source (mutable or immutable)
// O_WRONLY/RDWR → triggers copy-up if has_data == 0
func (fs *MutableFS) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	path := fs.inodeToPath(input.NodeId)
	if path == "" {
		return fuse.ENOENT
	}

	re, status := fs.resolve(path)
	if status != fuse.OK {
		return status
	}

	flags := int(input.Flags) & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)
	isWrite := flags&(os.O_WRONLY|os.O_RDWR) != 0

	if isWrite && !re.DataIsMut {
		// Trigger copy-up.
		if err := fs.copyUp(re); err != nil {
			return fuse.ToStatus(err)
		}
		re.DataIsMut = true
	}

	if re.DataIsMut {
		abs := fs.mutablePath(path)
		fd, err := syscall.Open(abs, flags, 0)
		if err != nil {
			return fuse.ToStatus(err)
		}
		fhID := fs.registerFh(path, fd)
		out.Fh = fhID
		out.OpenFlags = fuse.FOPEN_KEEP_CACHE
		return fuse.OK
	}

	// Immutable read.
	out.Fh = 0 // pxar reads don't use fh
	out.OpenFlags = fuse.FOPEN_KEEP_CACHE
	return fuse.OK
}

// Read reads data from the appropriate source.
func (fs *MutableFS) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	path := fs.inodeToPath(input.NodeId)
	if path == "" {
		return nil, fuse.ENOENT
	}

	re, status := fs.resolve(path)
	if status != fuse.OK {
		return nil, status
	}

	if re.DataIsMut {
		fh := fs.getFh(input.Fh)
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

	// Delegate to pxar using its native inode.
	pxarInput := *input
	pxarInput.NodeId = re.PxarNode.inode
	return fs.pxar.Read(cancel, &pxarInput, buf)
}

// Write writes data. Triggers copy-up if needed.
func (fs *MutableFS) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (uint32, fuse.Status) {
	path := fs.inodeToPath(input.NodeId)
	if path == "" {
		return 0, fuse.ENOENT
	}

	// Copy-up if needed.
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
		// Lazy-open after copy-up.
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

	// Update journal size and mtime after write.
	now := time.Now().UnixNano()
	if err := fs.journal.MarkModified(&JournalEntry{
		Path:    path,
		State:   StateModified,
		Mode:    re.Mode,
		UID:     re.UID,
		GID:     re.GID,
		Size:    uint64(int64(input.Offset) + int64(n)),
		MtimeNs: now,
		CtimeNs: now,
		HasData: true,
	}); err != nil {
		return 0, fuse.EIO
	}

	return uint32(n), fuse.OK
}

// SetAttr applies metadata changes via the journal.
func (fs *MutableFS) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {
	path := fs.inodeToPath(input.NodeId)
	if path == "" {
		return fuse.ENOENT
	}

	re, status := fs.resolve(path)
	if status != fuse.OK {
		return status
	}

	// Apply changes.
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
		// Truncate in data if copied up.
		if re.DataIsMut {
			abs := fs.mutablePath(path)
			_ = os.Truncate(abs, int64(v))
		}
	}
	if a, ok := input.GetATime(); ok {
		re.CtimeNs = a.UnixNano()
	}
	if m, ok := input.GetMTime(); ok {
		re.MtimeNs = m.UnixNano()
	}

	// If mutable data exists, apply directly.
	if re.DataIsMut {
		abs := fs.mutablePath(path)
		if m, ok := input.GetMode(); ok {
			_ = unix.Chmod(abs, m)
		}
		uid := -1
		gid := -1
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

	// Always update journal.
	je := &JournalEntry{
		Path:    path,
		State:   re.JournalState(),
		Mode:    re.Mode,
		UID:     re.UID,
		GID:     re.GID,
		Size:    re.Size,
		MtimeNs: re.MtimeNs,
		CtimeNs: re.CtimeNs,
		HasData: re.DataIsMut,
		Target:  re.SymlinkTgt,
	}
	if err := fs.journal.MarkModified(je); err != nil {
		return fuse.EIO
	}

	return fs.GetAttr(cancel, &fuse.GetAttrIn{InHeader: input.InHeader}, out)
}

// Create creates a new file in the mutable layer.
func (fs *MutableFS) Create(cancel <-chan struct{}, input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	parentPath := fs.inodeToPath(input.NodeId)
	childPath := joinPath(parentPath, name)

	// Check if an immutable entry existed — mark whiteout.
	if _, st := fs.resolve(childPath); st == fuse.OK {
		if err := fs.journal.MarkWhiteout(childPath); err != nil {
			return fuse.EIO
		}
	}

	abs := fs.mutablePath(childPath)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return fuse.ToStatus(err)
	}

	fd, err := syscall.Open(abs, int(input.Flags)|os.O_CREATE|os.O_EXCL, uint32(input.Mode&0o777))
	if err != nil {
		return fuse.ToStatus(err)
	}

	ino := fs.allocInode(false)
	fs.mapInode(ino, childPath)

	now := time.Now().UnixNano()
	je := &JournalEntry{
		Path:    childPath,
		State:   StateNew,
		Mode:    uint32(syscall.S_IFREG) | uint32(input.Mode&0o777),
		UID:     input.Uid,
		GID:     input.Gid,
		Size:    0,
		MtimeNs: now,
		CtimeNs: now,
		HasData: true,
	}
	if err := fs.journal.MarkNew(je); err != nil {
		syscall.Close(fd)
		os.Remove(abs)
		fs.unmapInode(childPath)
		return fuse.EIO
	}

	fs.applyACLOwnership(abs, false)

	fhID := fs.registerFh(childPath, fd)

	out.NodeId = ino
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	out.Fh = fhID
	out.OpenFlags = fuse.FOPEN_KEEP_CACHE
	fillAttrFromJE(&out.Attr, je)
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

	// If an immutable entry previously existed at this path, mark opaque
	// to hide its contents.
	hasImmutable := fs.hasPxarEntry(childPath)
	state := StateNew
	if hasImmutable {
		state = StateOpaque
	}

	now := time.Now().UnixNano()
	je := &JournalEntry{
		Path:    childPath,
		State:   state,
		Mode:    uint32(input.Mode&0o777) | syscall.S_IFDIR,
		UID:     input.Uid,
		GID:     input.Gid,
		Size:    0,
		MtimeNs: now,
		CtimeNs: now,
		HasData: false,
	}
	_ = fs.journal.MarkNew(je)

	ino := fs.allocInode(true)
	fs.mapInode(ino, childPath)

	out.NodeId = ino
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	fillAttrFromJE(&out.Attr, je)
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

	hasImmutable := fs.hasPxarEntry(childPath)
	state := StateNew
	if hasImmutable {
		state = StateModified
	}

	now := time.Now().UnixNano()
	je := &JournalEntry{
		Path:    childPath,
		State:   state,
		Mode:    uint32(input.Mode & 0o777),
		UID:     input.Uid,
		GID:     input.Gid,
		Size:    0,
		MtimeNs: now,
		CtimeNs: now,
		HasData: true,
	}
	_ = fs.journal.MarkModified(je)

	ino := fs.allocInode(false)
	fs.mapInode(ino, childPath)

	out.NodeId = ino
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	fillAttrFromJE(&out.Attr, je)
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

	hasImmutable := fs.hasPxarEntry(childPath)
	state := StateNew
	if hasImmutable {
		state = StateModified
	}

	now := time.Now().UnixNano()
	je := &JournalEntry{
		Path:    childPath,
		State:   state,
		Mode:    uint32(syscall.S_IFLNK | 0o777),
		UID:     header.Uid,
		GID:     header.Gid,
		Size:    0,
		MtimeNs: now,
		CtimeNs: now,
		HasData: true,
		Target:  target,
	}
	_ = fs.journal.MarkModified(je)

	ino := fs.allocInode(false)
	fs.mapInode(ino, childPath)

	out.NodeId = ino
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	fillAttrFromJE(&out.Attr, je)
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

	// If there's an immutable entry, insert a whiteout.
	if re.PxarNode != nil {
		_ = fs.journal.MarkWhiteout(childPath)
	} else {
		_ = fs.journal.RemoveEntry(childPath)
	}

	// Remove mutable data if present.
	if re.DataIsMut {
		abs := fs.mutablePath(childPath)
		_ = os.Remove(abs)
	}

	fs.unmapInode(childPath)
	return fuse.OK
}

// Rmdir removes a directory.
func (fs *MutableFS) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	return fs.Unlink(cancel, header, name)
}

// Rename renames a file or directory.
func (fs *MutableFS) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName string, newName string) fuse.Status {
	oldParentPath := fs.inodeToPath(input.NodeId)
	newParentPath := fs.inodeToPath(input.Newdir)
	oldPath := joinPath(oldParentPath, oldName)
	newPath := joinPath(newParentPath, newName)

	oldRE, oldStatus := fs.resolve(oldPath)
	if oldStatus != fuse.OK {
		return oldStatus
	}

	// Check if destination has an immutable entry.
	destHasImmutable := fs.hasPxarEntry(newPath)

	// Determine the source inode.
	ino := fs.pathToIno(oldPath, oldRE.IsDir)

	// Perform the rename.
	// If source has mutable data, move it.
	if oldRE.DataIsMut {
		oldAbs := fs.mutablePath(oldPath)
		newAbs := fs.mutablePath(newPath)
		if err := os.MkdirAll(filepath.Dir(newAbs), 0o755); err != nil {
			return fuse.ToStatus(err)
		}
		if err := os.Rename(oldAbs, newAbs); err != nil {
			return fuse.ToStatus(err)
		}
	}

	// Journal the rename.
	if oldRE.PxarNode != nil && oldRE.Journal == nil {
		// Pure immutable rename: whiteout old, create modified entry at new.
		je := pxarNodeToJE(oldRE.PxarNode, newPath)
		je.State = StateModified
		_ = fs.journal.RenameImmutable(oldPath, newPath, je)
	} else if oldRE.Journal != nil {
		_ = fs.journal.Rename(oldPath, newPath, destHasImmutable)
	} else {
		// Pure new entry: remove old, create new.
		_ = fs.journal.RemoveEntry(oldPath)
		je := &JournalEntry{
			Path:    newPath,
			State:   StateNew,
			Mode:    oldRE.Mode,
			UID:     oldRE.UID,
			GID:     oldRE.GID,
			Size:    oldRE.Size,
			MtimeNs: oldRE.MtimeNs,
			CtimeNs: oldRE.CtimeNs,
			HasData: oldRE.DataIsMut,
			Target:  oldRE.SymlinkTgt,
		}
		_ = fs.journal.MarkModified(je)
	}

	// Update inode mapping.
	fs.unmapInode(oldPath)
	fs.mapInode(ino, newPath)

	return fuse.OK
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

	// Check journal xattrs first.
	val, err := fs.journal.GetXAttr(path, attr)
	if err != nil {
		return 0, fuse.EIO
	}
	if val != nil {
		return xattrValue(val, dest)
	}

	// Check mutable data xattrs.
	re, _ := fs.resolve(path)
	if re != nil && re.DataIsMut {
		abs := fs.mutablePath(path)
		sz, xerr := unix.Getxattr(abs, attr, dest)
		if xerr == nil {
			return uint32(sz), fuse.OK
		}
	}

	// Fall back to pxar using its native inode.
	if re != nil && re.PxarNode != nil {
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

	names := make(map[string]bool)

	// Journal xattrs.
	journalNames, _ := fs.journal.ListXAttrs(path)
	for _, n := range journalNames {
		names[n] = true
	}

	// Pxar xattrs (merged, journal wins).
	re, _ := fs.resolve(path)
	if re != nil && re.PxarNode != nil {
		pxarHeader := *header
		pxarHeader.NodeId = re.PxarNode.inode
		_, _ = fs.pxar.ListXAttr(cancel, &pxarHeader, nil)
	}

	// Build output.
	var total uint32
	for n := range names {
		total += uint32(len(n)) + 1
	}
	if dest == nil {
		return total, fuse.OK
	}
	if uint32(len(dest)) < total {
		return 0, fuse.Status(syscall.ERANGE)
	}
	pos := 0
	for n := range names {
		pos += copy(dest[pos:], n)
		dest[pos] = 0
		pos++
	}
	return uint32(pos), fuse.OK
}

func (fs *MutableFS) SetXAttr(cancel <-chan struct{}, input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	path := fs.inodeToPath(input.NodeId)
	if path == "" {
		if fs.verbose {
			fmt.Fprintf(os.Stderr, "  SetXAttr: inode %d → empty path\n", input.NodeId)
		}
		return fuse.ENOENT
	}

	// Ensure a journal entry exists for this path.
	re, status := fs.resolve(path)
	if status != fuse.OK {
		if fs.verbose {
			fmt.Fprintf(os.Stderr, "  SetXAttr: resolve %q failed: %v\n", path, status)
		}
		return status
	}
	fs.ensureJournalEntry(re)

	if err := fs.journal.SetXAttr(path, attr, data); err != nil {
		if fs.verbose {
			fmt.Fprintf(os.Stderr, "  SetXAttr: journal.SetXAttr %q %s failed: %v\n", path, attr, err)
		}
		return fuse.EIO
	}

	// Also apply to mutable data if present.
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

	if err := fs.journal.RemoveXAttr(path, attr); err != nil {
		return fuse.EIO
	}

	re, _ := fs.resolve(path)
	if re != nil && re.DataIsMut {
		abs := fs.mutablePath(path)
		_ = unix.Removexattr(abs, attr)
	}

	return fuse.OK
}

// --- file handle lifecycle ---

func (fs *MutableFS) Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status {
	return fs.fsyncInternal(input.NodeId, input.Fh)
}

func (fs *MutableFS) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
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

// Release closes an open file handle.
func (fs *MutableFS) Release(cancel <-chan struct{}, input *fuse.ReleaseIn) {
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

// copyUp copies data from the immutable (pxar) layer to the mutable directory.
func (fs *MutableFS) copyUp(re *ResolvedEntry) error {
	inoMu := fs.getInoLock(re.Inode)
	inoMu.Lock()
	defer inoMu.Unlock()

	// Double-check: another goroutine may have already copied up.
	if re.DataIsMut {
		return nil
	}
	if re.Journal != nil && re.Journal.HasData {
		re.DataIsMut = true
		return nil
	}

	abs := fs.mutablePath(re.Path)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return err
	}

	if re.PxarNode != nil {
		if re.PxarNode.isReg {
			return fs.copyUpRegularFile(re.Path, re.PxarNode)
		}
		if re.PxarNode.isSymlink {
			entry, err := fs.pxar.GetPxarEntry(re.Inode)
			if err != nil {
				return err
			}
			if err := syscall.Symlink(entry.LinkTarget, abs); err != nil {
				return err
			}
		}
	}

	// Mark as having data.
	if err := fs.journal.SetHasData(re.Path); err != nil {
		return fmt.Errorf("journal set has_data: %w", err)
	}
	re.DataIsMut = true

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

// resolve looks up a path in journal → pxar and returns a ResolvedEntry.
func (fs *MutableFS) resolve(path string) (*ResolvedEntry, fuse.Status) {
	je, err := fs.journal.GetEntry(path)
	if err != nil {
		return nil, fuse.EIO
	}

	// Whiteout → ENOENT.
	if je != nil && je.State == StateWhiteout {
		return nil, fuse.ENOENT
	}

	// Journal entry exists — it's authoritative.
	if je != nil {
		return fs.resolveFromJournal(je)
	}

	// No journal entry — fall back to pxar.
	return fs.resolveFromPxar(path)
}

func (fs *MutableFS) resolveFromJournal(je *JournalEntry) (*ResolvedEntry, fuse.Status) {
	re := &ResolvedEntry{
		Path:       je.Path,
		Journal:    je,
		DataIsMut:  je.HasData,
		Mode:       je.Mode,
		UID:        je.UID,
		GID:        je.GID,
		Size:       je.Size,
		MtimeNs:    je.MtimeNs,
		CtimeNs:    je.CtimeNs,
		SymlinkTgt: je.Target,
		IsDir:      je.Mode&syscall.S_IFDIR != 0,
	}

	// Check if there's also a pxar entry (for metadata not in journal).
	if !je.HasData {
		pxarNode := fs.findPxarNode(je.Path)
		re.PxarNode = pxarNode
	}

	re.Inode = fs.pathToIno(je.Path, re.IsDir)
	return re, fuse.OK
}

func (fs *MutableFS) resolveFromPxar(path string) (*ResolvedEntry, fuse.Status) {
	pxarNode := fs.findPxarNode(path)
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

// findPxarNode walks the pxar tree to find a node by path.
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

// --- journal helpers ---

// ensureJournalEntry creates a journal entry from the resolved entry
// if one doesn't already exist.
func (fs *MutableFS) ensureJournalEntry(re *ResolvedEntry) {
	if re.Journal != nil {
		return
	}
	je := &JournalEntry{
		Path:    re.Path,
		State:   StateModified,
		Mode:    re.Mode,
		UID:     re.UID,
		GID:     re.GID,
		Size:    re.Size,
		MtimeNs: re.MtimeNs,
		CtimeNs: re.CtimeNs,
		HasData: re.DataIsMut,
		Target:  re.SymlinkTgt,
	}
	if err := fs.journal.MarkModified(je); err != nil {
		return
	}
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

// --- helper methods ---

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

// ForceACLOwnership walks the mutable dir and forces owner/group.
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

// Close releases resources.
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

// --- fill helpers for resolved entries ---

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

func pxarNodeToJE(n *node, path string) *JournalEntry {
	return &JournalEntry{
		Path:    path,
		Mode:    statMode(n.mode),
		UID:     n.uid,
		GID:     n.gid,
		Size:    n.fileSize,
		MtimeNs: int64(n.mtimeSecs)*1e9 + int64(n.mtimeNanos),
		CtimeNs: int64(n.mtimeSecs)*1e9 + int64(n.mtimeNanos),
	}
}

// JournalState returns the appropriate EntryState for a ResolvedEntry.
func (re *ResolvedEntry) JournalState() EntryState {
	if re.Journal != nil {
		return re.Journal.State
	}
	if re.PxarNode != nil {
		return StateModified
	}
	return StateNew
}

// splitPath splits a path into components, handling leading "/".
func splitPath(path string) []string {
	if path == "/" || path == "" {
		return nil
	}
	// Trim leading slash.
	p := path
	if p[0] == '/' {
		p = p[1:]
	}
	var parts []string
	for _, part := range splitSlash(p) {
		if part != "" {
			parts = append(parts, part)
		}
	}
	return parts
}

func splitSlash(s string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}

// munmap releases a memory-mapped region.
func munmap(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return syscall.Munmap(data)
}

// mmapFile memory-maps a file for reading.
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
