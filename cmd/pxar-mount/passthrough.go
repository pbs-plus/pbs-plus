package main

import (
	"os"
	"path/filepath"
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

// passthroughFS implements fuse.RawFileSystem as an overlay:
//   - pxarFS provides the read-only lower layer
//   - backingDir provides the read-write upper layer
//
// Files from pxar are read-only. Files created/modified via the mount
// are written to backingDir. Listing merges both layers (backing wins).
type passthroughFS struct {
	fuse.RawFileSystem

	pxar       *pxarFS
	backingDir string

	mu          sync.Mutex
	nextBackIno uint64
	nodePaths   map[uint64]string // inode → relative path from mount root
	backed      map[uint64]bool   // true if inode lives in backing dir

	fhmu    sync.Mutex
	handles map[uint64]*passFh // file handle id → open fd
	nextFh  uint64
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
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.backed[ino]
}

func (fs *passthroughFS) nodePath(ino uint64) string {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.nodePaths[ino]
}

func (fs *passthroughFS) setNode(ino uint64, relPath string, backed bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.nodePaths[ino] = relPath
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

// ensureBackingParent creates parent directories in backingDir for the
// given relative path so that the file itself can be created.
func (fs *passthroughFS) ensureBackingParent(rel string) error {
	parent := filepath.Dir(rel)
	if parent == "." || parent == "/" {
		return nil
	}
	return os.MkdirAll(fs.absPath(parent), 0o755)
}

// statBacked stat's a relative path in the backing dir and constructs
// backing node metadata. Returns nil if the file does not exist.
func (fs *passthroughFS) statBacked(rel string) (*node, error) {
	var st syscall.Stat_t
	if err := syscall.Lstat(fs.absPath(rel), &st); err != nil {
		return nil, err
	}
	mode := uint64(st.Mode)
	ino := fs.allocBackedIno(mode&syscall.S_IFDIR != 0)
	fs.setNode(ino, rel, true)
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
func (fs *passthroughFS) getPxarNode(ino uint64) *node {
	fs.pxar.mu.RLock()
	defer fs.pxar.mu.RUnlock()
	return fs.pxar.nodes[ino]
}

func (fs *passthroughFS) fillStatAttr(n *node, attr *fuse.Attr) {
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

// --- Lookup ---

func (fs *passthroughFS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	parentPath := fs.nodePath(header.NodeId)
	childPath := parentPath + "/" + name

	// Check backing dir first (upper layer takes priority)
	if nd, err := fs.statBacked(childPath); err == nil {
		fs.mu.Lock()
		nd.refs++
		nd.parent = header.NodeId
		fs.mu.Unlock()
		fillEntryOut(nd.inode, nd, out)
		return fuse.OK
	}

	// Delegate to pxar
	st := fs.pxar.Lookup(cancel, header, name, out)
	if st == fuse.OK {
		// Store the pxar node's path for future overlay lookups
		fs.setNode(out.NodeId, childPath, false)
		// Also store the parent path for pxar directories if not already set
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
		n := &node{
			inode:     input.NodeId,
			fileSize:  uint64(st.Size),
			mode:      uint64(st.Mode),
			uid:       st.Uid,
			gid:       st.Gid,
			mtimeSecs: int64(st.Mtim.Sec),
			isDir:     st.Mode&syscall.S_IFDIR != 0,
			isSymlink: st.Mode&syscall.S_IFLNK != 0,
			isReg:     st.Mode&syscall.S_IFREG != 0,
		}
		fillAttrOut(n, out)
		return fuse.OK
	}
	return fs.pxar.GetAttr(cancel, input, out)
}

func (fs *passthroughFS) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {
	if !fs.isBacked(input.NodeId) {
		return fuse.EROFS
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
	childPath := parentPath + "/" + name

	if err := fs.ensureBackingParent(childPath); err != nil {
		return fuse.ToStatus(err)
	}

	abs := fs.absPath(childPath)
	flags := int(input.Flags) | os.O_CREATE | os.O_TRUNC
	fd, err := syscall.Open(abs, flags, uint32(input.Mode&0o777))
	if err != nil {
		return fuse.ToStatus(err)
	}

	ino := fs.allocBackedIno(false)
	fs.setNode(ino, childPath, true)

	fs.fhmu.Lock()
	fhID := fs.nextFh
	fs.nextFh++
	fs.handles[fhID] = &passFh{fd: fd, inode: ino}
	fs.fhmu.Unlock()

	// Stat for the entry out
	var st syscall.Stat_t
	if err := syscall.Fstat(fd, &st); err != nil {
		_ = syscall.Close(fd)
		return fuse.ToStatus(err)
	}

	out.NodeId = ino
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	out.Fh = fhID
	out.OpenFlags = fuse.FOPEN_KEEP_CACHE
	fs.fillStatAttr(&node{
		inode: ino, fileSize: uint64(st.Size), mode: uint64(st.Mode),
		uid: st.Uid, gid: st.Gid,
		mtimeSecs: int64(st.Mtim.Sec), mtimeNanos: uint32(st.Mtim.Nsec),
		isReg: true,
	}, &out.Attr)

	return fuse.OK
}

func (fs *passthroughFS) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) fuse.Status {
	parentPath := fs.nodePath(input.NodeId)
	childPath := parentPath + "/" + name

	if err := fs.ensureBackingParent(childPath); err != nil {
		return fuse.ToStatus(err)
	}

	abs := fs.absPath(childPath)
	if err := syscall.Mkdir(abs, input.Mode&0o777); err != nil {
		return fuse.ToStatus(err)
	}

	ino := fs.allocBackedIno(true)
	fs.setNode(ino, childPath, true)

	var st syscall.Stat_t
	if err := syscall.Lstat(abs, &st); err != nil {
		return fuse.ToStatus(err)
	}

	out.NodeId = ino
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	fs.fillStatAttr(&node{
		inode: ino, fileSize: uint64(st.Size), mode: uint64(st.Mode),
		uid: st.Uid, gid: st.Gid,
		mtimeSecs: int64(st.Mtim.Sec), mtimeNanos: uint32(st.Mtim.Nsec),
		isDir: true,
	}, &out.Attr)

	return fuse.OK
}

func (fs *passthroughFS) Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) fuse.Status {
	parentPath := fs.nodePath(input.NodeId)
	childPath := parentPath + "/" + name

	if err := fs.ensureBackingParent(childPath); err != nil {
		return fuse.ToStatus(err)
	}

	abs := fs.absPath(childPath)
	if err := syscall.Mknod(abs, input.Mode, int(input.Rdev)); err != nil {
		return fuse.ToStatus(err)
	}

	ino := fs.allocBackedIno(input.Mode&syscall.S_IFDIR != 0)
	fs.setNode(ino, childPath, true)

	var st syscall.Stat_t
	if err := syscall.Lstat(abs, &st); err != nil {
		return fuse.ToStatus(err)
	}

	out.NodeId = ino
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	fs.fillStatAttr(&node{
		inode: ino, fileSize: uint64(st.Size), mode: uint64(st.Mode),
		uid: st.Uid, gid: st.Gid,
		mtimeSecs: int64(st.Mtim.Sec), mtimeNanos: uint32(st.Mtim.Nsec),
	}, &out.Attr)

	return fuse.OK
}

func (fs *passthroughFS) Symlink(cancel <-chan struct{}, header *fuse.InHeader, target string, linkName string, out *fuse.EntryOut) fuse.Status {
	parentPath := fs.nodePath(header.NodeId)
	childPath := parentPath + "/" + linkName

	if err := fs.ensureBackingParent(childPath); err != nil {
		return fuse.ToStatus(err)
	}

	abs := fs.absPath(childPath)
	if err := syscall.Symlink(target, abs); err != nil {
		return fuse.ToStatus(err)
	}

	ino := fs.allocBackedIno(false)
	fs.setNode(ino, childPath, true)

	var st syscall.Stat_t
	if err := syscall.Lstat(abs, &st); err != nil {
		return fuse.ToStatus(err)
	}

	out.NodeId = ino
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	fs.fillStatAttr(&node{
		inode: ino, fileSize: uint64(st.Size), mode: uint64(st.Mode),
		uid: st.Uid, gid: st.Gid,
		mtimeSecs: int64(st.Mtim.Sec), mtimeNanos: uint32(st.Mtim.Nsec),
		isSymlink: true,
	}, &out.Attr)

	return fuse.OK
}

// --- Unlink / Rmdir ---

func (fs *passthroughFS) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	parentPath := fs.nodePath(header.NodeId)
	childPath := parentPath + "/" + name

	abs := fs.absPath(childPath)
	if _, err := os.Lstat(abs); err != nil {
		return fuse.EROFS
	}

	if err := syscall.Unlink(abs); err != nil {
		return fuse.ToStatus(err)
	}
	return fuse.OK
}

func (fs *passthroughFS) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	parentPath := fs.nodePath(header.NodeId)
	childPath := parentPath + "/" + name

	abs := fs.absPath(childPath)
	if _, err := os.Lstat(abs); err != nil {
		return fuse.EROFS
	}

	if err := syscall.Rmdir(abs); err != nil {
		return fuse.ToStatus(err)
	}
	return fuse.OK
}

// --- Rename ---

func (fs *passthroughFS) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName string, newName string) fuse.Status {
	oldParentPath := fs.nodePath(input.NodeId)
	newParentPath := fs.nodePath(input.Newdir)
	oldPath := oldParentPath + "/" + oldName
	newPath := newParentPath + "/" + newName

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
		return fs.renameSimple(oldAbs, newAbs)
	}

	if input.Flags&renameExchange != 0 {
		return fs.renameExchange(oldAbs, newAbs)
	}

	// Default: rename with potential overwrite of existing backing file.
	if err := os.Rename(oldAbs, newAbs); err != nil {
		return fuse.ToStatus(err)
	}
	return fuse.OK
}

func (fs *passthroughFS) renameSimple(oldAbs, newAbs string) fuse.Status {
	if err := os.Rename(oldAbs, newAbs); err != nil {
		return fuse.ToStatus(err)
	}
	return fuse.OK
}

func (fs *passthroughFS) renameExchange(oldAbs, newAbs string) fuse.Status {
	if err := unix.Renameat2(unix.AT_FDCWD, oldAbs, unix.AT_FDCWD, newAbs, unix.RENAME_EXCHANGE); err != nil {
		return fuse.ToStatus(err)
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
		return fs.pxar.Open(cancel, input, out)
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

func (fs *passthroughFS) Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status {
	if !fs.isBacked(input.NodeId) {
		return fuse.OK
	}

	fh := fs.handleForNode(input.NodeId, input.Fh)
	if fh == nil {
		return fuse.EBADF
	}
	if err := syscall.Fsync(fh.fd); err != nil {
		return fuse.ToStatus(err)
	}
	return fuse.OK
}

func (fs *passthroughFS) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	if !fs.isBacked(input.NodeId) {
		return fuse.OK
	}

	fh := fs.handleForNode(input.NodeId, input.Fh)
	if fh == nil {
		return fuse.EBADF
	}
	if err := syscall.Fsync(fh.fd); err != nil {
		return fuse.ToStatus(err)
	}
	return fuse.OK
}

func (fs *passthroughFS) Fallocate(cancel <-chan struct{}, input *fuse.FallocateIn) fuse.Status {
	if !fs.isBacked(input.NodeId) {
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

	// Read pxar entries
	var pxarEntries []dirEntryMeta
	if pxarN, err := fs.pxar.readDirRaw(input.NodeId); err == nil {
		pxarEntries = pxarN
	}

	// Read backing dir entries
	var backedEntries []dirEntryMeta
	if parentPath != "" {
		absParent := fs.absPath(parentPath)
		des, err := os.ReadDir(absParent)
		if err == nil {
			for _, de := range des {
				info, err := de.Info()
				if err != nil {
					continue
				}
				childPath := parentPath + "/" + de.Name()
				mode := info.Mode()
				isDir := de.IsDir()
				ino := fs.allocBackedIno(isDir)
				fs.setNode(ino, childPath, true)

				backedEntries = append(backedEntries, dirEntryMeta{
					name:  de.Name(),
					inode: ino,
					mode:  uint32(mode),
				})
			}
		}
	}

	// Merge: backed entries shadow pxar entries
	backedName := make(map[string]bool, len(backedEntries))
	for _, be := range backedEntries {
		backedName[be.name] = true
	}

	var entries []dirEntryMeta
	for _, pe := range pxarEntries {
		if !backedName[pe.name] {
			entries = append(entries, pe)
			// Store pxar entry path for future overlay lookups
			fs.setNode(pe.inode, parentPath+"/"+pe.name, false)
		}
	}
	entries = append(entries, backedEntries...)

	// "." entry
	if input.Offset == 0 {
		mode := fs.dirModeForNode(input.NodeId)
		if plus {
			eo := out.AddDirLookupEntry(fuse.DirEntry{Name: ".", Ino: input.NodeId, Mode: mode})
			if eo == nil {
				return fuse.OK
			}
			fs.fillEntryOutForNode(input.NodeId, eo)
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
		// For backed nodes, try to find parent in pxar cache
		if fs.isBacked(input.NodeId) {
			if pn := fs.getPxarNode(input.NodeId); pn != nil {
				parentIno = pn.parent
			}
		}

		fs.pxar.mu.RLock()
		if pn := fs.pxar.nodes[parentIno]; pn != nil {
			parentMode = statMode(pn.mode)
		}
		fs.pxar.mu.RUnlock()

		if plus {
			eo := out.AddDirLookupEntry(fuse.DirEntry{Name: "..", Ino: parentIno, Mode: parentMode})
			if eo != nil {
				fs.fillEntryOutForNode(parentIno, eo)
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
			fs.fillEntryOutForNode(entries[i].inode, eo)
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

func (fs *passthroughFS) fillEntryOutForNode(ino uint64, out *fuse.EntryOut) {
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

// --- xattr ---

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
	if !fs.isBacked(input.NodeId) {
		return fuse.EROFS
	}
	rel := fs.nodePath(input.NodeId)
	flags := 0
	if input.Flags&xattrCreate != 0 {
		flags = unix.XATTR_CREATE
	} else if input.Flags&xattrReplace != 0 {
		flags = unix.XATTR_REPLACE
	}
	if err := unix.Setxattr(fs.absPath(rel), attr, data, flags); err != nil {
		return fuse.ToStatus(err)
	}
	return fuse.OK
}

func (fs *passthroughFS) RemoveXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string) fuse.Status {
	if !fs.isBacked(header.NodeId) {
		return fuse.EROFS
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
