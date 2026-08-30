package pxarmount

import (
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/puzpuzpuz/xsync/v4"
	"golang.org/x/sys/unix"
)

func (fs *MutableFS) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	fs.beginMutation()
	defer fs.endMutation()

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
		fhID := fs.registerFh(fs.newFh(fd, path, input.NodeId, re))
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
	if fh := fs.getFh(input.Fh); fh != nil && !fh.sparse {
		n, err := syscall.Pread(fh.fd, buf, int64(input.Offset))
		if err != nil && err != io.EOF {
			return nil, fuse.ToStatus(err)
		}
		if n <= 0 {
			return fuse.ReadResultData(nil), fuse.OK
		}
		return fuse.ReadResultData(buf[:n]), fuse.OK
	}

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
		var n int
		var err error
		if re.Node != nil && re.Node.SparseData {
			inoMu := fs.getInoLock(input.NodeId)
			inoMu.RLock()
			n, err = fs.readSparseAt(fh.fd, input.NodeId, re.Node, re.PxarNode, buf, int64(input.Offset))
			inoMu.RUnlock()
		} else {
			n, err = syscall.Pread(fh.fd, buf, int64(input.Offset))
		}
		if err != nil {
			if err == io.EOF {
				return fuse.ReadResultData(nil), fuse.OK
			}
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

// newFh snapshots layering state; copy-up always precedes handle creation.
func (fs *MutableFS) newFh(fd int, path string, ino uint64, re *ResolvedEntry) *passFh {
	fh := &passFh{fd: fd, path: path, ino: ino}
	if re.Node != nil {
		fh.nodeID = re.Node.ID
		fh.sparse = re.Node.SparseData
		fh.lowerSize = re.Node.LowerSize
	}
	if fh.sparse {
		fh.pxarNode = re.PxarNode
	}
	return fh
}

// openAnonFh serves writeback after Release; registering it would leak.
func (fs *MutableFS) openAnonFh(ino uint64) (*passFh, fuse.Status) {
	path := fs.inodeToPath(ino)
	if path == "" {
		return nil, fuse.ENOENT
	}
	re, status := fs.resolve(path)
	if status != fuse.OK {
		return nil, status
	}
	if !re.DataIsMut {
		if err := fs.copyUp(re); err != nil {
			return nil, fuse.ToStatus(err)
		}
	}
	fd, err := syscall.Open(fs.mutablePath(path), os.O_WRONLY, 0)
	if err != nil {
		return nil, fuse.ToStatus(err)
	}
	return fs.newFh(fd, path, ino, re), fuse.OK
}

func (fs *MutableFS) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (uint32, fuse.Status) {
	fs.beginMutation()
	defer fs.endMutation()

	fh := fs.getFh(input.Fh)
	fromAnon := fh == nil
	if fromAnon {
		anon, status := fs.openAnonFh(input.NodeId)
		if status != fuse.OK {
			return 0, status
		}
		fh = anon
		defer func() {
			if err := syscall.Close(anon.fd); err != nil {
				fs.logNonFatal("close-fd", anon.path, err)
			}
		}()
	}

	inoMu := fs.getInoLock(input.NodeId)
	inoMu.RLock()
	n, err := syscall.Pwrite(fh.fd, data, int64(input.Offset))
	inoMu.RUnlock()

	if err == syscall.EBADF && !fromAnon {
		anon, status := fs.openAnonFh(input.NodeId)
		if status != fuse.OK {
			return 0, status
		}
		inoMu.RLock()
		n, err = syscall.Pwrite(anon.fd, data, int64(input.Offset))
		inoMu.RUnlock()
		if cerr := syscall.Close(anon.fd); cerr != nil {
			fs.logNonFatal("close-fd", anon.path, cerr)
		}
		fh = anon
	}

	if n < 0 {
		n = 0
	}
	if n == 0 && err != nil {
		return 0, fuse.ToStatus(err)
	}

	newSize := uint64(input.Offset) + uint64(n)
	now := time.Now().UnixNano()
	fs.dirtyMeta.Compute(input.NodeId, func(old pendingMeta, exists bool) (pendingMeta, xsync.ComputeOp) {
		next := pendingMeta{size: newSize, mtimeNs: now, ctimeNs: now}
		if exists {
			if old.size > next.size {
				next.size = old.size
			}
			next.dataExtents = old.dataExtents
			next.writeErr = old.writeErr
		}
		if fh.sparse && n > 0 {
			next.dataExtents = insertDataExtent(next.dataExtents, uint64(input.Offset), newSize)
		}
		if err != nil && next.writeErr == nil {
			next.writeErr = err
		}
		return next, xsync.UpdateOp
	})

	return uint32(n), fuse.OK
}

func (fs *MutableFS) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {
	fs.beginMutation()
	defer fs.endMutation()

	path := fs.inodeToPath(input.NodeId)
	if path == "" {
		return fuse.ENOENT
	}

	re, status := fs.resolve(path)
	if status != fuse.OK {
		return status
	}
	if _, sizeChanged := input.GetSize(); sizeChanged && !re.DataIsMut && re.PxarNode != nil && re.PxarNode.isReg {
		if err := fs.copyUp(re); err != nil {
			return fuse.ToStatus(err)
		}
	}
	inoMu := fs.getInoLock(input.NodeId)
	inoMu.Lock()
	defer inoMu.Unlock()
	re, status = fs.resolve(path)
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
				return fuse.ToStatus(err)
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

	var pendingExtents []dataExtent
	if meta, ok := fs.dirtyMeta.LoadAndDelete(input.NodeId); ok {
		if !sizeChanged && meta.size > re.Size {
			re.Size = meta.size
		}
		if !mtimeSet {
			re.MtimeNs = meta.mtimeNs
		}
		pendingExtents = meta.dataExtents
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
		if re.Node.SparseData {
			if pendingExtents != nil {
				re.Node.DataExtents = mergeDataExtents(re.Node.DataExtents, pendingExtents)
			}
			if sizeChanged {
				re.Node.DataExtents = trimDataExtents(re.Node.DataExtents, re.Size)
				re.Node.LowerSize = min(re.Node.LowerSize, re.Size)
			}
		}
		if err := fs.journal.UpdateNode(re.Node); err != nil {
			return fuse.EIO
		}
	}

	return fs.GetAttr(cancel, &fuse.GetAttrIn{InHeader: input.InHeader}, out)
}

func (fs *MutableFS) Create(cancel <-chan struct{}, input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	fs.beginMutation()
	defer fs.endMutation()

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

	fhID := fs.registerFh(&passFh{fd: fd, path: childPath, ino: ino, nodeID: nodeID})

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

func (fs *MutableFS) Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status {
	fs.beginMutation()
	defer fs.endMutation()

	if input.Fh != 0 {
		if status := fs.fsyncInternal(input.NodeId, input.Fh); status != fuse.OK {
			return status
		}
	}
	if err := fs.flushDirtyMeta(input.NodeId); err != nil {
		return fuse.ToStatus(err)
	}
	return fuse.OK
}

func (fs *MutableFS) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	fs.beginMutation()
	defer fs.endMutation()

	if input.Fh != 0 {
		if status := fs.fsyncInternal(input.NodeId, input.Fh); status != fuse.OK {
			return status
		}
	}
	if err := fs.flushDirtyMeta(input.NodeId); err != nil {
		return fuse.ToStatus(err)
	}
	if err := fs.journal.Sync(); err != nil {
		log.Error(err, "journal sync")
		return fuse.EIO
	}
	return fuse.OK
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
	fs.beginMutation()
	defer fs.endMutation()

	if err := fs.flushDirtyMeta(input.NodeId); err != nil {
		log.Error(err, "release: flush dirty meta")
	}
	if input.Fh == 0 {
		return
	}
	if fh, ok := fs.handles.LoadAndDelete(input.Fh); ok {
		if err := syscall.Close(fh.fd); err != nil {
			fs.logNonFatal("close-fd", fh.path, err)
		}
	}
}

// Forget is called by the FUSE kernel when it evicts an inode from its
// dentry cache. We clean up per-inode and per-path synchronization
// state that would otherwise leak indefinitely.
// NOTE: We don't delete inoLookup/pathLookup here because there may
// still be open file handles (Read/Write/Flush/Release) that need the
// inode→path mapping. Those are cleaned up in unmapInode (Unlink,

// Forget is called by the FUSE kernel when it evicts an inode from its
// dentry cache. We clean up per-inode and per-path synchronization
// state that would otherwise leak indefinitely.
// NOTE: We don't delete inoLookup/pathLookup here because there may
// still be open file handles (Read/Write/Flush/Release) that need the
// inode→path mapping. Those are cleaned up in unmapInode (Unlink,
func (fs *MutableFS) Forget(nodeID, nlookup uint64) {
	fs.beginMutation()
	defer fs.endMutation()

	if err := fs.flushDirtyMeta(nodeID); err != nil {
		log.Error(err, "forget: flush dirty meta")
	}
	if path, ok := fs.pathLookup.Load(nodeID); ok {
		fs.ensureLocks.Delete(path)
	}
}

func (fs *MutableFS) CopyFileRange(cancel <-chan struct{}, input *fuse.CopyFileRangeIn) (uint32, fuse.Status) {
	fs.beginMutation()
	defer fs.endMutation()

	src := fs.getFh(input.FhIn)
	dst := fs.getFh(input.FhOut)
	if src == nil || dst == nil {
		return 0, fuse.EBADF
	}
	if src.sparse {
		return 0, fuse.ENOSYS
	}

	offIn := int64(input.OffIn)
	offOut := int64(input.OffOut)
	inoMu := fs.getInoLock(input.NodeIdOut)
	inoMu.RLock()
	n, err := unix.CopyFileRange(src.fd, &offIn, dst.fd, &offOut, int(input.Len), 0)
	inoMu.RUnlock()
	if err != nil {
		return 0, fuse.ToStatus(err)
	}

	newSize := input.OffOut + uint64(n)
	now := time.Now().UnixNano()
	fs.dirtyMeta.Compute(input.NodeIdOut, func(old pendingMeta, exists bool) (pendingMeta, xsync.ComputeOp) {
		next := pendingMeta{size: newSize, mtimeNs: now, ctimeNs: now}
		if exists {
			if old.size > next.size {
				next.size = old.size
			}
			next.dataExtents = old.dataExtents
			next.writeErr = old.writeErr
		}
		if dst.sparse && n > 0 {
			next.dataExtents = insertDataExtent(next.dataExtents, input.OffOut, newSize)
		}
		return next, xsync.UpdateOp
	})
	return uint32(n), fuse.OK
}

func (fs *MutableFS) Fallocate(cancel <-chan struct{}, input *fuse.FallocateIn) fuse.Status {
	fs.beginMutation()
	defer fs.endMutation()

	fh := fs.getFh(input.Fh)
	if fh == nil {
		return fuse.EBADF
	}

	inoMu := fs.getInoLock(input.NodeId)
	inoMu.Lock()
	defer inoMu.Unlock()
	if err := unix.Fallocate(fh.fd, input.Mode, int64(input.Offset), int64(input.Length)); err != nil {
		return fuse.ToStatus(err)
	}

	end := input.Offset + input.Length
	now := time.Now().UnixNano()
	punched := input.Mode&unix.FALLOC_FL_PUNCH_HOLE != 0
	grows := input.Mode&unix.FALLOC_FL_KEEP_SIZE == 0
	fs.dirtyMeta.Compute(input.NodeId, func(old pendingMeta, exists bool) (pendingMeta, xsync.ComputeOp) {
		next := pendingMeta{mtimeNs: now, ctimeNs: now}
		if exists {
			next.size = old.size
			next.dataExtents = old.dataExtents
			next.writeErr = old.writeErr
		}
		if grows && end > next.size {
			next.size = end
		}
		if fh.sparse && punched {
			next.dataExtents = insertDataExtent(next.dataExtents, input.Offset, end)
		}
		return next, xsync.UpdateOp
	})
	return fuse.OK
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
	path := fs.inodeToPath(input.NodeId)
	if path == "" {
		return fuse.ENOENT
	}
	re, status := fs.resolve(path)
	if status != fuse.OK {
		return status
	}
	if permitAccess(re, input.Uid, input.Gid, input.Mask) {
		return fuse.OK
	}
	return fuse.EACCES
}

// permitAccess implements the POSIX rwx check. Supplementary groups are not
// visible over FUSE, so mounts that need them must use default_permissions.
func permitAccess(re *ResolvedEntry, uid, gid, mask uint32) bool {
	if mask == 0 {
		return true
	}
	if uid == 0 {
		return mask&1 == 0 || re.IsDir || re.Mode&0o111 != 0
	}
	var perm uint32
	switch {
	case uid == re.UID:
		perm = (re.Mode >> 6) & 7
	case gid == re.GID:
		perm = (re.Mode >> 3) & 7
	default:
		perm = re.Mode & 7
	}
	return mask&perm == mask
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
