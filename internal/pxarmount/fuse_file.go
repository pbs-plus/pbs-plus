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
