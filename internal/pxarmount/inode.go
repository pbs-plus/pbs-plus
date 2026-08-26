package pxarmount

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
)

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
