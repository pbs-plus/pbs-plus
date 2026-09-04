package pxarmount

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/pbs-plus/pbs-plus/internal/log"
)

// fuseDirentHeaderSize matches fuse._Dirent: Ino, Off uint64 + NameLen, Typ uint32.
const fuseDirentHeaderSize = 24

const readDirBufSize = 1 << 16

// NFSFilesystem adapts a fuse.RawFileSystem to billy.Filesystem so the layers
// backing the FUSE mount can instead be exported over NFSv3 by go-nfs.
type NFSFilesystem struct {
	raw fuse.RawFileSystem

	root string

	refcounted bool

	readOnly bool
	uid, gid uint32
}

var (
	_ billy.Filesystem = (*NFSFilesystem)(nil)
	_ billy.Change     = (*NFSFilesystem)(nil)
)

// NewNFSFilesystem wraps raw for NFS export, rejecting mutations when readOnly.
func NewNFSFilesystem(raw fuse.RawFileSystem, readOnly bool) *NFSFilesystem {
	return &NFSFilesystem{
		raw:        raw,
		root:       "/",
		refcounted: isRefcountedRawFS(raw),
		readOnly:   readOnly,
		uid:        uint32(os.Getuid()),
		gid:        uint32(os.Getgid()),
	}
}

func isRefcountedRawFS(raw fuse.RawFileSystem) bool {
	_, ok := raw.(*PxarFS)
	return ok
}

func (f *NFSFilesystem) header(ino uint64) *fuse.InHeader {
	h := &fuse.InHeader{NodeId: ino}
	h.Uid = f.uid
	h.Gid = f.gid
	h.Pid = uint32(os.Getpid())
	return h
}

func (f *NFSFilesystem) abs(name string) string {
	cleaned := path.Clean("/" + strings.ReplaceAll(name, "\\", "/"))
	if f.root == "/" {
		return cleaned
	}
	return path.Join(f.root, cleaned)
}

func (f *NFSFilesystem) release(ino uint64) {
	if f.refcounted && ino != RootInode {
		f.raw.Forget(ino, 1)
	}
}

// resolve walks p through Lookup; the caller owns the returned inode and must
// pass it to release.
func (f *NFSFilesystem) resolve(p string) (uint64, *fuse.Attr, error) {
	parts := splitPath(f.abs(p))

	ino := RootInode
	if len(parts) == 0 {
		var out fuse.AttrOut
		in := &fuse.GetAttrIn{InHeader: *f.header(ino)}
		if st := f.raw.GetAttr(nil, in, &out); st != fuse.OK {
			return 0, nil, statusError(st)
		}
		return ino, &out.Attr, nil
	}

	var attr fuse.Attr
	for i, name := range parts {
		if name == "" || name == "." {
			continue
		}
		var out fuse.EntryOut
		st := f.raw.Lookup(nil, f.header(ino), name, &out)
		f.release(ino)
		if st != fuse.OK {
			return 0, nil, statusError(st)
		}
		ino = out.NodeId
		attr = out.Attr
		if i == len(parts)-1 {
			return ino, &attr, nil
		}
	}
	return ino, &attr, nil
}

// resolveParent returns p's parent inode, which the caller must release.
func (f *NFSFilesystem) resolveParent(p string) (uint64, string, error) {
	full := f.abs(p)
	base := path.Base(full)
	if base == "/" || base == "." {
		return 0, "", os.ErrInvalid
	}
	ino, attr, err := f.resolve(path.Dir(full))
	if err != nil {
		return 0, "", err
	}
	if attr.Mode&syscall.S_IFMT != syscall.S_IFDIR {
		f.release(ino)
		return 0, "", syscall.ENOTDIR
	}
	return ino, base, nil
}

func statusError(st fuse.Status) error {
	switch st {
	case fuse.OK:
		return nil
	case fuse.ENOENT:
		return os.ErrNotExist
	case fuse.EPERM, fuse.EACCES:
		return os.ErrPermission
	default:
		return syscall.Errno(st)
	}
}

func (f *NFSFilesystem) mutationError() error {
	if f.readOnly {
		return billy.ErrReadOnly
	}
	return nil
}

// Stat does not follow links: NFSv3 servers return link attributes and let the
// client issue READLINK, so Stat and Lstat are deliberately identical.
func (f *NFSFilesystem) Stat(filename string) (os.FileInfo, error) {
	return f.Lstat(filename)
}

func (f *NFSFilesystem) Lstat(filename string) (os.FileInfo, error) {
	ino, attr, err := f.resolve(filename)
	if err != nil {
		return nil, err
	}
	defer f.release(ino)
	return newNFSFileInfo(path.Base(f.abs(filename)), attr), nil
}

func (f *NFSFilesystem) Open(filename string) (billy.File, error) {
	return f.OpenFile(filename, os.O_RDONLY, 0)
}

func (f *NFSFilesystem) Create(filename string) (billy.File, error) {
	return f.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o666)
}

func (f *NFSFilesystem) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	wants := flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND) != 0
	if wants {
		if err := f.mutationError(); err != nil {
			return nil, err
		}
	}

	ino, attr, err := f.resolve(filename)
	switch {
	case err == nil && flag&os.O_EXCL != 0 && flag&os.O_CREATE != 0:
		f.release(ino)
		return nil, os.ErrExist
	case err == nil:
		if attr.Mode&syscall.S_IFMT == syscall.S_IFDIR {
			f.release(ino)
			return nil, syscall.EISDIR
		}
	case errors.Is(err, os.ErrNotExist) && flag&os.O_CREATE != 0:
		return f.create(filename, flag, perm)
	default:
		return nil, err
	}

	var out fuse.OpenOut
	in := &fuse.OpenIn{InHeader: *f.header(ino), Flags: uint32(flag)}
	if st := f.raw.Open(nil, in, &out); st != fuse.OK {
		f.release(ino)
		return nil, statusError(st)
	}

	file := &nfsFile{fs: f, name: f.abs(filename), ino: ino, fh: out.Fh, size: int64(attr.Size)}
	if flag&os.O_TRUNC != 0 {
		if err := file.Truncate(0); err != nil {
			_ = file.Close()
			return nil, err
		}
	}
	if flag&os.O_APPEND != 0 {
		file.offset = file.size
	}
	return file, nil
}

func (f *NFSFilesystem) create(filename string, flag int, perm os.FileMode) (billy.File, error) {
	parent, name, err := f.resolveParent(filename)
	if err != nil {
		return nil, err
	}
	defer f.release(parent)

	if perm == 0 {
		perm = 0o644
	}
	var out fuse.CreateOut
	in := &fuse.CreateIn{
		InHeader: *f.header(parent),
		Flags:    uint32(flag),
		Mode:     uint32(perm.Perm()),
	}
	in.Uid = f.uid
	in.Gid = f.gid
	if st := f.raw.Create(nil, in, name, &out); st != fuse.OK {
		return nil, statusError(st)
	}
	return &nfsFile{fs: f, name: f.abs(filename), ino: out.NodeId, fh: out.Fh}, nil
}

func (f *NFSFilesystem) ReadDir(dirname string) ([]os.FileInfo, error) {
	ino, attr, err := f.resolve(dirname)
	if err != nil {
		log.Error(err, "nfs readdir resolve failed", "dir", dirname)
		return nil, err
	}
	defer f.release(ino)
	if attr.Mode&syscall.S_IFMT != syscall.S_IFDIR {
		return nil, syscall.ENOTDIR
	}

	names, err := f.listNames(ino)
	if err != nil {
		log.Error(err, "nfs readdir failed", "dir", dirname)
		return nil, err
	}

	infos := make([]os.FileInfo, 0, len(names))
	for _, name := range names {
		var out fuse.EntryOut
		if st := f.raw.Lookup(nil, f.header(ino), name, &out); st != fuse.OK {
			continue
		}
		entryAttr := out.Attr
		f.release(out.NodeId)
		infos = append(infos, newNFSFileInfo(name, &entryAttr))
	}
	return infos, nil
}

// listNames drains ReadDir from the raw layer, decoding entries back out of the
// fuse_dirent buffer rather than reimplementing the pxar/journal directory merge.
// Both raw layers add entries with auto-incrementing offsets, so the offset
// delta across a call is the number of entries it wrote.
func (f *NFSFilesystem) listNames(ino uint64) ([]string, error) {
	buf := make([]byte, readDirBufSize)
	var names []string
	var offset uint64

	for {
		list := fuse.NewDirEntryList(buf, offset)
		in := &fuse.ReadIn{InHeader: *f.header(ino), Size: uint32(len(buf)), Offset: offset}
		if st := f.raw.ReadDir(nil, in, list); st != fuse.OK {
			return nil, statusError(st)
		}
		count := int(list.Offset - offset)
		if count <= 0 {
			return names, nil
		}
		offset = list.Offset

		for _, name := range decodeDirents(buf, count) {
			if name == "." || name == ".." {
				continue
			}
			names = append(names, name)
		}
	}
}

func decodeDirents(buf []byte, count int) []string {
	names := make([]string, 0, count)
	off := 0
	for range count {
		if off+fuseDirentHeaderSize > len(buf) {
			break
		}
		nameLen := int(binary.LittleEndian.Uint32(buf[off+16 : off+20]))
		start := off + fuseDirentHeaderSize
		if start+nameLen > len(buf) {
			break
		}
		names = append(names, string(buf[start:start+nameLen]))
		off = start + nameLen + (8-nameLen&7)&7
	}
	return names
}

func (f *NFSFilesystem) MkdirAll(filename string, perm os.FileMode) error {
	if err := f.mutationError(); err != nil {
		return err
	}
	if perm == 0 {
		perm = 0o755
	}

	full := f.abs(filename)
	parts := splitPath(full)
	ino := RootInode
	for _, name := range parts {
		if name == "" || name == "." {
			continue
		}
		var out fuse.EntryOut
		st := f.raw.Lookup(nil, f.header(ino), name, &out)
		if st == fuse.OK {
			f.release(ino)
			ino = out.NodeId
			if out.Attr.Mode&syscall.S_IFMT != syscall.S_IFDIR {
				f.release(ino)
				return syscall.ENOTDIR
			}
			continue
		}

		in := &fuse.MkdirIn{InHeader: *f.header(ino), Mode: uint32(perm.Perm())}
		in.Uid = f.uid
		in.Gid = f.gid
		var mkOut fuse.EntryOut
		st = f.raw.Mkdir(nil, in, name, &mkOut)
		f.release(ino)
		if st != fuse.OK {
			return statusError(st)
		}
		ino = mkOut.NodeId
	}
	f.release(ino)
	return nil
}

func (f *NFSFilesystem) Remove(filename string) error {
	if err := f.mutationError(); err != nil {
		return err
	}

	ino, attr, err := f.resolve(filename)
	if err != nil {
		return err
	}
	isDir := attr.Mode&syscall.S_IFMT == syscall.S_IFDIR
	f.release(ino)

	parent, name, err := f.resolveParent(filename)
	if err != nil {
		return err
	}
	defer f.release(parent)

	st := f.raw.Unlink(nil, f.header(parent), name)
	if isDir {
		st = f.raw.Rmdir(nil, f.header(parent), name)
	}
	return statusError(st)
}

func (f *NFSFilesystem) Rename(oldpath, newpath string) error {
	if err := f.mutationError(); err != nil {
		return err
	}

	oldParent, oldName, err := f.resolveParent(oldpath)
	if err != nil {
		return err
	}
	defer f.release(oldParent)

	newParent, newName, err := f.resolveParent(newpath)
	if err != nil {
		return err
	}
	defer f.release(newParent)

	in := &fuse.RenameIn{InHeader: *f.header(oldParent), Newdir: newParent}
	return statusError(f.raw.Rename(nil, in, oldName, newName))
}

func (f *NFSFilesystem) Symlink(target, link string) error {
	if err := f.mutationError(); err != nil {
		return err
	}
	if dir := path.Dir(link); dir != "." && dir != "/" {
		if err := f.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}

	parent, name, err := f.resolveParent(link)
	if err != nil {
		return err
	}
	defer f.release(parent)

	var out fuse.EntryOut
	st := f.raw.Symlink(nil, f.header(parent), target, name, &out)
	if st != fuse.OK {
		return statusError(st)
	}
	f.release(out.NodeId)
	return nil
}

func (f *NFSFilesystem) Readlink(link string) (string, error) {
	ino, _, err := f.resolve(link)
	if err != nil {
		return "", err
	}
	defer f.release(ino)

	target, st := f.raw.Readlink(nil, f.header(ino))
	if st != fuse.OK {
		return "", statusError(st)
	}
	return string(target), nil
}

// TempFile places the file inside the export; callers must remove it.
func (f *NFSFilesystem) TempFile(dir, prefix string) (billy.File, error) {
	if err := f.mutationError(); err != nil {
		return nil, err
	}
	if dir == "" {
		dir = "/"
	}
	if err := f.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	name := f.Join(dir, prefix+strconv.FormatInt(time.Now().UnixNano(), 36))
	return f.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
}

func (f *NFSFilesystem) Join(elem ...string) string {
	return path.Join(elem...)
}

func (f *NFSFilesystem) Root() string { return f.root }

func (f *NFSFilesystem) Chroot(p string) (billy.Filesystem, error) {
	child := *f
	child.root = f.abs(p)
	return &child, nil
}

func (f *NFSFilesystem) Chmod(name string, mode os.FileMode) error {
	return f.setAttr(name, func(in *fuse.SetAttrIn) {
		in.Valid |= fuse.FATTR_MODE
		in.Mode = uint32(mode.Perm())
	})
}

func (f *NFSFilesystem) Chown(name string, uid, gid int) error {
	return f.setAttr(name, func(in *fuse.SetAttrIn) {
		if uid >= 0 {
			in.Valid |= fuse.FATTR_UID
			in.Uid = uint32(uid)
		}
		if gid >= 0 {
			in.Valid |= fuse.FATTR_GID
			in.Gid = uint32(gid)
		}
	})
}

// Lchown matches Chown: resolve never follows links, so there is nothing to skip.
func (f *NFSFilesystem) Lchown(name string, uid, gid int) error {
	return f.Chown(name, uid, gid)
}

func (f *NFSFilesystem) Chtimes(name string, atime, mtime time.Time) error {
	return f.setAttr(name, func(in *fuse.SetAttrIn) {
		in.Valid |= fuse.FATTR_ATIME | fuse.FATTR_MTIME
		in.Atime = uint64(atime.Unix())
		in.Atimensec = uint32(atime.Nanosecond())
		in.Mtime = uint64(mtime.Unix())
		in.Mtimensec = uint32(mtime.Nanosecond())
	})
}

func (f *NFSFilesystem) setAttr(name string, fill func(*fuse.SetAttrIn)) error {
	if err := f.mutationError(); err != nil {
		return err
	}
	ino, _, err := f.resolve(name)
	if err != nil {
		return err
	}
	defer f.release(ino)

	in := &fuse.SetAttrIn{}
	in.InHeader = *f.header(ino)
	fill(in)

	var out fuse.AttrOut
	return statusError(f.raw.SetAttr(nil, in, &out))
}

type nfsFile struct {
	fs   *NFSFilesystem
	name string
	ino  uint64
	fh   uint64

	mu     sync.Mutex
	offset int64
	size   int64
	closed bool
}

var _ billy.File = (*nfsFile)(nil)

func (o *nfsFile) Name() string { return o.name }

func (o *nfsFile) Read(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	n, err := o.readAt(p, o.offset)
	o.offset += int64(n)
	return n, err
}

func (o *nfsFile) ReadAt(p []byte, off int64) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.readAt(p, off)
}

func (o *nfsFile) readAt(p []byte, off int64) (int, error) {
	if o.closed {
		return 0, os.ErrClosed
	}
	if off < 0 {
		return 0, os.ErrInvalid
	}
	if len(p) == 0 {
		return 0, nil
	}

	in := &fuse.ReadIn{
		InHeader: *o.fs.header(o.ino),
		Fh:       o.fh,
		Offset:   uint64(off),
		Size:     uint32(len(p)),
	}
	res, st := o.fs.raw.Read(nil, in, p)
	if st != fuse.OK {
		return 0, statusError(st)
	}
	data, st := res.Bytes(p)
	res.Done()
	if st != fuse.OK {
		return 0, statusError(st)
	}
	if len(data) == 0 {
		return 0, io.EOF
	}
	if &data[0] != &p[0] {
		copy(p, data)
	}
	if len(data) < len(p) {
		return len(data), io.EOF
	}
	return len(data), nil
}

func (o *nfsFile) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return 0, os.ErrClosed
	}
	if err := o.fs.mutationError(); err != nil {
		return 0, err
	}

	in := &fuse.WriteIn{
		InHeader: *o.fs.header(o.ino),
		Fh:       o.fh,
		Offset:   uint64(o.offset),
		Size:     uint32(len(p)),
	}
	n, st := o.fs.raw.Write(nil, in, p)
	if st != fuse.OK {
		return int(n), statusError(st)
	}
	o.offset += int64(n)
	o.size = max(o.size, o.offset)
	if int(n) != len(p) {
		return int(n), io.ErrShortWrite
	}
	return int(n), nil
}

func (o *nfsFile) Seek(offset int64, whence int) (int64, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return 0, os.ErrClosed
	}

	switch whence {
	case io.SeekStart:
		o.offset = offset
	case io.SeekCurrent:
		o.offset += offset
	case io.SeekEnd:
		o.offset = o.size + offset
	default:
		return 0, os.ErrInvalid
	}
	if o.offset < 0 {
		o.offset = 0
		return 0, os.ErrInvalid
	}
	return o.offset, nil
}

func (o *nfsFile) Truncate(size int64) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return os.ErrClosed
	}
	if size < 0 {
		return os.ErrInvalid
	}
	if err := o.fs.mutationError(); err != nil {
		return err
	}

	in := &fuse.SetAttrIn{}
	in.InHeader = *o.fs.header(o.ino)
	in.Valid = fuse.FATTR_SIZE | fuse.FATTR_FH
	in.Fh = o.fh
	in.Size = uint64(size)

	var out fuse.AttrOut
	if st := o.fs.raw.SetAttr(nil, in, &out); st != fuse.OK {
		return statusError(st)
	}
	o.size = size
	return nil
}

// Lock is a no-op: NFSv3 locking is the separate NLM protocol, which go-nfs
// does not serve, so there is no lock state to hold.
func (o *nfsFile) Lock() error { return nil }

func (o *nfsFile) Unlock() error { return nil }

// Close must both Flush and Release: MutableFS persists deferred write metadata
// to the journal in Flush and closes the backing fd in Release.
func (o *nfsFile) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return os.ErrClosed
	}
	o.closed = true

	flush := &fuse.FlushIn{InHeader: *o.fs.header(o.ino), Fh: o.fh}
	st := o.fs.raw.Flush(nil, flush)

	release := &fuse.ReleaseIn{InHeader: *o.fs.header(o.ino), Fh: o.fh}
	o.fs.raw.Release(nil, release)
	o.fs.release(o.ino)

	return statusError(st)
}

type nfsFileInfo struct {
	name string
	attr fuse.Attr
}

var _ os.FileInfo = (*nfsFileInfo)(nil)

func newNFSFileInfo(name string, attr *fuse.Attr) *nfsFileInfo {
	return &nfsFileInfo{name: name, attr: *attr}
}

func (i *nfsFileInfo) Name() string { return i.name }

func (i *nfsFileInfo) Size() int64 { return int64(i.attr.Size) }

func (i *nfsFileInfo) ModTime() time.Time {
	return time.Unix(int64(i.attr.Mtime), int64(i.attr.Mtimensec))
}

func (i *nfsFileInfo) IsDir() bool { return i.attr.Mode&syscall.S_IFMT == syscall.S_IFDIR }

func (i *nfsFileInfo) Sys() any {
	nlink := max(i.attr.Nlink, 1)
	return &syscall.Stat_t{
		Ino:   i.attr.Ino,
		Mode:  i.attr.Mode,
		Nlink: uint64(nlink),
		Uid:   i.attr.Uid,
		Gid:   i.attr.Gid,
		Size:  int64(i.attr.Size),
		Rdev:  uint64(i.attr.Rdev),
		Atim:  syscall.Timespec{Sec: int64(i.attr.Atime), Nsec: int64(i.attr.Atimensec)},
		Mtim:  syscall.Timespec{Sec: int64(i.attr.Mtime), Nsec: int64(i.attr.Mtimensec)},
		Ctim:  syscall.Timespec{Sec: int64(i.attr.Ctime), Nsec: int64(i.attr.Ctimensec)},
	}
}

func (i *nfsFileInfo) Mode() os.FileMode {
	mode := os.FileMode(i.attr.Mode & 0o777)
	switch i.attr.Mode & syscall.S_IFMT {
	case syscall.S_IFDIR:
		mode |= os.ModeDir
	case syscall.S_IFLNK:
		mode |= os.ModeSymlink
	case syscall.S_IFIFO:
		mode |= os.ModeNamedPipe
	case syscall.S_IFSOCK:
		mode |= os.ModeSocket
	case syscall.S_IFBLK:
		mode |= os.ModeDevice
	case syscall.S_IFCHR:
		mode |= os.ModeDevice | os.ModeCharDevice
	}
	if i.attr.Mode&syscall.S_ISUID != 0 {
		mode |= os.ModeSetuid
	}
	if i.attr.Mode&syscall.S_ISGID != 0 {
		mode |= os.ModeSetgid
	}
	if i.attr.Mode&syscall.S_ISVTX != 0 {
		mode |= os.ModeSticky
	}
	return mode
}
