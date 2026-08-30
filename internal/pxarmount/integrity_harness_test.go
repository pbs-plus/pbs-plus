package pxarmount

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"
)

// Spans several pages so partial writes leave real holes between extents.
const lowerFileSize = 40000

var (
	lowerFiles = []string{"/lower_a.bin", "/lower_b.bin", "/sub/lower_c.bin"}
	lowerDirs  = []string{"/sub"}
)

// Position-dependent so a read served from the wrong layer cannot coincidentally match.
func lowerContent(name string, size int) []byte {
	out := make([]byte, size)
	seed := byte(len(name))
	for i := range out {
		out[i] = byte(i*31) ^ seed ^ 0x5A
	}
	return out
}

func createIntegrityArchive(t *testing.T) (string, string, string) {
	t.Helper()
	dir := t.TempDir()

	config, _ := buzhash.NewConfig(4096)
	ls, err := backupproxy.NewLocalStore(dir, config, false)
	if err != nil {
		t.Fatal(err)
	}
	sess, err := ls.StartSession(context.TODO(), backupproxy.BackupConfig{
		BackupType: datastore.BackupVM,
		BackupID:   "integrity",
	})
	if err != nil {
		t.Fatal(err)
	}

	writer := transfer.NewSessionWriter(context.TODO(), sess, "root.mpxar.didx", "root.ppxar.didx")
	rootMeta := pxar.DirMetadata(0o755).Build()
	if err := writer.Begin(&rootMeta, transfer.Options{Format: format.FormatVersion2}); err != nil {
		t.Fatal(err)
	}

	fileMeta := pxar.FileMetadata(0o644).Build()
	writeFile := func(name string) {
		body := lowerContent(name, lowerFileSize)
		if err := writer.WriteEntry(&pxar.Entry{
			Path:     name,
			Kind:     pxar.KindFile,
			Metadata: fileMeta,
			FileSize: uint64(len(body)),
		}, body); err != nil {
			t.Fatal(err)
		}
	}

	writeFile("lower_a.bin")
	writeFile("lower_b.bin")

	dirMeta := pxar.DirMetadata(0o755).Build()
	if err := writer.BeginDirectory("sub", &dirMeta); err != nil {
		t.Fatal(err)
	}
	writeFile("lower_c.bin")

	if err := writer.Finish(); err != nil {
		t.Fatal(err)
	}
	if _, err := sess.Finish(context.TODO()); err != nil {
		t.Fatal(err)
	}

	return dir, filepath.Join(dir, "root.mpxar.didx"), filepath.Join(dir, "root.ppxar.didx")
}

type modelEntry struct {
	isDir bool
	data  []byte
}

// harness drives MutableFS through the raw FUSE entry points and mirrors every
// mutation into a model covering both namespace and content.
type harness struct {
	t       *testing.T
	mfs     *MutableFS
	journal *Journal
	root    string
	store   string
	meta    string
	payload string

	model   map[string]*modelEntry
	deleted map[string]bool
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	store, meta, payload := createIntegrityArchive(t)
	h := &harness{
		t:       t,
		root:    t.TempDir(),
		store:   store,
		meta:    meta,
		payload: payload,
		model:   map[string]*modelEntry{"/": {isDir: true}},
		deleted: make(map[string]bool),
	}
	for _, d := range lowerDirs {
		h.model[d] = &modelEntry{isDir: true}
	}
	for _, p := range lowerFiles {
		h.model[p] = &modelEntry{data: lowerContent(filepath.Base(p), lowerFileSize)}
	}
	h.mount()
	t.Cleanup(func() {
		if h.journal != nil {
			_ = h.journal.Close()
		}
	})
	return h
}

func (h *harness) mount() {
	h.t.Helper()
	pxarFS := openTestArchive(h.t, h.store, h.meta, h.payload)
	journal, err := OpenJournal(filepath.Join(h.root, "journal"))
	if err != nil {
		h.t.Fatal(err)
	}
	h.journal = journal
	h.mfs = NewMutableFS(pxarFS, journal, filepath.Join(h.root, "overlay"))
	if err := h.mfs.InitMutableRoot(); err != nil {
		h.t.Fatal(err)
	}
	h.mfs.mapInode(RootInode, "/")
}

// remountAfterCrash models a process kill: overlay and pending state are lost,
// only pebble-committed bytes survive.
func (h *harness) remountAfterCrash() {
	h.t.Helper()
	h.journal.abandonForTest()
	h.journal = nil
	h.mount()
	if err := h.mfs.ReconcileMutableDir(); err != nil {
		h.t.Fatalf("reconcile: %v", err)
	}
}

func (h *harness) remountClean() {
	h.t.Helper()
	if err := h.journal.Sync(); err != nil {
		h.t.Fatalf("sync: %v", err)
	}
	if err := h.journal.Close(); err != nil {
		h.t.Fatalf("close: %v", err)
	}
	h.journal = nil
	h.mount()
	if err := h.mfs.ReconcileMutableDir(); err != nil {
		h.t.Fatalf("reconcile: %v", err)
	}
}

func (h *harness) ino(path string) uint64 {
	h.t.Helper()
	re, status := h.mfs.resolve(path)
	if status != fuse.OK {
		h.t.Fatalf("resolve(%q): %s", path, status)
	}
	return re.Inode
}

func parentOf(path string) string {
	p := filepath.Dir(path)
	if p == "." {
		return "/"
	}
	return p
}

func (h *harness) open(path string, flags uint32) (uint64, uint64) {
	h.t.Helper()
	ino := h.ino(path)
	var out fuse.OpenOut
	in := &fuse.OpenIn{Flags: flags}
	in.NodeId = ino
	if status := h.mfs.Open(nil, in, &out); status != fuse.OK {
		h.t.Fatalf("open(%q): %s", path, status)
	}
	return ino, out.Fh
}

func (h *harness) release(ino, fh uint64) {
	in := &fuse.ReleaseIn{Fh: fh}
	in.NodeId = ino
	h.mfs.Release(nil, in)
}

func (h *harness) create(path string) {
	h.t.Helper()
	var out fuse.CreateOut
	in := &fuse.CreateIn{Flags: uint32(os.O_RDWR), Mode: 0o644}
	in.NodeId = h.ino(parentOf(path))
	if status := h.mfs.Create(nil, in, filepath.Base(path), &out); status != fuse.OK {
		h.t.Fatalf("create(%q): %s", path, status)
	}
	h.release(out.NodeId, out.Fh)
	h.model[path] = &modelEntry{}
	delete(h.deleted, path)
}

func (h *harness) mkdir(path string) {
	h.t.Helper()
	var out fuse.EntryOut
	in := &fuse.MkdirIn{Mode: 0o755}
	in.NodeId = h.ino(parentOf(path))
	if status := h.mfs.Mkdir(nil, in, filepath.Base(path), &out); status != fuse.OK {
		h.t.Fatalf("mkdir(%q): %s", path, status)
	}
	h.model[path] = &modelEntry{isDir: true}
	delete(h.deleted, path)
}

func (h *harness) unlink(path string) {
	h.t.Helper()
	hdr := &fuse.InHeader{NodeId: h.ino(parentOf(path))}
	if status := h.mfs.Unlink(nil, hdr, filepath.Base(path)); status != fuse.OK {
		h.t.Fatalf("unlink(%q): %s", path, status)
	}
	delete(h.model, path)
	h.deleted[path] = true
}

func (h *harness) rmdir(path string) {
	h.t.Helper()
	hdr := &fuse.InHeader{NodeId: h.ino(parentOf(path))}
	if status := h.mfs.Rmdir(nil, hdr, filepath.Base(path)); status != fuse.OK {
		h.t.Fatalf("rmdir(%q): %s", path, status)
	}
	delete(h.model, path)
	h.deleted[path] = true
}

func (h *harness) rename(oldPath, newPath string) {
	h.t.Helper()
	in := &fuse.RenameIn{Newdir: h.ino(parentOf(newPath))}
	in.NodeId = h.ino(parentOf(oldPath))
	status := h.mfs.Rename(nil, in, filepath.Base(oldPath), filepath.Base(newPath))
	if status != fuse.OK {
		h.t.Fatalf("rename(%q -> %q): %s", oldPath, newPath, status)
	}
	entry := h.model[oldPath]
	delete(h.model, oldPath)
	h.model[newPath] = entry
	h.deleted[oldPath] = true
	delete(h.deleted, newPath)
}

func (h *harness) write(path string, off int, data []byte) {
	h.t.Helper()
	ino, fh := h.open(path, uint32(os.O_RDWR))
	defer h.release(ino, fh)

	in := &fuse.WriteIn{Fh: fh, Offset: uint64(off), Size: uint32(len(data))}
	in.NodeId = ino
	n, status := h.mfs.Write(nil, in, data)
	if status != fuse.OK {
		h.t.Fatalf("write(%q, %d): %s", path, off, status)
	}
	if int(n) != len(data) {
		h.t.Fatalf("write(%q, %d) = %d, want %d", path, off, n, len(data))
	}

	cur := h.model[path].data
	if end := off + len(data); end > len(cur) {
		grown := make([]byte, end)
		copy(grown, cur)
		cur = grown
	}
	copy(cur[off:], data)
	h.model[path].data = cur

	h.flush(ino, fh)
}

func (h *harness) truncate(path string, size int) {
	h.t.Helper()
	in := &fuse.SetAttrIn{}
	in.NodeId = h.ino(path)
	in.Valid = fuse.FATTR_SIZE
	in.Size = uint64(size)
	var out fuse.AttrOut
	if status := h.mfs.SetAttr(nil, in, &out); status != fuse.OK {
		h.t.Fatalf("truncate(%q, %d): %s", path, size, status)
	}
	resized := make([]byte, size)
	copy(resized, h.model[path].data)
	h.model[path].data = resized
}

func (h *harness) flush(ino, fh uint64) {
	h.t.Helper()
	in := &fuse.FlushIn{Fh: fh}
	in.NodeId = ino
	if status := h.mfs.Flush(nil, in); status != fuse.OK {
		h.t.Fatalf("flush: %s", status)
	}
}

func (h *harness) fsync(path string) {
	h.t.Helper()
	ino, fh := h.open(path, uint32(os.O_RDWR))
	defer h.release(ino, fh)
	in := &fuse.FsyncIn{Fh: fh}
	in.NodeId = ino
	if status := h.mfs.Fsync(nil, in); status != fuse.OK {
		h.t.Fatalf("fsync(%q): %s", path, status)
	}
}

func (h *harness) readAll(path string) []byte {
	h.t.Helper()
	ino, fh := h.open(path, uint32(os.O_RDONLY))
	defer h.release(ino, fh)

	size := len(h.model[path].data)
	out := make([]byte, 0, size)
	buf := make([]byte, 4096)
	for off := 0; off < size; {
		in := &fuse.ReadIn{Fh: fh, Offset: uint64(off), Size: uint32(len(buf))}
		in.NodeId = ino
		res, status := h.mfs.Read(nil, in, buf)
		if status != fuse.OK {
			h.t.Fatalf("read(%q, %d): %s", path, off, status)
		}
		got, status := res.Bytes(buf)
		if status != fuse.OK {
			h.t.Fatalf("read bytes(%q, %d): %s", path, off, status)
		}
		if len(got) == 0 {
			break
		}
		out = append(out, got...)
		off += len(got)
	}
	return out
}

// readdir returns the entry names of a directory, excluding . and .., using a
// deliberately small buffer so that offset-resume paging is exercised.
func (h *harness) readdir(path string, bufSize int) []string {
	h.t.Helper()
	ino := h.ino(path)

	var names []string
	seen := make(map[string]bool)
	sawDot := false
	for offset := uint64(0); ; {
		buf := make([]byte, bufSize)
		list := fuse.NewDirEntryList(buf, offset)
		in := &fuse.ReadIn{Offset: offset}
		in.NodeId = ino
		if status := h.mfs.ReadDir(nil, in, list); status != fuse.OK {
			h.t.Fatalf("readdir(%q): %s", path, status)
		}
		next := list.Offset
		if next == offset {
			break
		}
		for _, n := range decodeDirNames(h.t, buf, int(next-offset)) {
			if n == "." {
				sawDot = true
			}
			if n == "." || n == ".." {
				continue
			}
			if seen[n] {
				h.t.Fatalf("readdir(%q): duplicate entry %q across paging", path, n)
			}
			seen[n] = true
			names = append(names, n)
		}
		offset = next
	}
	if !sawDot {
		h.t.Fatalf("readdir(%q): dirent decode produced no \".\" entry", path)
	}
	slices.Sort(names)
	return names
}

// verifyNamespace asserts every modelled path is reachable by both lookup and
// readdir, that types match, and that deleted paths are gone from both.
func (h *harness) verifyNamespace(bufSize int) {
	h.t.Helper()

	for path, want := range h.model {
		if path == "/" {
			continue
		}
		var out fuse.EntryOut
		hdr := &fuse.InHeader{NodeId: h.ino(parentOf(path))}
		if status := h.mfs.Lookup(nil, hdr, filepath.Base(path), &out); status != fuse.OK {
			h.t.Fatalf("lookup(%q): %s, want OK", path, status)
		}
		gotDir := out.Attr.Mode&fuse.S_IFDIR != 0
		if gotDir != want.isDir {
			h.t.Fatalf("lookup(%q): isDir=%v, want %v (mode 0%o)", path, gotDir, want.isDir, out.Attr.Mode)
		}
		if !want.isDir && out.Attr.Size != uint64(len(want.data)) {
			h.t.Fatalf("lookup(%q): size %d, want %d", path, out.Attr.Size, len(want.data))
		}
	}

	for path, want := range h.model {
		if !want.isDir {
			continue
		}
		got := h.readdir(path, bufSize)
		expected := h.childrenOf(path)
		if !slices.Equal(got, expected) {
			h.t.Fatalf("readdir(%q) = %v, want %v", path, got, expected)
		}
	}

	for path := range h.deleted {
		if _, alive := h.model[path]; alive {
			continue
		}
		var out fuse.EntryOut
		parent := parentOf(path)
		if _, ok := h.model[parent]; !ok {
			continue
		}
		hdr := &fuse.InHeader{NodeId: h.ino(parent)}
		if status := h.mfs.Lookup(nil, hdr, filepath.Base(path), &out); status != fuse.ENOENT {
			h.t.Fatalf("lookup(%q) after delete: %s, want ENOENT", path, status)
		}
	}
}

func (h *harness) childrenOf(dir string) []string {
	prefix := dir
	if prefix != "/" {
		prefix += "/"
	}
	var out []string
	for path := range h.model {
		if path == "/" || !strings.HasPrefix(path, prefix) {
			continue
		}
		rest := path[len(prefix):]
		if rest == "" || strings.Contains(rest, "/") {
			continue
		}
		out = append(out, rest)
	}
	slices.Sort(out)
	return out
}

// verifyContent reads every modelled file back in full and compares bytes.
func (h *harness) verifyContent() {
	h.t.Helper()
	for path, want := range h.model {
		if want.isDir {
			continue
		}
		got := h.readAll(path)
		if len(got) != len(want.data) {
			h.t.Fatalf("%s: length %d, want %d", path, len(got), len(want.data))
		}
		for i := range want.data {
			if got[i] != want.data[i] {
				h.t.Fatalf("%s: byte %d = %#x, want %#x (%d bytes differ)",
					path, i, got[i], want.data[i], countDiff(got, want.data))
			}
		}
	}
}

func (h *harness) verify() {
	h.t.Helper()
	h.verifyNamespace(4096)
	h.verifyContent()
}

// abandonForTest drops the journal's in-memory state before stopping the
// commit loop, so the final drain finds nothing. That is exactly what a
// process kill leaves behind: only pebble-committed keys.
func (j *Journal) abandonForTest() {
	j.mu.Lock()
	j.pending = nil
	j.overlay = make(map[string][]byte)
	j.mu.Unlock()

	close(j.stopCh)
	<-j.stopped
	_ = j.db.Close()
}

const direntHeaderSize = 24

// decodeDirNames parses the fuse_dirent wire format go-fuse writes into the
// caller's buffer: {ino u64, off u64, namelen u32, type u32}, name, pad to 8.
func decodeDirNames(t *testing.T, buf []byte, count int) []string {
	t.Helper()
	names := make([]string, 0, count)
	off := 0
	for range count {
		if off+direntHeaderSize > len(buf) {
			t.Fatalf("dirent buffer truncated at %d", off)
		}
		nameLen := int(binary.LittleEndian.Uint32(buf[off+16 : off+20]))
		start := off + direntHeaderSize
		if nameLen < 0 || start+nameLen > len(buf) {
			t.Fatalf("dirent name length %d overruns buffer", nameLen)
		}
		names = append(names, string(buf[start:start+nameLen]))
		off = start + nameLen
		off += (8 - nameLen&7) & 7
	}
	return names
}

func countDiff(a, b []byte) int {
	n := 0
	for i := range a {
		if a[i] != b[i] {
			n++
		}
	}
	return n
}

func fillPattern(tag byte, size int) []byte {
	out := make([]byte, size)
	for i := range out {
		out[i] = tag ^ byte(i)
	}
	return out
}

func (h *harness) filePaths() []string {
	var out []string
	for p, e := range h.model {
		if !e.isDir {
			out = append(out, p)
		}
	}
	slices.Sort(out)
	return out
}

func (h *harness) dirPaths() []string {
	var out []string
	for p, e := range h.model {
		if e.isDir {
			out = append(out, p)
		}
	}
	slices.Sort(out)
	return out
}

func newFileName(i int) string { return fmt.Sprintf("new_%d.bin", i) }
func newDirName(i int) string  { return fmt.Sprintf("newdir_%d", i) }
