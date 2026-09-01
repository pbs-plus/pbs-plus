//go:build linux

package arpcfs

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"os"
	"sync"
	"syscall"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs"
	"github.com/puzpuzpuz/xsync/v4"
)

func readAtBytes(b []byte) func(ctx context.Context, p []byte, off int64) (int, error) {
	r := bytes.NewReader(b)
	return func(ctx context.Context, p []byte, off int64) (int, error) {
		return r.ReadAt(p, off)
	}
}

func buildTestZip(t *testing.T, files map[string]int) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, size := range files {
		hdr := &zip.FileHeader{Name: name, Method: zip.Deflate}
		if len(name) > 5 && name[:5] == "store" {
			hdr.Method = zip.Store
		}
		w, err := zw.CreateHeader(hdr)
		if err != nil {
			t.Fatal(err)
		}
		prng := rand.New(rand.NewSource(int64(crc32.ChecksumIEEE([]byte(name)))))
		if _, err := io.CopyN(w, prng, int64(size)); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func testOverlay(t *testing.T, data []byte) *zipOverlay {
	t.Helper()
	ov, err := parseZipOverlay(readAtBytes(data), int64(len(data)), zipMaxEntries)
	if err != nil {
		t.Fatalf("parseZipOverlay: %v", err)
	}
	ov.zipPath = "/data/test.zip"
	return ov
}

func testFS(ovs ...*zipOverlay) *ARPCFS {
	fs := &ARPCFS{
		VFSBase: &vfs.VFSBase{
			FileCount:     xsync.NewCounter(),
			FolderCount:   xsync.NewCounter(),
			TotalBytes:    xsync.NewCounter(),
			StatCacheHits: xsync.NewCounter(),
		},
		expandArchives: true,
		zipOverlays:    map[string]*zipOverlay{},
		zipAnchors:     map[string][]*zipOverlay{},
		zipSkipped:     map[string]struct{}{},
	}
	for _, ov := range ovs {
		fs.zipOverlays[ov.zipPath] = ov
		anchor := "/"
		if i := len(ov.zipPath) - 1; i >= 0 {
			for j := i - 1; j >= 0; j-- {
				if ov.zipPath[j] == '/' {
					anchor = ov.zipPath[:j]
					if anchor == "" {
						anchor = "/"
					}
					break
				}
			}
		}
		fs.zipAnchors[anchor] = append(fs.zipAnchors[anchor], ov)
	}
	return fs
}

func TestZipParseAndResolve(t *testing.T) {
	data := buildTestZip(t, map[string]int{
		"store.txt":     4096,
		"big.bin":       3 << 20,
		"dir/nested.tx": 1000,
	})
	ov := testOverlay(t, data)

	for _, name := range []string{"store.txt", "big.bin", "dir/nested.tx"} {
		if _, ok := ov.byName[name]; !ok {
			t.Errorf("byName missing %s", name)
		}
	}
	if _, ok := ov.byName["dir"]; ok {
		t.Error("dir path registered as file")
	}
	if ov.dirs[""] == nil || ov.dirs["dir"] == nil {
		t.Fatal("missing virtual dirs")
	}
	var rootNames []string
	for _, c := range ov.dirs[""].children {
		rootNames = append(rootNames, c.name)
	}
	if len(rootNames) != 3 {
		t.Errorf("root children = %v", rootNames)
	}

	fs := testFS(ov)
	if fi, errno, ok := fs.zipAttr("/data/test.zip"); !ok || !errors.Is(errno, syscall.ENOENT) {
		t.Errorf("hidden zip: ok=%v errno=%v fi=%+v", ok, errno, fi)
	}
	if fi, errno, ok := fs.zipAttr("/data/store.txt"); !ok || errno != nil || fi.Size != 4096 || fi.IsDir {
		t.Errorf("file attr: ok=%v errno=%v fi=%+v", ok, errno, fi)
	}
	if fi, errno, ok := fs.zipAttr("/data/dir"); !ok || errno != nil || !fi.IsDir {
		t.Errorf("dir attr: ok=%v errno=%v fi=%+v", ok, errno, fi)
	}
	if fi, errno, ok := fs.zipAttr("/data/dir/nested.tx"); !ok || errno != nil || fi.Size != 1000 {
		t.Errorf("nested attr: ok=%v errno=%v fi=%+v", ok, errno, fi)
	}
	if _, _, ok := fs.zipAttr("/data/nope"); ok {
		t.Error("unknown path handled; must fall through to agent")
	}
}

func TestZipReadStore(t *testing.T) {
	data := buildTestZip(t, map[string]int{"store.txt": 1 << 16})
	ov := testOverlay(t, data)
	idx := ov.byName["store.txt"]
	zs := &zipFileState{ov: ov, ent: &ov.entries[idx], uncomp: ov.entries[idx].uncompSize}

	want := contentOf(t, data, "store.txt")
	var dest []byte
	for _, off := range []int64{0, 7, 30000, 65535} {
		dest = make([]byte, 512)
		n, err := zs.ReadAt(context.Background(), dest, off)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("read@%d: %v", off, err)
		}
		if got := string(dest[:n]); got != string(want[off:off+int64(n)]) {
			t.Errorf("read@%d mismatch", off)
		}
	}
	n, err := zs.ReadAt(context.Background(), dest, 1<<16)
	if n != 0 || !errors.Is(err, io.EOF) {
		t.Errorf("past EOF: n=%d err=%v", n, err)
	}
}

func TestZipReadDeflate(t *testing.T) {
	data := buildTestZip(t, map[string]int{"big.bin": 3 << 20})
	ov := testOverlay(t, data)
	idx := ov.byName["big.bin"]
	e := &ov.entries[idx]
	if e.method != 8 {
		t.Fatalf("method = %d, want deflate", e.method)
	}
	zs := &zipFileState{ov: ov, ent: e, uncomp: e.uncompSize}
	want := contentOf(t, data, "big.bin")

	dest := make([]byte, 128<<10)
	var off int64
	for off < int64(len(want)) {
		n, err := zs.ReadAt(context.Background(), dest, off)
		if n > 0 && string(dest[:n]) != string(want[off:off+int64(n)]) {
			t.Fatalf("sequential mismatch at %d", off)
		}
		off += int64(n)
		if err != nil {
			break
		}
	}
	if off != int64(len(want)) {
		t.Errorf("sequential read got %d bytes, want %d", off, len(want))
	}

	zs2 := &zipFileState{ov: ov, ent: e, uncomp: e.uncompSize}
	buf := make([]byte, 4096)
	n, err := zs2.ReadAt(context.Background(), buf, int64(len(want))-100)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("tail read: %v", err)
	}
	if string(buf[:n]) != string(want[len(want)-100:]) {
		t.Error("tail mismatch")
	}
	n, err = zs2.ReadAt(context.Background(), buf, 5)
	if err != nil {
		t.Fatalf("backward read: %v", err)
	}
	if string(buf[:n]) != string(want[5:5+n]) {
		t.Error("backward (restart) mismatch")
	}

	zs3 := &zipFileState{ov: ov, ent: e, uncomp: e.uncompSize}
	n, err = zs3.ReadAt(context.Background(), buf, (2<<20)+37)
	if err != nil {
		t.Fatalf("forward jump: %v", err)
	}
	if string(buf[:n]) != string(want[(2<<20)+37:])[:n] {
		t.Error("forward jump mismatch")
	}
	if zs3.Lseek(0, 4) != uint64(len(want)) {
		t.Error("SEEK_HOLE not dense-EOF")
	}
	if zs3.Lseek(10, 3) != 10 {
		t.Error("SEEK_DATA not identity")
	}
}

func TestZipConcurrentReads(t *testing.T) {
	data := buildTestZip(t, map[string]int{"big.bin": 3 << 20})
	ov := testOverlay(t, data)
	idx := ov.byName["big.bin"]
	ent := &ov.entries[idx]

	ref := make([]byte, ent.uncompSize)
	zr := &zipFileState{ov: ov, ent: ent, uncomp: ent.uncompSize}
	if _, err := zr.ReadAt(context.Background(), ref, 0); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("reference read: %v", err)
	}

	zs := &zipFileState{ov: ov, ent: ent, uncomp: ent.uncompSize}
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			prng := rand.New(rand.NewSource(seed))
			for range 200 {
				off := prng.Int63n(ent.uncompSize)
				n := 1 + prng.Intn(64*1024)
				if off+int64(n) > ent.uncompSize {
					n = int(ent.uncompSize - off)
				}
				buf := make([]byte, n)
				m, err := zs.ReadAt(context.Background(), buf, off)
				if err != nil && !errors.Is(err, io.EOF) {
					t.Errorf("read@%d: %v", off, err)
					return
				}
				if !bytes.Equal(buf[:m], ref[off:off+int64(m)]) {
					t.Errorf("read@%d: data mismatch", off)
					return
				}
			}
		}(int64(g) + 1)
	}
	wg.Wait()
}

func TestZipRootAnchor(t *testing.T) {
	data := buildTestZip(t, map[string]int{"alpha.txt": 10, "sub/beta.txt": 20})
	ov := testOverlay(t, data)
	ov.zipPath = "/test.zip"
	fs := testFS(ov)

	if _, err, virt := fs.zipAttr("/alpha.txt"); !virt || err != nil {
		t.Fatalf("root child attr: virt=%v err=%v", virt, err)
	}
	if _, err, virt := fs.zipAttr("/sub/beta.txt"); !virt || err != nil {
		t.Fatalf("root nested attr: virt=%v err=%v", virt, err)
	}
	if _, ok := fs.zipOpen(context.Background(), "/alpha.txt"); !ok {
		t.Error("root child not openable")
	}

	seen := map[string]int{}
	for _, vc := range fs.zipCollectChildren("/") {
		seen[vc.child.name]++
	}
	if seen["alpha.txt"] != 1 || seen["sub"] != 1 {
		t.Errorf("root children = %v", seen)
	}
	if n := len(fs.zipCollectChildren("/sub")); n != 1 {
		t.Errorf("nested children = %d", n)
	}
}

func TestZipXattrVirtualPath(t *testing.T) {
	data := buildTestZip(t, map[string]int{"alpha.txt": 10})
	fs := testFS(testOverlay(t, data))
	fs.Backup = coredb.Backup{IncludeXattr: true}

	fi, err := fs.ListXattr(context.Background(), "/data/alpha.txt")
	if err != nil {
		t.Fatalf("virtual xattr: %v", err)
	}
	if fi.Name != "alpha.txt" {
		t.Errorf("fi.Name = %q", fi.Name)
	}

	if _, err := fs.ListXattr(context.Background(), "/data/test.zip"); !errors.Is(err, syscall.ENOENT) {
		t.Errorf("hidden archive xattr: err = %v", err)
	}
}

func TestZipMergeStream(t *testing.T) {
	data := buildTestZip(t, map[string]int{
		"alpha.txt": 10,
		"sub/beta":  20,
		"gamma.txt": 30,
	})
	ov := testOverlay(t, data)
	fs := testFS(ov)

	s := &zipMergeStream{fs: fs, path: "/data"}
	got := map[string]bool{}
	for s.HasNext() {
		e, errno := s.Next()
		if errno != 0 {
			t.Fatalf("Next: %d", errno)
		}
		got[e.Name] = true
	}
	s.Close()
	for _, want := range []string{"alpha.txt", "sub", "gamma.txt"} {
		if !got[want] {
			t.Errorf("missing %s in merged listing: %v", want, got)
		}
	}
	if fs.FileCount.Value() != 2 || fs.FolderCount.Value() != 1 {
		t.Errorf("counters: files=%d dirs=%d", fs.FileCount.Value(), fs.FolderCount.Value())
	}
}

type rawEntry struct {
	name   string
	method uint16
	uncomp uint32
	comp   uint32
}

func buildRawZip(entries []rawEntry) []byte {
	var buf bytes.Buffer
	for range entries {
		buf.Write([]byte("PK\x03\x04"))
		buf.Write(make([]byte, 26))
	}
	cdStart := buf.Len()
	for _, e := range entries {
		var h [46]byte
		binary.LittleEndian.PutUint32(h[0:], 0x02014b50)
		binary.LittleEndian.PutUint16(h[10:], e.method)
		binary.LittleEndian.PutUint32(h[20:], e.comp)
		binary.LittleEndian.PutUint32(h[24:], e.uncomp)
		binary.LittleEndian.PutUint16(h[28:], uint16(len(e.name)))
		buf.Write(h[:])
		buf.WriteString(e.name)
	}
	cdSize := buf.Len() - cdStart
	var eocd [22]byte
	binary.LittleEndian.PutUint32(eocd[0:], 0x06054b50)
	binary.LittleEndian.PutUint16(eocd[8:], uint16(len(entries)))
	binary.LittleEndian.PutUint16(eocd[10:], uint16(len(entries)))
	binary.LittleEndian.PutUint32(eocd[12:], uint32(cdSize))
	binary.LittleEndian.PutUint32(eocd[16:], uint32(cdStart))
	buf.Write(eocd[:])
	return buf.Bytes()
}

func TestZipGatesAndNames(t *testing.T) {
	bomb := buildRawZip([]rawEntry{{name: "huge.bin", method: 0, uncomp: 300 << 20, comp: 4}})
	if _, err := parseZipOverlay(readAtBytes(bomb), int64(len(bomb)), zipMaxEntries); !errors.Is(err, errZipBomb) {
		t.Errorf("bomb: err=%v", err)
	}

	big := buildRawZip([]rawEntry{{name: "a", method: 0, uncomp: 1, comp: 1}})
	eocdOff := len(big) - 22
	binary.LittleEndian.PutUint16(big[eocdOff+10:], zipMaxEntries+1)
	if _, err := parseZipOverlay(readAtBytes(big), int64(len(big)), zipMaxEntries); !errors.Is(err, errZipTooMany) {
		t.Errorf("too many: err=%v", err)
	}

	var manyBuf bytes.Buffer
	mzw := zip.NewWriter(&manyBuf)
	for i := 0; i <= zipMaxEntries; i++ {
		w, err := mzw.CreateHeader(&zip.FileHeader{Name: fmt.Sprintf("f%06d", i), Method: zip.Store})
		if err != nil {
			t.Fatal(err)
		}
		w.Write([]byte("x"))
	}
	if err := mzw.Close(); err != nil {
		t.Fatal(err)
	}
	many := manyBuf.Bytes()
	if _, err := parseZipOverlay(readAtBytes(many), int64(len(many)), zipMaxEntries); !errors.Is(err, errZipTooMany) {
		t.Errorf("real many: err=%v", err)
	}
	if _, err := parseZipOverlay(readAtBytes(many), int64(len(many)), zipMaxEntries+1); err != nil {
		t.Errorf("entry limit override rejected: %v", err)
	}

	trav := buildRawZip([]rawEntry{
		{name: "../../evil", method: 0, uncomp: 1, comp: 1},
		{name: "ok.txt", method: 0, uncomp: 1, comp: 1},
	})
	ov, err := parseZipOverlay(readAtBytes(trav), int64(len(trav)), zipMaxEntries)
	if err != nil {
		t.Fatalf("traversal parse: %v", err)
	}
	if _, ok := ov.byName["../../evil"]; ok {
		t.Error("traversal name exposed")
	}
	if _, ok := ov.byName["ok.txt"]; !ok {
		t.Error("valid sibling dropped")
	}

	enc := buildRawZip([]rawEntry{{name: "x", method: 8, uncomp: 1, comp: 1}})
	binary.LittleEndian.PutUint16(enc[30+8:], 1)
	if _, err := parseZipOverlay(readAtBytes(enc), int64(len(enc)), zipMaxEntries); !errors.Is(err, errZipUnsupported) {
		t.Errorf("encrypted: err=%v", err)
	}
}

func TestZipRejectsCRCMismatch(t *testing.T) {
	for _, name := range []string{"store.txt", "deflate.txt"} {
		t.Run(name, func(t *testing.T) {
			data := buildTestZip(t, map[string]int{name: 4096})
			central := bytes.LastIndex(data, []byte("PK\x01\x02"))
			if central < 0 {
				t.Fatal("central directory not found")
			}
			data[central+16] ^= 0xff

			ov := testOverlay(t, data)
			idx := ov.byName[name]
			entry := &ov.entries[idx]
			zs := &zipFileState{ov: ov, ent: entry, uncomp: entry.uncompSize}
			buf := make([]byte, entry.uncompSize)
			if _, err := zs.ReadAt(context.Background(), buf, 0); !errors.Is(err, errZipCorrupt) {
				t.Fatalf("ReadAt error = %v, want %v", err, errZipCorrupt)
			}
		})
	}
}

func TestZipSymlinkReadlink(t *testing.T) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	hdr := &zip.FileHeader{Name: "link", Method: zip.Store}
	hdr.SetMode(os.ModeSymlink | 0o777)
	w, err := zw.CreateHeader(hdr)
	if err != nil {
		t.Fatal(err)
	}
	const want = "dir/target.txt"
	if _, err := io.WriteString(w, want); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	ov := testOverlay(t, buf.Bytes())
	fs := testFS(ov)
	target, ok, err := fs.zipReadlink(context.Background(), "/data/link")
	if err != nil || !ok || string(target) != want {
		t.Fatalf("zipReadlink = %q, %v, %v", target, ok, err)
	}

	child := ov.dirs[""].children[0]
	stream := &zipMergeStream{
		fs:        fs,
		path:      "/data",
		agentDone: true,
		vqueue:    []zipVChild{{ov: ov, child: child}},
		emitted:   map[string]struct{}{},
	}
	if !stream.HasNext() {
		t.Fatal("symlink missing from merged stream")
	}
	entry, errno := stream.Next()
	if errno != 0 || entry.Mode&uint32(syscall.S_IFLNK) == 0 {
		t.Fatalf("symlink dir entry = %+v, errno %v", entry, errno)
	}
}

func TestZipMergeStreamRealEntryShadowsVirtual(t *testing.T) {
	ov := testOverlay(t, buildTestZip(t, map[string]int{"alpha.txt": 10}))
	fs := testFS(ov)
	stream := &zipMergeStream{
		fs:        fs,
		path:      "/data",
		agentDone: true,
		vqueue:    []zipVChild{{ov: ov, child: ov.dirs[""].children[0]}},
	}
	stream.markEmitted("alpha.txt")
	if stream.HasNext() {
		t.Fatal("virtual entry emitted despite matching real entry")
	}

	fs.zipMarkShadowed("/data/alpha.txt")
	if _, _, ok := fs.zipAttr("/data/alpha.txt"); ok {
		t.Fatal("shadowed virtual attr remained visible")
	}
	if _, ok := fs.zipOpen(context.Background(), "/data/alpha.txt"); ok {
		t.Fatal("shadowed virtual file remained openable")
	}
}

func contentOf(t *testing.T, data []byte, name string) []byte {
	t.Helper()
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range zr.File {
		if f.Name != name {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}
		defer rc.Close()
		b, err := io.ReadAll(rc)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	t.Fatalf("entry %s not found", name)
	return nil
}
