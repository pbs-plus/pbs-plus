package main

import (
	"bytes"
	"syscall"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"
)

// Test helpers

func mustTestArchive(tb testing.TB) (*bytes.Reader, int64) {
	tb.Helper()
	ts := format.StatxTimestampFromDurationSinceEpoch(1430487000 * 1e9)
	var buf bytes.Buffer
	rootMeta := &pxar.Metadata{
		Stat: format.Stat{
			Mode: format.ModeIFDIR | 0o755,
			UID:  0, GID: 0,
			Mtime: ts,
		},
	}
	enc := encoder.NewEncoder(&buf, nil, rootMeta, nil)

	fileMeta := &pxar.Metadata{
		Stat: format.Stat{
			Mode: format.ModeIFREG | 0o644,
			UID:  1000, GID: 1000,
			Mtime: ts,
		},
	}
	_, _ = enc.AddFile(fileMeta, "hello.txt", []byte("hello world"))

	dirMeta := &pxar.Metadata{
		Stat: format.Stat{
			Mode: format.ModeIFDIR | 0o755,
			UID:  0, GID: 0,
			Mtime: ts,
		},
	}
	_ = enc.CreateDirectory("subdir", dirMeta)
	_, _ = enc.AddFile(fileMeta, "nested.txt", []byte("nested content"))
	_ = enc.Finish()

	symMeta := &pxar.Metadata{
		Stat: format.Stat{
			Mode:  format.ModeIFLNK | 0o777,
			Mtime: ts,
		},
	}
	_ = enc.AddSymlink(symMeta, "link", "hello.txt")

	enc.Close()
	data := buf.Bytes()
	return bytes.NewReader(data), int64(len(data))
}

func newTestFS(tb testing.TB) (*pxarFS, *bytes.Reader) {
	tb.Helper()
	r, size := mustTestArchive(tb)
	far := transfer.NewFileArchiveReader(r)
	_ = far // use raw reader for now

	// Build a simple mock using the accessor directly
	fs := &pxarFS{
		reader: nil,
		nodes:  make(map[uint64]*node),
		size:   size,
	}

	root, err := far.ReadRoot()
	if err != nil {
		tb.Fatal(err)
	}

	fs.nodes[rootInode] = nodeFromEntry(root, rootInode, rootInode)

	return fs, r
}

func TestToInode(t *testing.T) {
	df := &pxar.Entry{
		Kind:       pxar.KindDirectory,
		FileOffset: 100,
		FileSize:   4096,
	}
	dfIno := toInode(df)
	if dfIno != 100+4096 {
		t.Errorf("dir inode = %d, want %d", dfIno, 100+4096)
	}
	if !isDirInode(dfIno) {
		t.Error("dir inode should beDirInode")
	}

	f := &pxar.Entry{
		Kind:       pxar.KindFile,
		FileOffset: 200,
		FileSize:   1024,
	}
	fIno := toInode(f)
	if fIno != 200|nonDirBit {
		t.Errorf("file inode = %d, want %d", fIno, 200|nonDirBit)
	}
	if isDirInode(fIno) {
		t.Error("file inode should not beDirInode")
	}
}

func TestStatMode(t *testing.T) {
	tests := []struct {
		mode uint64
		want uint32
	}{
		{format.ModeIFREG | 0o644, syscall.S_IFREG | 0o644},
		{format.ModeIFDIR | 0o755, syscall.S_IFDIR | 0o755},
		{format.ModeIFLNK | 0o777, syscall.S_IFLNK | 0o777},
		{format.ModeIFBLK | 0o600, syscall.S_IFBLK | 0o600},
		{format.ModeIFCHR | 0o600, syscall.S_IFCHR | 0o600},
		{format.ModeIFIFO | 0o644, syscall.S_IFIFO | 0o644},
		{format.ModeIFSOCK | 0o755, syscall.S_IFSOCK | 0o755},
	}
	for _, tc := range tests {
		got := statMode(tc.mode)
		if got != tc.want {
			t.Errorf("statMode(%o) = %o, want %o", tc.mode, got, tc.want)
		}
	}
}

func TestIsDirInode(t *testing.T) {
	if !isDirInode(rootInode) {
		t.Error("root inode should be a directory")
	}
	if !isDirInode(42) {
		t.Error("42 (no NonDirBit) should be a directory")
	}
	if isDirInode(42 | nonDirBit) {
		t.Error("42|NonDirBit should not be a directory")
	}
}

func TestBytesEq(t *testing.T) {
	if !bytesEq([]byte("hello"), "hello") {
		t.Error("bytesEq should match")
	}
	if bytesEq([]byte("hello"), "world") {
		t.Error("bytesEq should not match")
	}
	if bytesEq([]byte("hell"), "hello") {
		t.Error("bytesEq different lengths should not match")
	}
}

func TestXattrValue(t *testing.T) {
	val := []byte("test-value")

	// nil dest returns size
	n, st := xattrValue(val, nil)
	if st != 0 || n != 10 {
		t.Errorf("nil dest: got (%d, %v), want (10, OK)", n, st)
	}

	// short dest returns ERANGE
	_, st = xattrValue(val, make([]byte, 5))
	if st == 0 {
		t.Error("short dest should return ERANGE")
	}

	// sufficient dest copies
	dest := make([]byte, 20)
	n, st = xattrValue(val, dest)
	if st != 0 || n != 10 {
		t.Errorf("copy: got (%d, %v), want (10, OK)", n, st)
	}
	if string(dest[:n]) != "test-value" {
		t.Errorf("copied = %q, want %q", dest[:n], "test-value")
	}
}
