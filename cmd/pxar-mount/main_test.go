package main

import (
	"syscall"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/format"
)

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
