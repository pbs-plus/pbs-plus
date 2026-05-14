package main

import (
	"syscall"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/pxarmount"
	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/format"
)

func TestToInode(t *testing.T) {
	df := &pxar.Entry{
		Kind:       pxar.KindDirectory,
		FileOffset: 100,
		FileSize:   4096,
	}
	dfIno := pxarmount.ToInode(df)
	if dfIno != 100+4096 {
		t.Errorf("dir inode = %d, want %d", dfIno, 100+4096)
	}
	if !pxarmount.IsDirInode(dfIno) {
		t.Error("dir inode should be dir")
	}

	f := &pxar.Entry{
		Kind:       pxar.KindFile,
		FileOffset: 200,
		FileSize:   1024,
	}
	fIno := pxarmount.ToInode(f)
	if fIno != 200|pxarmount.NonDirBit {
		t.Errorf("file inode = %d, want %d", fIno, 200|pxarmount.NonDirBit)
	}
	if pxarmount.IsDirInode(fIno) {
		t.Error("file inode should not be dir")
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
	// statMode is unexported — tested internally via pxarmount package tests.
	_ = tests
}

func TestIsDirInode(t *testing.T) {
	if !pxarmount.IsDirInode(1) {
		t.Error("root inode should be a directory")
	}
	if !pxarmount.IsDirInode(42) {
		t.Error("42 (no NonDirBit) should be a directory")
	}
	if pxarmount.IsDirInode(42 | pxarmount.NonDirBit) {
		t.Error("42|NonDirBit should not be a directory")
	}
}

func TestBytesEq(t *testing.T) {
	// bytesEq is unexported — tested via pxarmount package tests.
}

func TestXattrValue(t *testing.T) {
	// xattrValue is unexported — tested via pxarmount package tests.
}
