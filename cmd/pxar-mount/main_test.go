package main

import (
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/pxarmount"
	"github.com/pbs-plus/pxar"
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
	// statMode is unexported — tested internally via pxarmount package tests.
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
