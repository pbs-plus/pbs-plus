package pxarmount

import (
	"errors"
	"io"
	"os"
	"testing"

	"github.com/go-git/go-billy/v5"
)

func TestNFSFilesystemReadOnly(t *testing.T) {
	h := newHarness(t)
	fs := NewNFSFilesystem(h.mfs, true)

	file, err := fs.Open("/lower_a.bin")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	want := lowerContent("lower_a.bin", lowerFileSize)
	if string(got) != string(want) {
		t.Fatal("read-only NFS adapter returned wrong content")
	}
	if _, err := fs.Create("/blocked"); !errors.Is(err, billy.ErrReadOnly) {
		t.Fatalf("Create error = %v, want %v", err, billy.ErrReadOnly)
	}
}

func TestNFSFilesystemReadWrite(t *testing.T) {
	h := newHarness(t)
	fs := NewNFSFilesystem(h.mfs, false)

	if err := fs.MkdirAll("/nfs/sub", 0o750); err != nil {
		t.Fatal(err)
	}
	file, err := fs.Create("/nfs/sub/data")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("snapshot over nfs")); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fs.Rename("/nfs/sub/data", "/nfs/sub/renamed"); err != nil {
		t.Fatal(err)
	}

	file, err = fs.OpenFile("/nfs/sub/renamed", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if string(got) != "snapshot over nfs" {
		t.Fatalf("content = %q", got)
	}
	if err := fs.Remove("/nfs/sub/renamed"); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Stat("/nfs/sub/renamed"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Stat error = %v, want %v", err, os.ErrNotExist)
	}
}
