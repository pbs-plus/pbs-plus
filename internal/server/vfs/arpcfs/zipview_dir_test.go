package arpcfs

import (
	"archive/zip"
	"bytes"
	"io"
	"testing"
)

func buildZipWithExplicitDirs(t *testing.T, files map[string]string, dirs ...string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, d := range dirs {
		if _, err := zw.CreateHeader(&zip.FileHeader{Name: d, Method: zip.Store}); err != nil {
			t.Fatal(err)
		}
	}
	for name, content := range files {
		w, err := zw.CreateHeader(&zip.FileHeader{Name: name, Method: zip.Deflate})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(w, content); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestZipChildlessVirtualDirReaddir(t *testing.T) {
	root := testOverlay(t, buildZipWithExplicitDirs(t,
		map[string]string{"readme.txt": "zip readme content"},
		"sub/", "empty/"))
	fs := testFS(root)

	if got := len(fs.zipCollectChildren("/data/sub")); got != 0 {
		t.Fatalf("children of /data/sub = %d, want 0", got)
	}
	if !fs.zipIsVirtualDir("/data/sub") {
		t.Fatal("zipIsVirtualDir(/data/sub) = false, want true")
	}
	if !fs.zipIsVirtualDir("/data/empty") {
		t.Fatal("zipIsVirtualDir(/data/empty) = false, want true")
	}
	if fs.zipIsVirtualDir("/data") {
		t.Fatal("zipIsVirtualDir(/data) = true, want false")
	}
	if _, err, virt := fs.zipAttr("/data/empty"); !virt || err != nil {
		t.Fatalf("zipAttr(/data/empty) = err=%v virt=%v; want virtual dir", err, virt)
	}

	if got := len(fs.zipCollectChildren("/data")); got != 3 {
		t.Fatalf("children of /data = %d, want 3", got)
	}
}
