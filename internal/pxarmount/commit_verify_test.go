package pxarmount

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/zeebo/xxh3"
)

func TestVerifyBackedFileHashesConcurrent(t *testing.T) {
	dir := t.TempDir()
	mfs := NewMutableFS(nil, nil, dir)
	hashes := make(map[string]uint64, 64)

	for i := range 64 {
		name := fmt.Sprintf("file-%02d", i)
		data := bytes.Repeat([]byte{byte(i)}, 64*1024)
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
		hashes[name] = xxh3.Hash(data)
	}

	prog := NewProgressReporter(io.Discard)
	if err := verifyBackedFileHashes(mfs, hashes, prog); err != nil {
		t.Fatal(err)
	}

	hashes["missing"] = 0
	if err := verifyBackedFileHashes(mfs, hashes, prog); err == nil {
		t.Fatal("missing file passed verification")
	}
}
