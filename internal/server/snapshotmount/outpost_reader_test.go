//go:build linux

package snapshotmount

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pbs-plus/pxar/datastore"
)

func TestOpenSplitReaderArchiveFormats(t *testing.T) {
	for _, split := range []bool{false, true} {
		name := "legacy"
		if split {
			name = "split"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			dir := groupParentDir(root, "Media/D-100", "host", "scanner")
			stamp := "2026-01-01T00:00:00Z"
			dir = filepath.Join(dir, stamp)
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			index := datastore.NewDynamicIndexWriter(0)
			raw, err := index.Finish()
			if err != nil {
				t.Fatal(err)
			}
			file := "data.pxar.didx"
			files := []string{file}
			if split {
				file = "data.mpxar.didx"
				files = []string{file, "data.ppxar.didx"}
			}
			for _, file := range files {
				if err := os.WriteFile(filepath.Join(dir, file), raw, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			reader, err := openSplitReader(root, file, stamp, "Media/D-100", "host", "scanner")
			if err != nil {
				t.Fatal(err)
			}
			if (reader.PayloadIndex() != nil) != split {
				t.Error("payload reader does not match archive format")
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
