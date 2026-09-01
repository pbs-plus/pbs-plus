package agentfs

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
)

func BenchmarkNextBatch(b *testing.B) {
	for _, count := range []int{0, 16, 1024} {
		b.Run(fmt.Sprintf("entries=%d", count), func(b *testing.B) {
			dir := b.TempDir()
			for i := range count {
				if err := os.WriteFile(filepath.Join(dir, strconv.Itoa(i)), nil, 0o644); err != nil {
					b.Fatal(err)
				}
			}

			b.ReportAllocs()
			for b.Loop() {
				f, err := os.Open(dir)
				if err != nil {
					b.Fatal(err)
				}
				r, err := NewDirReader(f, dir)
				if err != nil {
					b.Fatal(err)
				}
				for {
					_, err = r.NextBatch(b.Context(), 4096)
					if errors.Is(err, os.ErrProcessDone) {
						break
					}
					if err != nil {
						b.Fatal(err)
					}
				}
				if err := r.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkEncodeDirEntries(b *testing.B) {
	for _, attrs := range []bool{false, true} {
		name := "without_attributes"
		if attrs {
			name = "with_attributes"
		}
		b.Run(name, func(b *testing.B) {
			entries := make([]fswire.AgentFileInfo, 1024)
			for i := range entries {
				entries[i] = fswire.AgentFileInfo{
					Name:           "file-" + strconv.Itoa(i),
					Size:           4096,
					Mode:           0o644,
					ModTime:        1_700_000_000_000_000_000,
					CreationTime:   1_700_000_000_000_000_000,
					LastAccessTime: 1_700_000_000_000_000_000,
					LastWriteTime:  1_700_000_000_000_000_000,
				}
				if attrs {
					entries[i].FileAttributes = map[string]bool{"archive": true}
				}
			}

			r := DirReader{encodeWriter: &bytes.Buffer{}}
			b.ReportAllocs()
			for b.Loop() {
				r.encodeWriter.Reset()
				enc := cbor.NewEncoder(r.encodeWriter)
				if err := enc.StartIndefiniteArray(); err != nil {
					b.Fatal(err)
				}
				for _, info := range entries {
					ok, err := r.tryEncode(enc, info)
					if err != nil {
						b.Fatal(err)
					}
					if !ok {
						b.Fatal("entry batch exceeded encoding limit")
					}
				}
				if err := enc.EndIndefinite(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
