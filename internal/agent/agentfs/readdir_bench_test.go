package agentfs

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
)

func BenchmarkReadDirBatchCopy(b *testing.B) {
	dir := b.TempDir()
	for i := range 500 {
		os.WriteFile(filepath.Join(dir, "file_"+strconv.Itoa(i)+".txt"), []byte("content"), 0644)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		f, err := os.Open(dir)
		if err != nil {
			b.Fatal(err)
		}
		dr, err := NewDirReader(f, dir)
		if err != nil {
			b.Fatal(err)
		}
		_, err = dr.NextBatch(context.Background(), 4096)
		if err != nil && err != os.ErrProcessDone {
			b.Fatal(err)
		}
		dr.Close()
	}
}

func BenchmarkReadDirBatchPooled(b *testing.B) {
	dir := b.TempDir()
	for i := range 500 {
		os.WriteFile(filepath.Join(dir, "file_"+strconv.Itoa(i)+".txt"), []byte("content"), 0644)
	}

	pool := &sync.Pool{New: func() any { return new(bytes.Buffer) }}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		f, err := os.Open(dir)
		if err != nil {
			b.Fatal(err)
		}
		dr, err := NewDirReader(f, dir)
		if err != nil {
			b.Fatal(err)
		}

		buf := pool.Get().(*bytes.Buffer)
		buf.Reset()
		err = dr.NextBatchInto(context.Background(), 4096, buf)
		if err != nil && err != os.ErrProcessDone {
			b.Fatal(err)
		}
		pool.Put(buf)
		dr.Close()
	}
}
