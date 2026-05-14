package pxarmount

import (
	"bufio"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// --- Benchmark 1: Hash-based verification ---

func BenchmarkVerifySHA256(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "testfile")

	f, err := os.Create(path)
	if err != nil {
		b.Fatal(err)
	}
	if err := f.Truncate(64 * 1024 * 1024); err != nil {
		b.Fatal(err)
	}
	f.Close()

	b.ResetTimer()
	b.SetBytes(64 * 1024 * 1024)
	for range b.N {
		f, err := os.Open(path)
		if err != nil {
			b.Fatal(err)
		}
		h := sha256.New()
		if _, err := io.Copy(h, f); err != nil {
			b.Fatal(err)
		}
		h.Sum(nil)
		f.Close()
	}
}

func BenchmarkVerifySHA256_64KBBuffer(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "testfile")

	f, err := os.Create(path)
	if err != nil {
		b.Fatal(err)
	}
	if err := f.Truncate(64 * 1024 * 1024); err != nil {
		b.Fatal(err)
	}
	f.Close()

	buf := make([]byte, 64*1024)

	b.ResetTimer()
	b.SetBytes(64 * 1024 * 1024)
	for range b.N {
		f, err := os.Open(path)
		if err != nil {
			b.Fatal(err)
		}
		h := sha256.New()
		io.CopyBuffer(h, f, buf)
		h.Sum(nil)
		f.Close()
	}
}

// --- Benchmark 2: Transaction log batching ---

func BenchmarkTxnLog_Batched(b *testing.B) {
	dir := b.TempDir()
	tl, err := OpenTransactionLog(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = tl.Close() }()

	b.ResetTimer()
	for range b.N {
		if _, err := tl.Record(TxnDelete, "/some/test/path/file.txt"); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	_ = tl.Sync()
}

func BenchmarkTxnLog_BatchedParallel(b *testing.B) {
	dir := b.TempDir()
	tl, err := OpenTransactionLog(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = tl.Close() }()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := tl.Record(TxnModify, "/parallel/file.txt"); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.StopTimer()
	_ = tl.Sync()
}

// BenchmarkTxnLog_PerRecordFlush simulates the old per-record flush behavior.
func BenchmarkTxnLog_PerRecordFlush(b *testing.B) {
	dir := b.TempDir()
	logPath := filepath.Join(dir, "test.log")

	b.ResetTimer()
	for range b.N {
		f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			b.Fatal(err)
		}
		buf := bufio.NewWriterSize(f, 64*1024)
		buf.WriteString(`{"type":"DELETE","path":"/some/test/path/file.txt"}` + "\n")
		buf.Flush()
		f.Sync()
		f.Close()
	}
}

// --- Benchmark 3: mmap vs ReadFile for DIDX-sized payloads ---

func BenchmarkMmap_1MB(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "didx")
	writeTestFile(b, path, 1*1024*1024)

	b.ResetTimer()
	b.SetBytes(1 * 1024 * 1024)
	for range b.N {
		data, err := mmapFile(path)
		if err != nil {
			b.Fatal(err)
		}
		_ = munmap(data)
	}
}

func BenchmarkReadFile_1MB(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "didx")
	writeTestFile(b, path, 1*1024*1024)

	b.ResetTimer()
	b.SetBytes(1 * 1024 * 1024)
	for range b.N {
		_, err := os.ReadFile(path)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMmap_64MB(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "didx")
	writeTestFile(b, path, 64*1024*1024)

	b.ResetTimer()
	b.SetBytes(64 * 1024 * 1024)
	for range b.N {
		data, err := mmapFile(path)
		if err != nil {
			b.Fatal(err)
		}
		_ = munmap(data)
	}
}

func BenchmarkReadFile_64MB(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "didx")
	writeTestFile(b, path, 64*1024*1024)

	b.ResetTimer()
	b.SetBytes(64 * 1024 * 1024)
	for range b.N {
		_, err := os.ReadFile(path)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// --- Benchmark 4: Materialization copy buffer ---

func BenchmarkCopyBuffer_1MB(b *testing.B) {
	dir := b.TempDir()
	srcPath := filepath.Join(dir, "source")
	dstPath := filepath.Join(dir, "dest")
	writeTestFile(b, srcPath, 64*1024*1024)

	b.ResetTimer()
	b.SetBytes(64 * 1024 * 1024)
	for range b.N {
		src, err := os.Open(srcPath)
		if err != nil {
			b.Fatal(err)
		}
		dst, err := os.Create(dstPath)
		if err != nil {
			src.Close()
			b.Fatal(err)
		}
		bufp := copyBufPool.Get().(*[]byte)
		_, err = io.CopyBuffer(dst, src, *bufp)
		copyBufPool.Put(bufp)
		dst.Close()
		src.Close()
		if err != nil {
			b.Fatal(err)
		}
		_ = os.Truncate(dstPath, 0)
	}
}

func BenchmarkCopyDefault_32KB(b *testing.B) {
	dir := b.TempDir()
	srcPath := filepath.Join(dir, "source")
	dstPath := filepath.Join(dir, "dest")
	writeTestFile(b, srcPath, 64*1024*1024)

	b.ResetTimer()
	b.SetBytes(64 * 1024 * 1024)
	for range b.N {
		src, err := os.Open(srcPath)
		if err != nil {
			b.Fatal(err)
		}
		dst, err := os.Create(dstPath)
		if err != nil {
			src.Close()
			b.Fatal(err)
		}
		_, err = io.Copy(dst, src)
		dst.Close()
		src.Close()
		if err != nil {
			b.Fatal(err)
		}
		_ = os.Truncate(dstPath, 0)
	}
}

// --- Benchmark 5: Pre-walk backing dir ---

func BenchmarkPreWalkBackingDir_10KFiles(b *testing.B) {
	dir := b.TempDir()
	for i := range 1000 {
		d := filepath.Join(dir, fmt.Sprintf("dir%04d", i))
		if err := os.MkdirAll(d, 0o755); err != nil {
			b.Fatal(err)
		}
		for j := range 10 {
			if err := os.WriteFile(filepath.Join(d, fmt.Sprintf("file%02d.txt", j)), []byte("hello"), 0o644); err != nil {
				b.Fatal(err)
			}
		}
	}

	fs := &PassthroughFS{backingDir: dir}

	b.ResetTimer()
	for range b.N {
		fs.preWalkBackingDir()
	}
}

func BenchmarkReadDir_PerDirectory_10KFiles(b *testing.B) {
	dir := b.TempDir()
	for i := range 1000 {
		d := filepath.Join(dir, fmt.Sprintf("dir%04d", i))
		if err := os.MkdirAll(d, 0o755); err != nil {
			b.Fatal(err)
		}
		for j := range 10 {
			if err := os.WriteFile(filepath.Join(d, fmt.Sprintf("file%02d.txt", j)), []byte("hello"), 0o644); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ResetTimer()
	for range b.N {
		des, _ := os.ReadDir(dir)
		for _, de := range des {
			if de.IsDir() {
				os.ReadDir(filepath.Join(dir, de.Name()))
			}
		}
	}
}

// --- Benchmark 6: Copy buffer pool overhead ---

func BenchmarkCopyBufPool_GetPut(b *testing.B) {
	for range b.N {
		bufp := copyBufPool.Get().(*[]byte)
		copyBufPool.Put(bufp)
	}
}

func BenchmarkCopyBufPool_Alloc(b *testing.B) {
	for range b.N {
		_ = make([]byte, 1024*1024)
	}
}

// --- Helpers ---

func writeTestFile(tb testing.TB, path string, size int) {
	tb.Helper()
	f, err := os.Create(path)
	if err != nil {
		tb.Fatal(err)
	}
	if err := f.Truncate(int64(size)); err != nil {
		tb.Fatal(err)
	}
	f.Close()
}
