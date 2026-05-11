//go:build linux

package pxar

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/accessor"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"
	"github.com/puzpuzpuz/xsync/v4"
)

// --- helpers for building test pxar archives ---

func dirMeta(mode uint64) *pxar.Metadata {
	ts := format.StatxTimestampFromDurationSinceEpoch(1430487000 * time.Second)
	return &pxar.Metadata{
		Stat: format.Stat{
			Mode:  format.ModeIFDIR | mode,
			Mtime: ts,
		},
	}
}

func fileMeta(mode uint64, uid, gid uint32, size uint64) *pxar.Metadata {
	ts := format.StatxTimestampFromDurationSinceEpoch(1430487000 * time.Second)
	return &pxar.Metadata{
		Stat: format.Stat{
			Mode:  format.ModeIFREG | mode,
			UID:   uid,
			GID:   gid,
			Mtime: ts,
		},
	}
}

func fileMetaXattr(mode uint64, uid, gid uint32, size uint64, xattrs ...[2]string) *pxar.Metadata {
	ts := format.StatxTimestampFromDurationSinceEpoch(1430487000 * time.Second)
	m := &pxar.Metadata{
		Stat: format.Stat{
			Mode:  format.ModeIFREG | mode,
			UID:   uid,
			GID:   gid,
			Mtime: ts,
		},
	}
	for _, xa := range xattrs {
		m.XAttrs = append(m.XAttrs, format.NewXAttr([]byte(xa[0]), []byte(xa[1])))
	}
	return m
}

func symlinkMeta(mode uint64, uid, gid uint32) *pxar.Metadata {
	ts := format.StatxTimestampFromDurationSinceEpoch(1430487000 * time.Second)
	return &pxar.Metadata{
		Stat: format.Stat{
			Mode:  format.ModeIFLNK | mode,
			UID:   uid,
			GID:   gid,
			Mtime: ts,
		},
	}
}

func makeTestArchive(tb testing.TB) *bytes.Reader {
	tb.Helper()
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMeta(0o755), nil)

	_, _ = enc.AddFile(fileMeta(0o644, 1000, 1000, 11), "hello.txt", []byte("hello world"))
	_ = enc.CreateDirectory("subdir", dirMeta(0o755))
	_, _ = enc.AddFile(fileMeta(0o644, 1000, 1000, 14), "nested.txt", []byte("nested content"))
	_ = enc.Finish()
	_ = enc.AddSymlink(symlinkMeta(0o777, 0, 0), "link", "hello.txt")
	enc.Close()

	return bytes.NewReader(buf.Bytes())
}

func makeTestArchiveManyFiles(tb testing.TB, n int) *bytes.Reader {
	tb.Helper()
	var buf bytes.Buffer
	enc := encoder.NewEncoder(&buf, nil, dirMeta(0o755), nil)

	for i := range n {
		name := fmt.Sprintf("file-%04d.txt", i)
		_, _ = enc.AddFile(fileMeta(0o644, 1000, 1000, 10), name, []byte("0123456789"))
	}
	enc.Close()

	return bytes.NewReader(buf.Bytes())
}

// --- benchmarks: entryToEntryInfo ---

func BenchmarkEntryToEntryInfo_File(b *testing.B) {
	e := &pxar.Entry{
		Kind:          pxar.KindFile,
		Path:          "testfile.txt",
		FileOffset:    1000,
		FileSize:      4096,
		ContentOffset: 2000,
		Metadata: pxar.Metadata{
			Stat: format.Stat{
				Mode:  format.ModeIFREG | 0o644,
				UID:   1000,
				GID:   1000,
				Mtime: format.StatxTimestamp{Secs: 1234567890},
			},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		info := entryToEntryInfo(e)
		_ = info
	}
}

func BenchmarkEntryToEntryInfo_FileWithXattrs(b *testing.B) {
	e := &pxar.Entry{
		Kind:          pxar.KindFile,
		Path:          "testfile.txt",
		FileOffset:    1000,
		FileSize:      4096,
		ContentOffset: 2000,
		Metadata: pxar.Metadata{
			Stat: format.Stat{
				Mode:  format.ModeIFREG | 0o644,
				UID:   1000,
				GID:   1000,
				Mtime: format.StatxTimestamp{Secs: 1234567890},
			},
			XAttrs: []format.XAttr{
				format.NewXAttr([]byte("user.key1"), []byte("val1")),
				format.NewXAttr([]byte("user.key2"), []byte("val2")),
				format.NewXAttr([]byte("user.key3"), []byte("val3")),
			},
			FCaps: []byte{0x01, 0x02, 0x03, 0x04},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		info := entryToEntryInfo(e)
		_ = info
	}
}

func BenchmarkEntryToEntryInfo_Dir(b *testing.B) {
	e := &pxar.Entry{
		Kind:          pxar.KindDirectory,
		Path:          "mydir",
		FileOffset:    5000,
		FileSize:      8192,
		ContentOffset: 5100,
		Metadata: pxar.Metadata{
			Stat: format.Stat{
				Mode:  format.ModeIFDIR | 0o755,
				UID:   0,
				GID:   0,
				Mtime: format.StatxTimestamp{Secs: 1234567890},
			},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		info := entryToEntryInfo(e)
		_ = info
	}
}

func BenchmarkEntryToEntryInfo_Symlink(b *testing.B) {
	e := &pxar.Entry{
		Kind:       pxar.KindSymlink,
		Path:       "mylink",
		FileOffset: 7000,
		FileSize:   0,
		LinkTarget: "/usr/bin/target",
		Metadata: pxar.Metadata{
			Stat: format.Stat{
				Mode:  format.ModeIFLNK | 0o777,
				UID:   0,
				GID:   0,
				Mtime: format.StatxTimestamp{Secs: 1234567890},
			},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		info := entryToEntryInfo(e)
		_ = info
	}
}

// --- benchmark: cacheEntry / getCachedEntry ---

func BenchmarkCacheEntryAndLookup(b *testing.B) {
	r := &PxarReader{
		entryCache:   make(map[uint64]*pxar.Entry),
		contentCache: make(map[uint64]*pxar.Entry),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		off := uint64(i % 1000)
		e := &pxar.Entry{
			Kind:          pxar.KindFile,
			FileOffset:    off,
			FileSize:      4096,
			ContentOffset: off + 10000,
		}
		r.cacheEntry(e)
		_ = r.getCachedEntry(off)
		_ = r.getCachedContentEntry(e.ContentOffset)
	}
}

// --- benchmark: xattr allocation hot path ---

func BenchmarkEntryToEntryInfo_XattrAlloc(b *testing.B) {
	// Benchmark with varying xattr counts
	for _, nx := range []int{0, 1, 3, 10} {
		b.Run(fmt.Sprintf("xattrs=%d", nx), func(b *testing.B) {
			var xas []format.XAttr
			for i := range nx {
				xas = append(xas, format.NewXAttr(
					fmt.Appendf(nil, "user.key%d", i),
					fmt.Appendf(nil, "val%d", i),
				))
			}
			e := &pxar.Entry{
				Kind:          pxar.KindFile,
				Path:          "test.txt",
				FileOffset:    100,
				FileSize:      4096,
				ContentOffset: 500,
				Metadata: pxar.Metadata{
					Stat: format.Stat{
						Mode: format.ModeIFREG | 0o644,
						UID:  1000, GID: 1000,
						Mtime: format.StatxTimestamp{Secs: 1234567890},
					},
					XAttrs: xas,
				},
			}

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_ = entryToEntryInfo(e)
			}
		})
	}
}

// --- unit tests ---

func TestEntryToEntryInfo_File(t *testing.T) {
	e := &pxar.Entry{
		Kind:          pxar.KindFile,
		Path:          "testfile.txt",
		FileOffset:    1000,
		FileSize:      4096,
		ContentOffset: 2000,
		Metadata: pxar.Metadata{
			Stat: format.Stat{
				Mode:  format.ModeIFREG | 0o644,
				UID:   1000,
				GID:   1000,
				Mtime: format.StatxTimestamp{Secs: 1234567890, Nanos: 500},
			},
		},
	}

	info := entryToEntryInfo(e)
	if string(info.FileName) != "testfile.txt" {
		t.Errorf("FileName = %s, want testfile.txt", info.FileName)
	}
	if info.FileType != FileTypeFile {
		t.Errorf("FileType = %d, want FileTypeFile", info.FileType)
	}
	if info.Size != 4096 {
		t.Errorf("Size = %d, want 4096", info.Size)
	}
	if info.Mode != (format.ModeIFREG | 0o644) {
		t.Errorf("Mode = %o, want %o", info.Mode, format.ModeIFREG|0o644)
	}
	if info.UID != 1000 || info.GID != 1000 {
		t.Errorf("UID=%d GID=%d, want 1000 1000", info.UID, info.GID)
	}
	if info.MtimeSecs != 1234567890 || info.MtimeNsecs != 500 {
		t.Errorf("Mtime = %d.%d, want 1234567890.500", info.MtimeSecs, info.MtimeNsecs)
	}
	if info.ContentRange == nil {
		t.Error("ContentRange should not be nil for regular file")
	} else if len(info.ContentRange) != 2 || info.ContentRange[0] != 2000 || info.ContentRange[1] != 2000+4096 {
		t.Errorf("ContentRange = %v, want [2000, 6096]", info.ContentRange)
	}
	if info.Xattrs != nil {
		t.Errorf("Xattrs should be nil for file without xattrs, got %v", info.Xattrs)
	}
}

func TestEntryToEntryInfo_NoXattrMeansNil(t *testing.T) {
	e := &pxar.Entry{
		Kind:          pxar.KindFile,
		Path:          "emptyx.txt",
		FileOffset:    100,
		FileSize:      100,
		ContentOffset: 200,
		Metadata: pxar.Metadata{
			Stat: format.Stat{
				Mode:  format.ModeIFREG | 0o644,
				Mtime: format.StatxTimestamp{Secs: 1},
			},
		},
	}
	info := entryToEntryInfo(e)
	if info.Xattrs != nil {
		t.Errorf("Xattrs should be nil when entry has no xattrs/fcaps, got %v", info.Xattrs)
	}
}

func TestEntryToEntryInfo_WithXattrs(t *testing.T) {
	e := &pxar.Entry{
		Kind:          pxar.KindFile,
		Path:          "xattr.txt",
		FileOffset:    100,
		FileSize:      100,
		ContentOffset: 200,
		Metadata: pxar.Metadata{
			Stat: format.Stat{
				Mode:  format.ModeIFREG | 0o644,
				Mtime: format.StatxTimestamp{Secs: 1},
			},
			XAttrs: []format.XAttr{
				format.NewXAttr([]byte("user.a"), []byte("1")),
				format.NewXAttr([]byte("user.b"), []byte("2")),
			},
			FCaps: []byte{0xde, 0xad},
		},
	}
	info := entryToEntryInfo(e)
	if info.Xattrs == nil {
		t.Fatal("Xattrs should not be nil")
	}
	if string(info.Xattrs["user.a"]) != "1" {
		t.Errorf("Xattrs[user.a] = %q, want %q", info.Xattrs["user.a"], "1")
	}
	if string(info.Xattrs["user.b"]) != "2" {
		t.Errorf("Xattrs[user.b] = %q, want %q", info.Xattrs["user.b"], "2")
	}
	if string(info.Xattrs["security.capability"]) != string([]byte{0xde, 0xad}) {
		t.Errorf("Xattrs[security.capability] = %v, want dead", info.Xattrs["security.capability"])
	}
}

func TestEntryToEntryInfo_Dir(t *testing.T) {
	e := &pxar.Entry{
		Kind:          pxar.KindDirectory,
		Path:          "mydir",
		FileOffset:    500,
		FileSize:      4096,
		ContentOffset: 600,
		Metadata: pxar.Metadata{
			Stat: format.Stat{
				Mode:  format.ModeIFDIR | 0o755,
				UID:   0,
				GID:   0,
				Mtime: format.StatxTimestamp{Secs: 999},
			},
		},
	}
	info := entryToEntryInfo(e)
	if info.FileType != FileTypeDirectory {
		t.Errorf("FileType = %d, want FileTypeDirectory", info.FileType)
	}
	if info.ContentRange != nil {
		t.Errorf("ContentRange should be nil for directory, got %v", info.ContentRange)
	}
}

func TestEntryToEntryInfo_Symlink(t *testing.T) {
	e := &pxar.Entry{
		Kind:       pxar.KindSymlink,
		Path:       "mylink",
		FileOffset: 1,
		FileSize:   0,
		LinkTarget: "/etc/hosts",
		Metadata: pxar.Metadata{
			Stat: format.Stat{
				Mode:  format.ModeIFLNK | 0o777,
				Mtime: format.StatxTimestamp{Secs: 1},
			},
		},
	}
	info := entryToEntryInfo(e)
	if info.FileType != FileTypeSymlink {
		t.Errorf("FileType = %d, want FileTypeSymlink", info.FileType)
	}
	if info.LinkTarget != "/etc/hosts" {
		t.Errorf("LinkTarget = %q, want /etc/hosts", info.LinkTarget)
	}
}

func TestEntryToEntryInfo_AllKinds(t *testing.T) {
	tests := []struct {
		kind pxar.EntryKind
		ft   FileType
	}{
		{pxar.KindFile, FileTypeFile},
		{pxar.KindDirectory, FileTypeDirectory},
		{pxar.KindSymlink, FileTypeSymlink},
		{pxar.KindHardlink, FileTypeHardlink},
		{pxar.KindDevice, FileTypeDevice},
		{pxar.KindFifo, FileTypeFifo},
		{pxar.KindSocket, FileTypeSocket},
	}

	for _, tc := range tests {
		e := &pxar.Entry{
			Kind:       tc.kind,
			Path:       "entry",
			FileOffset: 1,
			FileSize:   0,
			Metadata: pxar.Metadata{
				Stat: format.Stat{
					Mode:  format.ModeIFREG | 0o644,
					Mtime: format.StatxTimestamp{Secs: 1},
				},
			},
		}
		info := entryToEntryInfo(e)
		if info.FileType != tc.ft {
			t.Errorf("Kind=%v: FileType=%d, want %d", tc.kind, info.FileType, tc.ft)
		}
	}
}

// --- cache mutex tests ---

func TestCacheEntryConcurrent(t *testing.T) {
	r := &PxarReader{
		entryCache:   make(map[uint64]*pxar.Entry),
		contentCache: make(map[uint64]*pxar.Entry),
	}

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(off uint64) {
			defer wg.Done()
			e := &pxar.Entry{
				Kind:          pxar.KindFile,
				FileOffset:    off,
				FileSize:      4096,
				ContentOffset: off + 1000,
			}
			r.cacheEntry(e)
			got := r.getCachedEntry(off)
			if got == nil {
				t.Errorf("getCachedEntry(%d) returned nil after cacheEntry", off)
			}
			if e.IsRegularFile() {
				got = r.getCachedContentEntry(e.ContentOffset)
				if got == nil {
					t.Errorf("getCachedContentEntry(%d) returned nil", e.ContentOffset)
				}
			}
		}(uint64(i))
	}
	wg.Wait()
}

func TestCacheEntryOverwrite(t *testing.T) {
	r := &PxarReader{
		entryCache:   make(map[uint64]*pxar.Entry),
		contentCache: make(map[uint64]*pxar.Entry),
	}

	f1 := &pxar.Entry{
		Kind:          pxar.KindFile,
		FileOffset:    42,
		FileSize:      100,
		ContentOffset: 500,
	}
	f2 := &pxar.Entry{
		Kind:          pxar.KindFile,
		FileOffset:    42,
		FileSize:      200,
		ContentOffset: 999,
	}

	r.cacheEntry(f1)
	r.cacheEntry(f2)

	got := r.getCachedEntry(42)
	if got.FileSize != 200 {
		t.Errorf("expected overwritten entry size 200, got %d", got.FileSize)
	}
	got = r.getCachedContentEntry(999)
	if got == nil {
		t.Error("new content cache entry not found")
	}
}

// --- stats tests ---

func TestPxarReaderStats(t *testing.T) {
	fc := xsync.NewCounter()
	foc := xsync.NewCounter()
	tb := xsync.NewCounter()
	fc.Add(10)
	foc.Add(3)
	tb.Add(4096)
	r := &PxarReader{
		FileCount:   fc,
		FolderCount: foc,
		TotalBytes:  tb,
	}
	stats := r.GetStats()
	if stats.FilesAccessed != 10 {
		t.Errorf("FilesAccessed = %d, want 10", stats.FilesAccessed)
	}
	if stats.FoldersAccessed != 3 {
		t.Errorf("FoldersAccessed = %d, want 3", stats.FoldersAccessed)
	}
	if stats.TotalAccessed != 13 {
		t.Errorf("TotalAccessed = %d, want 13", stats.TotalAccessed)
	}
	if stats.TotalBytes != 4096 {
		t.Errorf("TotalBytes = %d, want 4096", stats.TotalBytes)
	}
}

// TestReaderConcurrentAccess demonstrates the race condition in the
// shared metadata reader. Multiple goroutines calling
// ListDirectory concurrently on the same FileArchiveReader will race
// on the internal offset of the backing bytes.Reader. This test
// triggers the race by spawning many goroutines that repeatedly list
// the root directory, checking for errors or wrong entry counts.
func TestReaderConcurrentAccess(t *testing.T) {
	buf := makeTestArchiveManyFiles(t, 100)
	// bytes.Reader is used as backing — it has the same non-thread-safe
	// Seek+Read pattern as ChunkedReadSeeker.
	ar := transfer.NewFileArchiveReader(buf)
	defer ar.Close()

	// Get the root to find the directory content offset
	root, err := ar.ReadRoot()
	if err != nil {
		t.Fatal(err)
	}
	rootContentOffset := int64(root.ContentOffset)

	var errCount atomic.Int64
	var wrongCount atomic.Int64
	var wg sync.WaitGroup

	const numGoroutines = 20
	const iterations = 50

	for range numGoroutines {
		wg.Go(func() {
			for range iterations {
				count := 0
				err := ar.ListDirectory(rootContentOffset, accessor.ListOption{Minimal: true}, func(e *pxar.Entry) error {
					count++
					return nil
				})
				if err != nil {
					errCount.Add(1)
					continue
				}
				if count != 100 {
					wrongCount.Add(1)
				}
			}
		})
	}

	wg.Wait()

	t.Logf("errors: %d, wrong counts: %d", errCount.Load(), wrongCount.Load())
	if errCount.Load() > 0 || wrongCount.Load() > 0 {
		t.Errorf("concurrent access produced %d errors, %d wrong counts (both should be 0)",
			errCount.Load(), wrongCount.Load())
	}
}

var _ context.Context = context.Background()
