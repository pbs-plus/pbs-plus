//go:build linux

package pxar

import (
	"fmt"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/format"
)

// --- helpers for building test pxar archives ---

// --- benchmarks: pxar.EntryToFileInfo ---

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
		info := pxar.EntryToFileInfo(e)
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
		info := pxar.EntryToFileInfo(e)
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
		info := pxar.EntryToFileInfo(e)
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
		info := pxar.EntryToFileInfo(e)
		_ = info
	}
}

// --- benchmark: cache lookup (delegated to library) ---
// Removed: cache tests now in pxar/vfs/offset_test.go

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
				_ = pxar.EntryToFileInfo(e)
			}
		})
	}
}

// --- unit tests ---

func TestEntryToFileInfo_File(t *testing.T) {
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

	info := pxar.EntryToFileInfo(e)
	if string(info.FileName) != "testfile.txt" {
		t.Errorf("FileName = %s, want testfile.txt", info.FileName)
	}
	if info.FileType != pxar.FileTypeFile {
		t.Errorf("pxar.FileType = %d, want pxar.FileTypeFile", info.FileType)
	}
	if info.RawSize != 4096 {
		t.Errorf("Size = %d, want 4096", info.RawSize)
	}
	if info.RawMode != (format.ModeIFREG | 0o644) {
		t.Errorf("Mode = %o, want %o", info.RawMode, format.ModeIFREG|0o644)
	}
	if info.RawUID != 1000 || info.RawGID != 1000 {
		t.Errorf("UID=%d GID=%d, want 1000 1000", info.RawUID, info.RawGID)
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

func TestEntryToFileInfo_NoXattrMeansNil(t *testing.T) {
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
	info := pxar.EntryToFileInfo(e)
	if info.Xattrs != nil {
		t.Errorf("Xattrs should be nil when entry has no xattrs/fcaps, got %v", info.Xattrs)
	}
}

func TestEntryToFileInfo_WithXattrs(t *testing.T) {
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
	info := pxar.EntryToFileInfo(e)
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

func TestEntryToFileInfo_Dir(t *testing.T) {
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
	info := pxar.EntryToFileInfo(e)
	if info.FileType != pxar.FileTypeDirectory {
		t.Errorf("pxar.FileType = %d, want pxar.FileTypeDirectory", info.FileType)
	}
	if info.ContentRange != nil {
		t.Errorf("ContentRange should be nil for directory, got %v", info.ContentRange)
	}
}

func TestEntryToFileInfo_Symlink(t *testing.T) {
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
	info := pxar.EntryToFileInfo(e)
	if info.FileType != pxar.FileTypeSymlink {
		t.Errorf("pxar.FileType = %d, want pxar.FileTypeSymlink", info.FileType)
	}
	if info.LinkTarget != "/etc/hosts" {
		t.Errorf("LinkTarget = %q, want /etc/hosts", info.LinkTarget)
	}
}

func TestEntryToFileInfo_AllKinds(t *testing.T) {
	tests := []struct {
		kind pxar.EntryKind
		ft   pxar.FileType
	}{
		{pxar.KindFile, pxar.FileTypeFile},
		{pxar.KindDirectory, pxar.FileTypeDirectory},
		{pxar.KindSymlink, pxar.FileTypeSymlink},
		{pxar.KindHardlink, pxar.FileTypeHardlink},
		{pxar.KindDevice, pxar.FileTypeDevice},
		{pxar.KindFIFO, pxar.FileTypeFifo},
		{pxar.KindSocket, pxar.FileTypeSocket},
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
		info := pxar.EntryToFileInfo(e)
		if info.FileType != tc.ft {
			t.Errorf("Kind=%v: pxar.FileType=%d, want %d", tc.kind, info.FileType, tc.ft)
		}
	}
}

// --- cache tests removed: now covered by pxar/vfs/offset_test.go ---

// --- stats tests ---

func TestPxarReaderStats(t *testing.T) {
	// Stats now delegate to LocalOffsetFS
	// Tested in pxar/vfs/offset_test.go
}

// TestReaderConcurrentAccess removed: now covered by pxar/vfs/offset_test.go
