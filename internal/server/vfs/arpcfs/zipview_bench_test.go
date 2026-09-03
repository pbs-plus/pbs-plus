//go:build linux

package arpcfs

import "testing"

// Lookup hits zipAttr for every path in the mount, archives present or not.
func BenchmarkZipAttrNoArchives(b *testing.B) {
	fs := testFS()
	b.ReportAllocs()
	for b.Loop() {
		fs.zipAttr("/data/a/b/c/d/e/f/g/file.txt")
	}
}

// Baseline: the pre-flag path, an RLock plus a per-component anchor probe.
func BenchmarkZipAttrNoArchivesLocked(b *testing.B) {
	fs := testFS()
	fs.zipActive.Store(true)
	b.ReportAllocs()
	for b.Loop() {
		fs.zipAttr("/data/a/b/c/d/e/f/g/file.txt")
	}
}

func BenchmarkZipAttrNoArchivesParallel(b *testing.B) {
	fs := testFS()
	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			fs.zipAttr("/data/a/b/c/d/e/f/g/file.txt")
		}
	})
}

func BenchmarkZipAttrNoArchivesLockedParallel(b *testing.B) {
	fs := testFS()
	fs.zipActive.Store(true)
	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			fs.zipAttr("/data/a/b/c/d/e/f/g/file.txt")
		}
	})
}

func BenchmarkZipAttrMiss(b *testing.B) {
	fs := testFS(testOverlay(b, buildZipFiles(b, map[string][]byte{"x.txt": []byte("x")})))
	b.ReportAllocs()
	for b.Loop() {
		fs.zipAttr("/data/a/b/c/d/e/f/g/file.txt")
	}
}

func BenchmarkZipAttrHit(b *testing.B) {
	fs := testFS(testOverlay(b, buildZipFiles(b, map[string][]byte{"dir/x.txt": []byte("x")})))
	b.ReportAllocs()
	for b.Loop() {
		if _, _, ok := fs.zipAttr("/data/dir/x.txt"); !ok {
			b.Fatal("miss")
		}
	}
}
