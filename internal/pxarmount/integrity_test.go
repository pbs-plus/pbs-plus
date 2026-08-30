package pxarmount

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
)

// TestOverlayLayeringCases covers the hand-picked shapes where the sparse
// overlay has historically served lower-layer bytes over written ranges.
func TestOverlayLayeringCases(t *testing.T) {
	cases := []struct {
		name string
		run  func(h *harness)
	}{
		{"write head of lower file", func(h *harness) {
			h.write("/lower_a.bin", 0, fillPattern(0x11, 100))
		}},
		{"write tail of lower file", func(h *harness) {
			h.write("/lower_a.bin", lowerFileSize-100, fillPattern(0x22, 100))
		}},
		{"write past lower EOF grows file", func(h *harness) {
			h.write("/lower_a.bin", lowerFileSize+5000, fillPattern(0x33, 200))
		}},
		{"two writes leaving a hole between them", func(h *harness) {
			h.write("/lower_a.bin", 1000, fillPattern(0x44, 500))
			h.write("/lower_a.bin", 20000, fillPattern(0x55, 500))
		}},
		{"adjacent writes coalesce", func(h *harness) {
			h.write("/lower_a.bin", 4096, fillPattern(0x66, 4096))
			h.write("/lower_a.bin", 8192, fillPattern(0x77, 4096))
		}},
		{"overlapping rewrite", func(h *harness) {
			h.write("/lower_a.bin", 1000, fillPattern(0x88, 2000))
			h.write("/lower_a.bin", 1500, fillPattern(0x99, 2000))
		}},
		{"backwards writes", func(h *harness) {
			h.write("/lower_a.bin", 30000, fillPattern(0xAA, 100))
			h.write("/lower_a.bin", 100, fillPattern(0xBB, 100))
		}},
		{"shrink then read", func(h *harness) {
			h.write("/lower_a.bin", 5000, fillPattern(0xCC, 100))
			h.truncate("/lower_a.bin", 3000)
		}},
		{"shrink below written extent then grow back", func(h *harness) {
			h.write("/lower_a.bin", 5000, fillPattern(0xDD, 100))
			h.truncate("/lower_a.bin", 1000)
			h.truncate("/lower_a.bin", lowerFileSize)
		}},
		{"truncate to zero discards lower entirely", func(h *harness) {
			h.truncate("/lower_a.bin", 0)
		}},
		{"nested lower file", func(h *harness) {
			h.write("/sub/lower_c.bin", 700, fillPattern(0xEE, 9000))
		}},
		{"new file untouched by lower layer", func(h *harness) {
			h.create("/fresh.bin")
			h.write("/fresh.bin", 0, fillPattern(0x01, 5000))
			h.write("/fresh.bin", 10000, fillPattern(0x02, 5000))
		}},
		{"fsync between writes", func(h *harness) {
			h.write("/lower_b.bin", 200, fillPattern(0x03, 300))
			h.fsync("/lower_b.bin")
			h.write("/lower_b.bin", 9000, fillPattern(0x04, 300))
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			tc.run(h)
			h.verify()
			h.remountClean()
			h.verify()
		})
	}
}

// TestNamespaceCases asserts that every created path is reachable by lookup
// and listed by readdir, and that removed paths disappear from both.
func TestNamespaceCases(t *testing.T) {
	cases := []struct {
		name string
		run  func(h *harness)
	}{
		{"create files at root", func(h *harness) {
			h.create("/n1.bin")
			h.create("/n2.bin")
		}},
		{"mkdir and populate", func(h *harness) {
			h.mkdir("/d1")
			h.create("/d1/inner.bin")
			h.write("/d1/inner.bin", 0, fillPattern(0x10, 900))
		}},
		{"nested mkdir chain", func(h *harness) {
			h.mkdir("/a")
			h.mkdir("/a/b")
			h.mkdir("/a/b/c")
			h.create("/a/b/c/deep.bin")
		}},
		{"create inside a lower-layer directory", func(h *harness) {
			h.create("/sub/added.bin")
			h.write("/sub/added.bin", 0, fillPattern(0x20, 400))
		}},
		{"unlink a lower-layer file", func(h *harness) {
			h.unlink("/lower_a.bin")
		}},
		{"unlink a created file", func(h *harness) {
			h.create("/temp.bin")
			h.unlink("/temp.bin")
		}},
		{"rmdir a created directory", func(h *harness) {
			h.mkdir("/gone")
			h.rmdir("/gone")
		}},
		{"rename within a directory", func(h *harness) {
			h.create("/src.bin")
			h.write("/src.bin", 0, fillPattern(0x30, 700))
			h.rename("/src.bin", "/dst.bin")
		}},
		{"rename across directories", func(h *harness) {
			h.mkdir("/dir_x")
			h.create("/moving.bin")
			h.write("/moving.bin", 0, fillPattern(0x40, 700))
			h.rename("/moving.bin", "/dir_x/moved.bin")
		}},
		{"rename a copied-up lower file", func(h *harness) {
			h.write("/lower_b.bin", 50, fillPattern(0x50, 4000))
			h.rename("/lower_b.bin", "/renamed_lower.bin")
		}},
		{"unlink then recreate the same name", func(h *harness) {
			h.unlink("/lower_a.bin")
			h.create("/lower_a.bin")
			h.write("/lower_a.bin", 0, fillPattern(0x60, 128))
		}},
		{"many entries force readdir paging", func(h *harness) {
			for i := range 40 {
				h.create("/" + newFileName(i))
			}
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			tc.run(h)
			h.verify()
			h.remountClean()
			h.verify()
		})
	}
}

// TestReaddirPagingBufferSizes runs the same directory through buffer sizes
// small enough to force multi-call resume, where a mis-ordered merge shows up
// as a duplicated or dropped entry.
func TestReaddirPagingBufferSizes(t *testing.T) {
	for _, bufSize := range []int{64, 96, 128, 256, 1024, 65536} {
		t.Run(fmt.Sprint(bufSize), func(t *testing.T) {
			h := newHarness(t)
			h.mkdir("/many")
			for i := range 25 {
				h.create("/many/" + newFileName(i))
			}
			for i := range 5 {
				h.mkdir("/many/" + newDirName(i))
			}
			h.verifyNamespace(bufSize)
		})
	}
}

// randomOps applies a deterministic pseudo-random operation stream. The same
// seed must always produce the same sequence so a failure is replayable.
func randomOps(h *harness, seed int64, count int) {
	rng := rand.New(rand.NewSource(seed))
	created := 0
	dirs := 0

	for range count {
		files := h.filePaths()
		allDirs := h.dirPaths()

		switch rng.Intn(10) {
		case 0, 1, 2, 3:
			if len(files) == 0 {
				continue
			}
			path := files[rng.Intn(len(files))]
			off := rng.Intn(lowerFileSize + 8000)
			size := 1 + rng.Intn(6000)
			h.write(path, off, fillPattern(byte(rng.Intn(256)), size))
		case 4:
			if len(files) == 0 {
				continue
			}
			path := files[rng.Intn(len(files))]
			h.truncate(path, rng.Intn(lowerFileSize+4000))
		case 5:
			parent := allDirs[rng.Intn(len(allDirs))]
			h.create(filepath.Join(parent, newFileName(created)))
			created++
		case 6:
			parent := allDirs[rng.Intn(len(allDirs))]
			h.mkdir(filepath.Join(parent, newDirName(dirs)))
			dirs++
		case 7:
			if len(files) == 0 {
				continue
			}
			h.unlink(files[rng.Intn(len(files))])
		case 8:
			if len(files) == 0 {
				continue
			}
			src := files[rng.Intn(len(files))]
			parent := allDirs[rng.Intn(len(allDirs))]
			dst := filepath.Join(parent, newFileName(created))
			created++
			if dst == src {
				continue
			}
			h.rename(src, dst)
		case 9:
			if len(files) == 0 {
				continue
			}
			h.fsync(files[rng.Intn(len(files))])
		}
		h.verify()
	}
}

func TestOverlayModelRandomized(t *testing.T) {
	for seed := range int64(25) {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			h := newHarness(t)
			randomOps(h, seed, 60)
			h.verify()
			h.remountClean()
			h.verify()
		})
	}
}

func FuzzOverlayModel(f *testing.F) {
	for seed := range int64(5) {
		f.Add(seed, 40)
	}
	f.Fuzz(func(t *testing.T, seed int64, count int) {
		if count < 1 || count > 200 {
			t.Skip()
		}
		h := newHarness(t)
		randomOps(h, seed, count)
		h.verify()
	})
}

// TestCrashConsistency kills the mount after each operation and asserts the
// remount serves exactly what was written. Backing-file bytes reach the OS
// before the journal does, so a lost journal batch must be repaired by
// reconcile rather than shadowing writes with backup content.
func TestCrashConsistency(t *testing.T) {
	type crashOp func(*harness)
	scripts := []struct {
		name string
		ops  []crashOp
	}{
		{"write to lower file", []crashOp{
			func(h *harness) { h.write("/lower_a.bin", 3000, fillPattern(0x71, 2000)) },
		}},
		{"write leaving holes", []crashOp{
			func(h *harness) { h.write("/lower_a.bin", 0, fillPattern(0x72, 100)) },
			func(h *harness) { h.write("/lower_a.bin", 25000, fillPattern(0x73, 100)) },
		}},
		{"grow beyond lower size", []crashOp{
			func(h *harness) { h.write("/lower_b.bin", lowerFileSize+1000, fillPattern(0x74, 3000)) },
		}},
		{"create and write a new file", []crashOp{
			func(h *harness) { h.create("/crash_new.bin") },
			func(h *harness) { h.write("/crash_new.bin", 0, fillPattern(0x75, 9000)) },
		}},
		{"mkdir then create inside", []crashOp{
			func(h *harness) { h.mkdir("/crash_dir") },
			func(h *harness) { h.create("/crash_dir/inner.bin") },
			func(h *harness) { h.write("/crash_dir/inner.bin", 0, fillPattern(0x76, 500)) },
		}},
		{"nested mkdir chain", []crashOp{
			func(h *harness) { h.mkdir("/cd1") },
			func(h *harness) { h.mkdir("/cd1/cd2") },
			func(h *harness) { h.create("/cd1/cd2/leaf.bin") },
			func(h *harness) { h.write("/cd1/cd2/leaf.bin", 0, fillPattern(0x77, 300)) },
		}},
		{"truncate a lower file", []crashOp{
			func(h *harness) { h.write("/lower_a.bin", 100, fillPattern(0x78, 400)) },
			func(h *harness) { h.truncate("/lower_a.bin", 2000) },
		}},
		{"fsync then further writes", []crashOp{
			func(h *harness) { h.write("/lower_a.bin", 100, fillPattern(0x79, 400)) },
			func(h *harness) { h.fsync("/lower_a.bin") },
			func(h *harness) { h.write("/lower_a.bin", 15000, fillPattern(0x7A, 400)) },
		}},
	}

	for _, sc := range scripts {
		t.Run(sc.name, func(t *testing.T) {
			for cut := 1; cut <= len(sc.ops); cut++ {
				t.Run(fmt.Sprintf("after_%d", cut), func(t *testing.T) {
					h := newHarness(t)
					for _, op := range sc.ops[:cut] {
						op(h)
					}
					h.remountAfterCrash()
					h.verify()
				})
			}
		})
	}
}

func TestCrashConsistencyRandomized(t *testing.T) {
	for seed := range int64(15) {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			h := newHarness(t)
			randomOps(h, seed, 30)
			h.remountAfterCrash()
			h.verify()
		})
	}
}
