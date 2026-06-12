package pxarmount

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
)

func benchJournal(b *testing.B) (*Journal, func()) {
	b.Helper()
	dir, err := os.MkdirTemp("", "pxar-bench-journal-*")
	if err != nil {
		b.Fatal(err)
	}
	journalDir := filepath.Join(dir, "journal")
	j, err := OpenJournal(journalDir)
	if err != nil {
		_ = os.RemoveAll(dir)
		b.Fatal(err)
	}
	return j, func() {
		_ = j.Close()
		_ = os.RemoveAll(dir)
	}
}

func benchMutableFS(b *testing.B) (*MutableFS, func()) {
	b.Helper()
	dir, err := os.MkdirTemp("", "pxar-bench-mfs-*")
	if err != nil {
		b.Fatal(err)
	}
	pxarFS, err := NewPxarFS(nil)
	if err != nil {
		_ = os.RemoveAll(dir)
		b.Fatal(err)
	}
	journalDir := filepath.Join(dir, "journal")
	journal, err := OpenJournal(journalDir)
	if err != nil {
		_ = os.RemoveAll(dir)
		b.Fatal(err)
	}
	mfs := NewMutableFS(pxarFS, journal, dir)
	if err := mfs.InitMutableRoot(); err != nil {
		_ = journal.Close()
		_ = os.RemoveAll(dir)
		b.Fatal(err)
	}
	return mfs, func() {
		_ = mfs.journal.Sync()
		mfs.Close()
		_ = os.RemoveAll(dir)
	}
}

func BenchmarkJournal_EnsureNodePath(b *testing.B) {
	for _, parallelism := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("parallel=%d", parallelism), func(b *testing.B) {
			j, cleanup := benchJournal(b)
			defer cleanup()

			b.ResetTimer()
			b.ReportAllocs()
			b.SetParallelism(parallelism)
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					path := fmt.Sprintf("/bench/%d/file_%d.txt", i%100, i)
					node := &GraphNode{Kind: NodeFile, Mode: 0o644}
					if _, err := j.EnsureNodePath(path, node, false); err != nil {
						b.Fatal(err)
					}
					i++
				}
			})
		})
	}
}

func BenchmarkJournal_UpdateNode(b *testing.B) {
	for _, parallelism := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("parallel=%d", parallelism), func(b *testing.B) {
			j, cleanup := benchJournal(b)
			defer cleanup()

			node := &GraphNode{Kind: NodeFile, Mode: 0o644}
			id, _ := j.EnsureNodePath("/bench_update.txt", node, false)
			node.ID = id

			var counter atomic.Uint64

			b.ResetTimer()
			b.ReportAllocs()
			b.SetParallelism(parallelism)
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					n := &GraphNode{
						ID:      id,
						Kind:    NodeFile,
						Mode:    0o644,
						Size:    counter.Add(1),
						MtimeNs: time.Now().UnixNano(),
					}
					if err := j.UpdateNode(n); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkJournal_ResolvePath(b *testing.B) {
	for _, depth := range []int{1, 3, 6, 10} {
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			j, cleanup := benchJournal(b)
			defer cleanup()

			var path strings.Builder
			for d := range depth {
				fmt.Fprintf(&path, "/level_%02d", d)
			}
			path.WriteString("/leaf.txt")
			testPath := path.String()

			node := &GraphNode{Kind: NodeFile, Mode: 0o644}
			if _, err := j.EnsureNodePath(testPath, node, false); err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					nodeID, _, _, _, err := j.ResolvePath(testPath)
					if err != nil {
						b.Fatal(err)
					}
					if nodeID == 0 {
						b.Fatal("nodeID = 0")
					}
				}
			})
		})
	}
}

func BenchmarkJournal_SetXAttr(b *testing.B) {
	for _, parallelism := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("parallel=%d", parallelism), func(b *testing.B) {
			j, cleanup := benchJournal(b)
			defer cleanup()

			node := &GraphNode{Kind: NodeFile, Mode: 0o644}
			id, _ := j.EnsureNodePath("/bench_xattr.txt", node, false)

			b.ResetTimer()
			b.ReportAllocs()
			b.SetParallelism(parallelism)
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					name := fmt.Sprintf("user.attr_%d", i%1000)
					val := fmt.Appendf(nil, "value_%d", i)
					if err := j.SetXAttr(id, name, val); err != nil {
						b.Fatal(err)
					}
					i++
				}
			})
		})
	}
}

func BenchmarkJournal_ConcurrentReadWrite(b *testing.B) {
	j, cleanup := benchJournal(b)
	defer cleanup()

	node := &GraphNode{Kind: NodeFile, Mode: 0o644}
	_, _ = j.EnsureNodePath("/rw_test.txt", node, false)

	for _, readRatio := range []int{90, 50, 10} {
		b.Run(fmt.Sprintf("read%%=%d", readRatio), func(b *testing.B) {
			var reads atomic.Int64
			var writes atomic.Int64

			b.ResetTimer()
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					if i%100 < readRatio {
						_, _, _, _, err := j.ResolvePath("/rw_test.txt")
						if err != nil {
							b.Fatal(err)
						}
						reads.Add(1)
					} else {
						n := &GraphNode{ID: 2, Kind: NodeFile, Mode: 0o644, Size: uint64(i)}
						if err := j.UpdateNode(n); err != nil {
							b.Fatal(err)
						}
						writes.Add(1)
					}
					i++
				}
			})
			b.ReportMetric(float64(reads.Load()), "reads")
			b.ReportMetric(float64(writes.Load()), "writes")
		})
	}
}

func BenchmarkFreeze_ContendedWait(b *testing.B) {
	mfs, cleanup := benchMutableFS(b)
	defer cleanup()

	for _, concurrentOps := range []int{8, 32, 128} {
		b.Run(fmt.Sprintf("waiters=%d", concurrentOps), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for range b.N {
				mfs.freezeMu.Lock()
				mfs.frozen = true
				mfs.freezeMu.Unlock()

				var wg sync.WaitGroup
				wg.Add(concurrentOps)

				for range concurrentOps {
					go func() {
						defer wg.Done()
						mfs.waitIfFrozen()
					}()
				}

				mfs.freezeMu.Lock()
				mfs.frozen = false
				mfs.freezeMu.Unlock()
				mfs.freezeCond.Broadcast()

				wg.Wait()
			}
		})
	}
}

func BenchmarkMutableFS_WritePath(b *testing.B) {
	for _, parallelism := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("parallel=%d", parallelism), func(b *testing.B) {
			mfs, cleanup := benchMutableFS(b)
			defer cleanup()

			mfs.mapInode(RootInode, "/")

			numFiles := parallelism * 10
			for i := range numFiles {
				childPath := fmt.Sprintf("/write_%d.txt", i)
				ino := mfs.pathToIno(childPath, false)
				mfs.mapInode(ino, childPath)

				abs := mfs.mutablePath(childPath)
				if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
					b.Fatal(err)
				}
				fd, err := syscall.Open(abs, syscall.O_CREAT|syscall.O_WRONLY|syscall.O_TRUNC, 0o644)
				if err != nil {
					b.Fatal(err)
				}
				_ = syscall.Close(fd)

				now := time.Now().UnixNano()
				journalNode := &GraphNode{
					Kind:    NodeFile,
					Mode:    uint32(0o644),
					Size:    0,
					MtimeNs: now,
					CtimeNs: now,
					HasData: true,
				}
				parentID := mfs.resolveParentNodeID("/")
				nodeID, err := mfs.journal.CreateNodeEdgeAndWhiteout(parentID, fmt.Sprintf("write_%d.txt", i), journalNode, false)
				if err != nil {
					b.Fatal(err)
				}
				journalNode.ID = nodeID

				fd2, err := syscall.Open(abs, syscall.O_RDWR, 0)
				if err != nil {
					b.Fatal(err)
				}
				mfs.registerFh(childPath, fd2)
			}

			b.ResetTimer()
			b.ReportAllocs()
			b.SetParallelism(parallelism)
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					fileIdx := i % numFiles
					ino := mfs.pathToIno(fmt.Sprintf("/write_%d.txt", fileIdx), false)
					data := fmt.Appendf(nil, "data_%d", i)
					input := &fuse.WriteIn{
						InHeader: fuse.InHeader{NodeId: ino},
						Offset:   uint64(i) * 64,
						Size:     uint32(len(data)),
					}
					_, status := mfs.Write(nil, input, data)
					if status != fuse.OK {
						b.Fatalf("Write: %s", status)
					}
					i++
				}
			})
		})
	}
}

func BenchmarkMutableFS_FlushPattern(b *testing.B) {
	mfs, cleanup := benchMutableFS(b)
	defer cleanup()

	mfs.mapInode(RootInode, "/")

	const files = 1000
	inodes := make([]uint64, files)

	for i := range files {
		childPath := fmt.Sprintf("/flush_%04d.txt", i)
		ino := mfs.pathToIno(childPath, false)
		mfs.mapInode(ino, childPath)
		inodes[i] = ino

		abs := mfs.mutablePath(childPath)
		if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
			b.Fatal(err)
		}
		fd, err := syscall.Open(abs, syscall.O_CREAT|syscall.O_WRONLY|syscall.O_TRUNC, 0o644)
		if err != nil {
			b.Fatal(err)
		}
		_ = syscall.Close(fd)

		now := time.Now().UnixNano()
		journalNode := &GraphNode{
			Kind:    NodeFile,
			Mode:    uint32(0o644),
			Size:    0,
			MtimeNs: now,
			CtimeNs: now,
			HasData: true,
		}
		parentID := mfs.resolveParentNodeID("/")
		nodeID, err := mfs.journal.CreateNodeEdgeAndWhiteout(parentID, fmt.Sprintf("flush_%04d.txt", i), journalNode, false)
		if err != nil {
			b.Fatal(err)
		}
		journalNode.ID = nodeID
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		for i := range files {
			mfs.dirtyMeta.Store(inodes[i], pendingMeta{
				size:    uint64(i + 100),
				mtimeNs: time.Now().UnixNano(),
				ctimeNs: time.Now().UnixNano(),
			})
		}

		for i := range files {
			inoMu := mfs.getInoLock(inodes[i])
			inoMu.Lock()
			if meta, ok := mfs.dirtyMeta.LoadAndDelete(inodes[i]); ok {
				path := fmt.Sprintf("/flush_%04d.txt", i)
				if re, status := mfs.resolve(path); status == fuse.OK && re.Node != nil {
					if meta.size > re.Node.Size {
						re.Node.Size = meta.size
					}
					re.Node.MtimeNs = meta.mtimeNs
					re.Node.CtimeNs = meta.ctimeNs
					_ = mfs.journal.UpdateNode(re.Node)
				}
			}
			inoMu.Unlock()
		}
	}
}

func BenchmarkMutableFS_CopyUpContention(b *testing.B) {
	mfs, cleanup := benchMutableFS(b)
	defer cleanup()

	mfs.mapInode(RootInode, "/")

	for _, concurrency := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("goroutines=%d", concurrency), func(b *testing.B) {
			var seq atomic.Uint64

			b.ResetTimer()
			b.ReportAllocs()

			for range b.N {
				n := seq.Add(1)
				childPath := fmt.Sprintf("/copyup_%d.txt", n)
				ino := mfs.pathToIno(childPath, false)
				mfs.mapInode(ino, childPath)

				abs := mfs.mutablePath(childPath)
				_ = os.MkdirAll(filepath.Dir(abs), 0o755)

				fd, err := syscall.Open(abs, syscall.O_CREAT|syscall.O_WRONLY|syscall.O_TRUNC, 0o644)
				if err != nil {
					b.Fatal(err)
				}
				_ = syscall.Close(fd)

				now := time.Now().UnixNano()
				journalNode := &GraphNode{
					Kind:    NodeFile,
					Mode:    uint32(0o644),
					Size:    5,
					MtimeNs: now,
					CtimeNs: now,
					HasData: true,
				}
				parentID := mfs.resolveParentNodeID("/")
				nodeID, err := mfs.journal.CreateNodeEdgeAndWhiteout(parentID, fmt.Sprintf("copyup_%d.txt", n), journalNode, false)
				if err != nil {
					b.Fatal(err)
				}
				journalNode.ID = nodeID

				re := &ResolvedEntry{
					Path:      childPath,
					Inode:     ino,
					Node:      journalNode,
					IsDir:     false,
					Mode:      0o644,
					Size:      5,
					DataIsMut: false,
				}

				var wg sync.WaitGroup
				wg.Add(concurrency)
				for range concurrency {
					go func() {
						defer wg.Done()
						_ = mfs.copyUp(re)
					}()
				}
				wg.Wait()

				mfs.unmapInode(childPath)
				_ = os.Remove(abs)
			}
		})
	}
}
