package pxarmount

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
)

func testMutableFS(t *testing.T) (*MutableFS, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "pxar-bottleneck-test-*")
	if err != nil {
		t.Fatal(err)
	}
	pxarFS, err := NewPxarFS(nil)
	if err != nil {
		_ = os.RemoveAll(dir)
		t.Fatal(err)
	}
	journalDir := filepath.Join(dir, "journal")
	journal, err := OpenJournal(journalDir)
	if err != nil {
		_ = os.RemoveAll(dir)
		t.Fatal(err)
	}
	mfs := NewMutableFS(pxarFS, journal, dir)
	mfs.verbose = true
	if err := mfs.InitMutableRoot(); err != nil {
		_ = journal.Close()
		_ = os.RemoveAll(dir)
		t.Fatal(err)
	}
	return mfs, func() {
		mfs.Close()
		_ = os.RemoveAll(dir)
	}
}

func TestBottleneck_JournalMutexSerialization(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	const writers = 64
	const opsPerWriter = 100
	var wg sync.WaitGroup
	wg.Add(writers)

	var maxPending atomic.Int64
	start := time.Now()

	for w := range writers {
		go func(wID int) {
			defer wg.Done()
			for i := range opsPerWriter {
				path := fmt.Sprintf("/w%d/f%d.txt", wID, i)
				node := &GraphNode{Kind: NodeFile, Mode: 0o644}
				if _, err := j.EnsureNodePath(path, node, false); err != nil {
					t.Errorf("worker %d op %d: %v", wID, i, err)
					return
				}
				p := j.flushPending.Value()
				for {
					old := maxPending.Load()
					if p <= old || maxPending.CompareAndSwap(old, p) {
						break
					}
				}
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)
	totalOps := writers * opsPerWriter
	opsPerSec := float64(totalOps) / elapsed.Seconds()

	t.Logf("Journal mutex: %d ops in %v (%.0f ops/sec), maxPending=%d",
		totalOps, elapsed, opsPerSec, maxPending.Load())

	if opsPerSec < 500 {
		t.Errorf("Journal throughput too low: %.0f ops/sec (threshold: 500)", opsPerSec)
	}
}

func TestBottleneck_ConcurrentUpdateNode(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	node := &GraphNode{Kind: NodeFile, Mode: 0o644}
	id, err := j.EnsureNodePath("/concurrent_update.txt", node, false)
	if err != nil {
		t.Fatal(err)
	}
	node.ID = id

	const writers = 32
	const opsPerWriter = 200
	var wg sync.WaitGroup
	wg.Add(writers)

	start := time.Now()

	for w := range writers {
		go func(wID int) {
			defer wg.Done()
			for i := range opsPerWriter {
				n := &GraphNode{
					ID:      id,
					Kind:    NodeFile,
					Mode:    0o644,
					Size:    uint64(wID*opsPerWriter + i),
					MtimeNs: time.Now().UnixNano(),
				}
				if err := j.UpdateNode(n); err != nil {
					t.Errorf("worker %d op %d: %v", wID, i, err)
					return
				}
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)
	totalOps := writers * opsPerWriter
	opsPerSec := float64(totalOps) / elapsed.Seconds()

	t.Logf("UpdateNode: %d ops in %v (%.0f ops/sec)", totalOps, elapsed, opsPerSec)

	got, _ := j.GetNode(id)
	if got == nil {
		t.Fatal("node missing after concurrent updates")
	}

	if opsPerSec < 500 {
		t.Errorf("UpdateNode throughput too low: %.0f ops/sec (threshold: 500)", opsPerSec)
	}
}

func TestBottleneck_FreezerContention(t *testing.T) {
	mfs, cleanup := testMutableFS(t)
	defer cleanup()

	const rounds = 1000
	const concurrentOps = 32

	for range rounds {
		mfs.freezeMu.Lock()
		mfs.frozen = true
		mfs.freezeMu.Unlock()

		var wg sync.WaitGroup
		wg.Add(concurrentOps)

		var unblocked atomic.Int32

		for range concurrentOps {
			go func() {
				defer wg.Done()
				mfs.freezeMu.Lock()
				for mfs.frozen {
					mfs.freezeCond.Wait()
				}
				unblocked.Add(1)
				mfs.freezeMu.Unlock()
			}()
		}

		runtime := time.AfterFunc(5*time.Millisecond, func() {
			mfs.freezeMu.Lock()
			mfs.frozen = false
			mfs.freezeMu.Unlock()
			mfs.freezeCond.Broadcast()
		})

		wg.Wait()
		runtime.Stop()

		if v := unblocked.Load(); v != int32(concurrentOps) {
			t.Fatalf("only %d/%d ops unblocked", v, concurrentOps)
		}
	}
}

func TestBottleneck_EnsureLocksGrowth(t *testing.T) {
	mfs, cleanup := testMutableFS(t)
	defer cleanup()

	const iterations = 10000
	for i := range iterations {
		path := fmt.Sprintf("/path_%d", i)
		mfs.ensureLocks.LoadOrStore(path, &sync.Mutex{})
	}

	count := 0
	mfs.ensureLocks.Range(func(_ string, _ *sync.Mutex) bool {
		count++
		return true
	})

	t.Logf("ensureLocks after %d inserts: %d entries", iterations, count)

	if count != iterations {
		t.Errorf("ensureLocks has %d entries, expected %d", count, iterations)
	}

	for i := range iterations {
		path := fmt.Sprintf("/path_%d", i)
		mfs.ensureLocks.Delete(path)
	}

	count2 := 0
	mfs.ensureLocks.Range(func(_ string, _ *sync.Mutex) bool {
		count2++
		return true
	})

	if count2 != 0 {
		t.Errorf("ensureLocks after cleanup: %d entries, expected 0", count2)
	}
}

func TestBottleneck_FlushPerCloseJournalWrite(t *testing.T) {
	mfs, cleanup := testMutableFS(t)
	defer cleanup()

	const files = 500

	parentPath := "/"
	mfs.mapInode(RootInode, parentPath)

	for i := range files {
		childPath := fmt.Sprintf("/file_%04d.txt", i)
		ino := mfs.pathToIno(childPath, false)
		mfs.mapInode(ino, childPath)

		abs := mfs.mutablePath(childPath)
		if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
			t.Fatal(err)
		}
		fd, err := syscall.Open(abs, syscall.O_CREAT|syscall.O_WRONLY|syscall.O_TRUNC, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = syscall.Write(fd, []byte("hello"))
		_ = syscall.Close(fd)

		now := time.Now().UnixNano()
		node := &GraphNode{
			Kind:    NodeFile,
			Mode:    uint32(0o644),
			UID:     0,
			GID:     0,
			Size:    5,
			MtimeNs: now,
			CtimeNs: now,
			HasData: true,
		}

		parentID := mfs.resolveParentNodeID(parentPath)
		_, err = mfs.journal.CreateNodeEdgeAndWhiteout(parentID, fmt.Sprintf("file_%04d.txt", i), node, false)
		if err != nil {
			t.Fatal(err)
		}
	}

	start := time.Now()
	for i := range files {
		ino := mfs.pathToIno(fmt.Sprintf("/file_%04d.txt", i), false)
		mfs.dirtyMeta.Store(ino, pendingMeta{
			size:    5,
			mtimeNs: time.Now().UnixNano(),
			ctimeNs: time.Now().UnixNano(),
		})
	}

	for i := range files {
		ino := mfs.pathToIno(fmt.Sprintf("/file_%04d.txt", i), false)
		inoMu := mfs.getInoLock(ino)
		inoMu.Lock()
		if meta, ok := mfs.dirtyMeta.LoadAndDelete(ino); ok {
			path := fmt.Sprintf("/file_%04d.txt", i)
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

	elapsed := time.Since(start)
	t.Logf("Flush pattern: %d files in %v (%.0f files/sec)", files, elapsed, float64(files)/elapsed.Seconds())

	if elapsed > 5*time.Second {
		t.Errorf("Flush pattern too slow: %v for %d files", elapsed, files)
	}
}

func TestBottleneck_ConcurrentResolvePath(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	node := &GraphNode{Kind: NodeFile, Mode: 0o644}
	_, err := j.EnsureNodePath("/a/b/c/d/e/f.txt", node, false)
	if err != nil {
		t.Fatal(err)
	}

	const workers = 32
	const opsPerWorker = 500
	var wg sync.WaitGroup
	wg.Add(workers)

	start := time.Now()

	for range workers {
		go func() {
			defer wg.Done()
			for range opsPerWorker {
				nodeID, _, _, _, err := j.ResolvePath("/a/b/c/d/e/f.txt")
				if err != nil {
					t.Errorf("ResolvePath: %v", err)
					return
				}
				if nodeID == 0 {
					t.Error("nodeID = 0")
					return
				}
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)
	totalOps := workers * opsPerWorker
	opsPerSec := float64(totalOps) / elapsed.Seconds()

	t.Logf("ResolvePath: %d ops in %v (%.0f ops/sec)", totalOps, elapsed, opsPerSec)

	if opsPerSec < 1000 {
		t.Errorf("ResolvePath throughput too low: %.0f ops/sec (threshold: 1000)", opsPerSec)
	}
}

func TestRace_CopyUpDoubleInvocation(t *testing.T) {
	mfs, cleanup := testMutableFS(t)
	defer cleanup()

	parentPath := "/"
	mfs.mapInode(RootInode, parentPath)

	childPath := "/copyup_race.txt"
	ino := mfs.pathToIno(childPath, false)
	mfs.mapInode(ino, childPath)

	abs := mfs.mutablePath(childPath)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		t.Fatal(err)
	}

	re := &ResolvedEntry{
		Path:      childPath,
		Inode:     ino,
		IsDir:     false,
		Mode:      0o644,
		UID:       0,
		GID:       0,
		Size:      0,
		DataIsMut: false,
	}

	var copyUpCount atomic.Int32

	const concurrency = 16
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for range concurrency {
		go func() {
			defer wg.Done()
			err := mfs.copyUp(re)
			if err != nil {
				copyUpCount.Add(1)
				t.Errorf("copyUp: %v", err)
			}
		}()
	}

	wg.Wait()

	t.Logf("copyUp invoked %d times (expected 1 with per-inode lock)", copyUpCount.Load())
}

func TestRace_WriteWithoutHandle(t *testing.T) {
	mfs, cleanup := testMutableFS(t)
	defer cleanup()

	parentPath := "/"
	mfs.mapInode(RootInode, parentPath)

	childPath := "/write_no_fh.txt"
	ino := mfs.pathToIno(childPath, false)
	mfs.mapInode(ino, childPath)

	abs := mfs.mutablePath(childPath)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		t.Fatal(err)
	}
	fd, err := syscall.Open(abs, syscall.O_CREAT|syscall.O_WRONLY|syscall.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
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
	parentID := mfs.resolveParentNodeID(parentPath)
	nodeID, err := mfs.journal.CreateNodeEdgeAndWhiteout(parentID, "write_no_fh.txt", journalNode, false)
	if err != nil {
		t.Fatal(err)
	}
	journalNode.ID = nodeID

	const writers = 16
	const writesPerWriter = 100
	var wg sync.WaitGroup
	wg.Add(writers)

	var totalWritten atomic.Int64
	var writeErrors atomic.Int64

	for w := range writers {
		go func(wID int) {
			defer wg.Done()
			for i := range writesPerWriter {
				data := fmt.Appendf(nil, "w%d_%d\n", wID, i)
				input := &fuse.WriteIn{
					InHeader: fuse.InHeader{NodeId: ino},
					Offset:   uint64(wID*writesPerWriter*64 + i*64),
					Size:     uint32(len(data)),
				}
				written, status := mfs.Write(nil, input, data)
				if status != fuse.OK {
					writeErrors.Add(1)
					t.Errorf("Write w%d op%d: %s", wID, i, status)
					return
				}
				totalWritten.Add(int64(written))
			}
		}(w)
	}

	wg.Wait()

	if errCount := writeErrors.Load(); errCount > 0 {
		t.Errorf("%d write errors", errCount)
	}

	t.Logf("Write without fh: %d bytes written, %d errors", totalWritten.Load(), writeErrors.Load())
}
