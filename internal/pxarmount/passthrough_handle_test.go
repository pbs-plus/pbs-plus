package pxarmount

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// TestSentinelFhZeroCollision demonstrates that fh=0 sentinel returned by
// Open for non-backed pxar files can collide with a real handle allocated
// by registerFh (which also starts at 0). This causes:
//   - Write to a lazy-opened file may use wrong fd
//   - Release may close wrong file's fd
func TestSentinelFhZeroCollision(t *testing.T) {
	fs, backingDir, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	fs.mutationMode = true

	// Step 1: Create a real backed file. registerFh will allocate fhID=1 (0 is sentinel).
	var cout fuse.CreateOut
	createSt := fs.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: RootInode},
		Mode:     0o644,
		Flags:    uint32(syscall.O_WRONLY | syscall.O_CREAT),
	}, "backed_file.txt", &cout)
	if createSt != fuse.OK {
		t.Fatalf("Create backed_file.txt: %v", createSt)
	}
	backedIno := cout.NodeId
	backedFh := cout.Fh

	// The first registered handle should be 1 (0 is reserved sentinel).
	if backedFh != 1 {
		t.Fatalf("expected first handle ID to be 1, got %d", backedFh)
	}

	// Step 2: Set up a pxar file node (non-backed).
	const pxarIno uint64 = 100 | NonDirBit
	relPath := "/pxar_file.txt"
	fs.pxar.mu.Lock()
	fs.pxar.nodes[pxarIno] = node{
		inode:  pxarIno,
		parent: RootInode,
		mode:   uint64(syscall.S_IFREG | 0o644),
		isReg:  true,
		refs:   1,
	}
	fs.pxar.mu.Unlock()

	fs.mu.Lock()
	fs.nodePaths[pxarIno] = relPath
	fs.pathToIno[relPath] = pxarIno
	fs.mu.Unlock()

	// Step 3: Open the pxar file for write.
	// Open should return fh=0 as sentinel (deferred materialization).
	var openOut fuse.OpenOut
	openSt := fs.Open(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: pxarIno},
		Flags:    uint32(syscall.O_WRONLY),
	}, &openOut)
	if openSt != fuse.OK {
		t.Fatalf("Open pxar file: %v", openSt)
	}

	// Open returns sentinel fh=0, but there's a REAL handle at id=1.
	if openOut.Fh != 0 {
		t.Fatalf("expected sentinel fh=0, got %d", openOut.Fh)
	}

	// Step 4: Materialize the pxar file so Write can proceed.
	// (In real use, Write calls materializePxarFile internally,
	// but we need the file on disk for the collision to manifest.)
	abs := filepath.Join(backingDir, "pxar_file.txt")
	f, err := os.OpenFile(abs, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	fs.setNode(pxarIno, relPath, true)

	// Step 5: Write to the pxar file with fh=0.
	// handleForNode(pxarIno, 0) will look for handles[0] which doesn't exist
	// (0 is never allocated). So it returns nil, and lazyOpenFh opens a new fd.
	// lazyOpenFh allocates id=2 (nextFh is 2 after Create+materialize).
	data := []byte("hello pxar")
	written, writeSt := fs.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: pxarIno},
		Fh:       0, // sentinel fh=0
		Offset:   0,
	}, data)

	if writeSt != fuse.OK {
		t.Fatalf("Write pxar file: %v", writeSt)
	}
	if written != uint32(len(data)) {
		t.Fatalf("Write returned %d, want %d", written, len(data))
	}

	// Step 6: Verify the data went to the right file.
	got, err := os.ReadFile(abs)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Errorf("pxar_file.txt content = %q, want %q", got, data)
	}

	// Step 7: Write to the backed file to verify its fd still works.
	_, wSt := fs.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: backedIno},
		Fh:       backedFh, // fh=1 — the real handle
		Offset:   0,
	}, []byte("original data"))
	if wSt != fuse.OK {
		t.Fatalf("Write backed file: %v", wSt)
	}

	// Step 8: Release the pxar file with fh=0.
	// handles[0] does NOT exist (0 is never allocated), so Release is a no-op.
	// The backed file's handle at id=1 remains intact.
	fs.Release(nil, &fuse.ReleaseIn{
		InHeader: fuse.InHeader{NodeId: pxarIno},
		Fh:       0,
	})

	// After Release(fh=0), the backed file's handle should still exist.
	fs.fhmu.Lock()
	_, stillExists := fs.handles[backedFh]
	fs.fhmu.Unlock()
	if !stillExists {
		t.Error("handle for backed_file.txt should NOT have been removed by pxar Release(fh=0)")
	}

	// Step 9: Write to backed file again — should succeed because
	// Release(fh=0) was a no-op (sentinel never collides with real handles).
	_, wSt2 := fs.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: backedIno},
		Fh:       backedFh,
		Offset:   13, // append after "original data"
	}, []byte(" appended"))
	if wSt2 != fuse.OK {
		t.Errorf("Write to backed file after Release(fh=0 sentinel) should succeed: %v", wSt2)
	}

	// Verify backed file content.
	backedContent, _ := os.ReadFile(filepath.Join(backingDir, "backed_file.txt"))
	wantBacked := "original data appended"
	if string(backedContent) != wantBacked {
		t.Errorf("backed_file.txt = %q, want %q", backedContent, wantBacked)
	}
}

// TestRelease_SentinelFhZeroDoesNotCloseWrongHandle verifies that
// Release(fh=0) for a lazily-opened file doesn't close an unrelated handle.
func TestRelease_SentinelFhZeroDoesNotCloseWrongHandle(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	fs.mutationMode = true

	// Create a backed file — gets fhID=1 (0 is reserved sentinel)
	var cout fuse.CreateOut
	if st := fs.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: RootInode},
		Mode:     0o644,
		Flags:    uint32(syscall.O_WRONLY | syscall.O_CREAT),
	}, "real_file.txt", &cout); st != fuse.OK {
		t.Fatalf("Create: %v", st)
	}
	realFh := cout.Fh
	realIno := cout.NodeId

	if realFh != 1 {
		t.Fatalf("expected first fh=1, got %d", realFh)
	}

	// Write to real file.
	data := []byte("original data")
	if _, st := fs.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: realIno},
		Fh:       realFh,
		Offset:   0,
	}, data); st != fuse.OK {
		t.Fatalf("Write real file: %v", st)
	}

	// Now simulate a Release from Open(fh=0 sentinel) for a DIFFERENT inode.
	// In real use, the kernel sends Release(fh=0) for a lazily-opened pxar file.
	// This should NOT close the real file's handle.
	differentIno := uint64(999)
	fs.Release(nil, &fuse.ReleaseIn{
		InHeader: fuse.InHeader{NodeId: differentIno},
		Fh:       0, // same fh value as real file
	})

	// The real file's handle should still be usable.
	if _, st := fs.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: realIno},
		Fh:       realFh,
		Offset:   14,
	}, []byte(" appended")); st != fuse.OK {
		t.Errorf("Write to real file after unrelated Release(fh=0) failed: %v", st)
		t.Error("BUG: Release(fh=0 sentinel) closed wrong handle")
	}
}

// TestLazyOpenFh_DedupClosesLoserFd verifies that when two goroutines race
// to lazy-open the same file, the loser's fd is properly closed.
func TestLazyOpenFh_DedupClosesLoserFd(t *testing.T) {
	fs, backingDir, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	// Create a file.
	abs := filepath.Join(backingDir, "race_file.txt")
	if err := os.WriteFile(abs, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	const ino uint64 = 100 | NonDirBit
	relPath := "/race_file.txt"
	fs.setNode(ino, relPath, true)

	var wg sync.WaitGroup
	const goroutines = 10
	results := make([]*passFh, goroutines)
	statuses := make([]fuse.Status, goroutines)

	for i := range goroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			fh, st := fs.lazyOpenFh(ino)
			results[idx] = fh
			statuses[idx] = st
		}(i)
	}
	wg.Wait()

	// At most one handle should be registered (the winner).
	fs.fhmu.Lock()
	handleCount := len(fs.handles)
	fs.fhmu.Unlock()

	// All goroutines should have gotten OK status.
	for i, st := range statuses {
		if st != fuse.OK {
			t.Errorf("goroutine %d: lazyOpenFh returned %v", i, st)
		}
	}

	// All goroutines should have gotten the SAME handle pointer.
	first := results[0]
	for i, fh := range results {
		if fh != first {
			t.Errorf("goroutine %d got different handle", i)
		}
	}

	// Verify exactly one handle for this inode.
	uniqueFds := make(map[int]bool)
	for _, fh := range results {
		if fh != nil {
			uniqueFds[fh.fd] = true
		}
	}
	if len(uniqueFds) > 1 {
		t.Errorf("expected at most 1 unique fd, got %d (fd leak)", len(uniqueFds))
	}

	t.Logf("handles registered: %d, unique fds: %d", handleCount, len(uniqueFds))
}

// TestReadDirRaw_PoolSliceNotReturnedToPool verifies that ReadDirRaw callers
// in passthrough.go properly return pool slices. If they don't, repeated
// calls allocate new slices each time (pool exhaustion).
func TestReadDirRaw_PoolSliceNotReturnedToPool(t *testing.T) {
	fs, backingDir, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	fs.mutationMode = true

	// Create files in the backing dir.
	for i := range 20 {
		path := filepath.Join(backingDir, fmt.Sprintf("file_%d.txt", i))
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	fs.setNode(RootInode, "/", true)

	// Call readDirImpl many times. Each call internally calls ReadDirRaw
	// which gets a slice from the pool. If the slice isn't returned,
	// the pool keeps creating new ones.
	const rounds = 100
	for i := range rounds {
		buf := make([]byte, 64*1024)
		out := fuse.NewDirEntryList(buf, 0)
		st := fs.readDirImpl(nil, &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: RootInode},
		}, out, true)
		if st != fuse.OK {
			t.Fatalf("readDirImpl round %d: %v", i, st)
		}
	}
}
