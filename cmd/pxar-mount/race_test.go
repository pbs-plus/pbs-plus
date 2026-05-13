package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// newTestPassthroughFS creates a passthroughFS backed by a temp directory.
func newTestPassthroughFS(t *testing.T) (*passthroughFS, string, func()) {
	t.Helper()
	backingDir, err := os.MkdirTemp("", "pxar-race-test-")
	if err != nil {
		t.Fatal(err)
	}

	pxar := &pxarFS{
		nodes: make(map[uint64]node),
	}

	fs := &passthroughFS{
		pxar:       pxar,
		backingDir: backingDir,
		nodePaths:  make(map[uint64]string),
		pathToIno:  make(map[string]uint64),
		backed:     make(map[uint64]bool),
		pxarDir:    make(map[uint64]bool),
		handles:    make(map[uint64]*passFh),
	}

	// Initialize root
	fs.setNode(rootInode, "/", true)
	fs.mu.Lock()
	fs.pxarDir[rootInode] = true
	fs.mu.Unlock()

	cleanup := func() {
		_ = os.RemoveAll(backingDir)
	}
	return fs, backingDir, cleanup
}

// --- Test 1: Concurrent Mkdir — single inode per directory ---
// Race: Multiple goroutines call Mkdir for the same name concurrently.
// All may succeed (kernel semantics allow this), but the key invariant is
// that the directory is assigned exactly ONE inode — no duplicates.

func TestConcurrentMkdir_NoDuplicates(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	const goroutines = 20
	var wg sync.WaitGroup

	for range goroutines {
		wg.Go(func() {
			input := &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: rootInode}}
			var out fuse.EntryOut
			fs.Mkdir(nil, input, "testdir", &out)
		})
	}
	wg.Wait()

	// The directory should exist on disk
	if _, err := os.Stat(filepath.Join(fs.backingDir, "testdir")); err != nil {
		t.Errorf("testdir should exist on disk: %v", err)
	}

	// The path should have exactly one inode mapping
	for path, count := range countPaths(fs) {
		if path == "/testdir" && count > 1 {
			t.Errorf("path /testdir has %d inode mappings (should be 1)", count)
		}
	}
}

// --- Test 2: Concurrent Create — single inode per file ---

func TestConcurrentCreate_NoDuplicates(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	const goroutines = 20
	var wg sync.WaitGroup

	for range goroutines {
		wg.Go(func() {
			input := &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: rootInode}}
			var out fuse.CreateOut
			fs.Create(nil, input, "testfile.txt", &out)
		})
	}
	wg.Wait()

	// The file should exist on disk
	if _, err := os.Stat(filepath.Join(fs.backingDir, "testfile.txt")); err != nil {
		t.Errorf("testfile.txt should exist on disk: %v", err)
	}

	// No duplicate path→inode mappings
	for path, count := range countPaths(fs) {
		if path == "/testfile.txt" && count > 1 {
			t.Errorf("path /testfile.txt has %d inode mappings (should be 1)", count)
		}
	}
}

// --- Test 3: Concurrent ReadDir allocates consistent inodes ---
// Two ReadDirPlus calls for the same directory that has backing files
// should assign the same inode to each file across calls.

func TestConcurrentReadDir_ConsistentInodes(t *testing.T) {
	fs, backingDir, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	// Pre-create some files in the backing dir
	for i := range 5 {
		path := filepath.Join(backingDir, fmt.Sprintf("file%d.txt", i))
		if err := os.WriteFile(path, []byte("hello"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// Run multiple concurrent ReadDir calls and collect inodes
	const readers = 10
	results := make([]map[string]uint64, readers)
	var wg sync.WaitGroup

	for i := range readers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			buf := make([]byte, 4096)
			out := fuse.NewDirEntryList(buf, 0)
			inodes := make(map[string]uint64)

			fs.readDirImpl(nil, &fuse.ReadIn{
				InHeader: fuse.InHeader{NodeId: rootInode},
			}, out, true)

			fs.mu.RLock()
			for ino, path := range fs.nodePaths {
				if path != "/" {
					name := filepath.Base(path)
					inodes[name] = ino
				}
			}
			fs.mu.RUnlock()
			results[idx] = inodes
		}(i)
	}
	wg.Wait()

	// All readers should see the same inode for each file
	for i := 1; i < readers; i++ {
		for name, ino := range results[0] {
			if ino2, ok := results[i][name]; !ok {
				t.Errorf("reader %d missing entry for %s", i, name)
			} else if ino2 != ino {
				t.Errorf("reader %d sees inode %d for %s, but reader 0 sees %d", i, ino2, name, ino)
			}
		}
	}

	// No duplicate path mappings
	for path, count := range countPaths(fs) {
		if path != "/" && count > 1 {
			t.Errorf("path %q has %d inode mappings (should be 1)", path, count)
		}
	}
}

// --- Test 4: Concurrent Create + ReadDir doesn't lose entries ---
// One goroutine creates files while another reads the directory.

func TestConcurrentCreateReadDir_Consistency(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	const filesToCreate = 50
	var wg sync.WaitGroup
	created := atomic.Int32{}

	// Creator goroutine: creates files
	wg.Go(func() {
		for i := range filesToCreate {
			name := fmt.Sprintf("concurrent_%d.txt", i)
			input := &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: rootInode}}
			var out fuse.CreateOut
			if st := fs.Create(nil, input, name, &out); st == fuse.OK {
				created.Add(1)
			}
		}
	})

	// Reader goroutine: reads directory repeatedly
	wg.Go(func() {
		for range 50 {
			buf := make([]byte, 16*1024)
			out := fuse.NewDirEntryList(buf, 0)
			fs.readDirImpl(nil, &fuse.ReadIn{
				InHeader: fuse.InHeader{NodeId: rootInode},
			}, out, false)
		}
	})

	wg.Wait()

	c := created.Load()
	t.Logf("created %d/%d files", c, filesToCreate)

	// All created files should be visible
	dents, err := os.ReadDir(fs.backingDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(dents) < int(c) {
		t.Errorf("expected at least %d entries in backing dir, got %d", c, len(dents))
	}

	// No duplicate path mappings
	for path, count := range countPaths(fs) {
		if path != "/" && count > 1 {
			t.Errorf("path %q has %d inode mappings (should be 1)", path, count)
		}
	}
}

// --- Test 5: Concurrent Symlink — single inode per link ---

func TestConcurrentSymlink_NoDuplicates(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	const goroutines = 20
	var wg sync.WaitGroup

	for range goroutines {
		wg.Go(func() {
			header := &fuse.InHeader{NodeId: rootInode}
			var out fuse.EntryOut
			fs.Symlink(nil, header, "/target", "testlink", &out)
		})
	}
	wg.Wait()

	// No duplicate path mappings
	for path, count := range countPaths(fs) {
		if path == "/testlink" && count > 1 {
			t.Errorf("path /testlink has %d inode mappings (should be 1)", count)
		}
	}
}

// --- Test 6: Concurrent Mkdir + Create in same parent ---
// Two goroutines: one creates a directory, the other creates a file.

func TestConcurrentMkdirAndCreate_NoConflicts(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	var wg sync.WaitGroup
	var mkdirOK, createOK atomic.Bool

	wg.Add(2)
	go func() {
		defer wg.Done()
		input := &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: rootInode}}
		var out fuse.EntryOut
		if fs.Mkdir(nil, input, "dir1", &out) == fuse.OK {
			mkdirOK.Store(true)
		}
	}()
	go func() {
		defer wg.Done()
		input := &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: rootInode}}
		var out fuse.CreateOut
		if fs.Create(nil, input, "file1.txt", &out) == fuse.OK {
			createOK.Store(true)
		}
	}()
	wg.Wait()

	if !mkdirOK.Load() {
		t.Error("Mkdir should have succeeded")
	}
	if !createOK.Load() {
		t.Error("Create should have succeeded")
	}

	for _, name := range []string{"dir1", "file1.txt"} {
		if _, err := os.Stat(filepath.Join(fs.backingDir, name)); err != nil {
			t.Errorf("%s should exist: %v", name, err)
		}
	}
}

// --- Test 7: ReadDir inode stability across multiple calls ---

func TestReadDir_InodeStability(t *testing.T) {
	fs, backingDir, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	for i := range 10 {
		path := filepath.Join(backingDir, fmt.Sprintf("stable_%d.txt", i))
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// First read
	buf1 := make([]byte, 16*1024)
	out1 := fuse.NewDirEntryList(buf1, 0)
	fs.readDirImpl(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: rootInode},
	}, out1, true)

	firstPaths := snapshotPaths(fs)

	// Second read
	buf2 := make([]byte, 16*1024)
	out2 := fuse.NewDirEntryList(buf2, 0)
	fs.readDirImpl(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: rootInode},
	}, out2, true)

	secondPaths := snapshotPaths(fs)

	// Inodes should be stable
	for path, ino1 := range firstPaths {
		ino2, ok := secondPaths[path]
		if !ok {
			t.Errorf("path %q disappeared after second readDir", path)
		} else if ino1 != ino2 {
			t.Errorf("path %q inode changed: %d → %d", path, ino1, ino2)
		}
	}

	for path, count := range countPaths(fs) {
		if count > 1 {
			t.Errorf("path %q has %d inode mappings", path, count)
		}
	}
}

// --- Test 8: Concurrent Lookup for same name returns same inode ---

func TestConcurrentLookup_ConsistentInodes(t *testing.T) {
	fs, backingDir, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	if err := os.WriteFile(filepath.Join(backingDir, "lookup_test.txt"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	const lookups = 20
	inodes := make([]uint64, lookups)
	var wg sync.WaitGroup

	for i := range lookups {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var out fuse.EntryOut
			header := &fuse.InHeader{NodeId: rootInode}
			if st := fs.Lookup(nil, header, "lookup_test.txt", &out); st == fuse.OK {
				inodes[idx] = out.NodeId
			}
		}(i)
	}
	wg.Wait()

	firstIno := inodes[0]
	if firstIno == 0 {
		t.Fatal("first lookup failed")
	}
	for i, ino := range inodes {
		if ino != firstIno {
			t.Errorf("lookup %d returned inode %d, expected %d", i, ino, firstIno)
		}
	}

	for path, count := range countPaths(fs) {
		if path == "/lookup_test.txt" && count > 1 {
			t.Errorf("path /lookup_test.txt has %d inode mappings (should be 1)", count)
		}
	}
}

// --- Test 9: Concurrent Mkdir in different subdirectories ---

func TestConcurrentMkdir_DifferentPaths(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	const dirs = 20
	var wg sync.WaitGroup
	successes := atomic.Int32{}

	for i := range dirs {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := fmt.Sprintf("unique_dir_%d", idx)
			input := &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: rootInode}}
			var out fuse.EntryOut
			if fs.Mkdir(nil, input, name, &out) == fuse.OK {
				successes.Add(1)
			}
		}(i)
	}
	wg.Wait()

	s := successes.Load()
	if s != dirs {
		t.Errorf("expected %d successes, got %d", dirs, s)
	}

	for i := range dirs {
		name := fmt.Sprintf("unique_dir_%d", i)
		if _, err := os.Stat(filepath.Join(fs.backingDir, name)); err != nil {
			t.Errorf("dir %s should exist: %v", name, err)
		}
	}
}

// --- Test 10: readDirStats concurrent access (now uses local map, no data race) ---

func TestReadDirStats_ConcurrentAccess(t *testing.T) {
	fs, backingDir, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	for i := range 10 {
		path := filepath.Join(backingDir, fmt.Sprintf("statfile_%d.txt", i))
		if err := os.WriteFile(path, []byte("data"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	const rounds = 50
	var wg sync.WaitGroup
	for range rounds {
		wg.Add(2)
		go func() {
			defer wg.Done()
			buf := make([]byte, 16*1024)
			out := fuse.NewDirEntryList(buf, 0)
			fs.readDirImpl(nil, &fuse.ReadIn{
				InHeader: fuse.InHeader{NodeId: rootInode},
			}, out, true)
		}()
		go func() {
			defer wg.Done()
			buf := make([]byte, 16*1024)
			out := fuse.NewDirEntryList(buf, 0)
			fs.readDirImpl(nil, &fuse.ReadIn{
				InHeader: fuse.InHeader{NodeId: rootInode},
			}, out, true)
		}()
	}
	wg.Wait()
}

// --- Test 11: Create then Stat ---

func TestCreateThenStat(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	const iterations = 100
	for i := range iterations {
		name := fmt.Sprintf("stat_test_%d", i)
		input := &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: rootInode}}
		var cout fuse.CreateOut
		if st := fs.Create(nil, input, name, &cout); st != fuse.OK {
			t.Fatalf("Create %s failed: %v", name, st)
		}

		ino := cout.NodeId
		var aout fuse.AttrOut
		if st := fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: ino}}, &aout); st != fuse.OK {
			t.Errorf("GetAttr for %s (ino=%d) failed: %v", name, ino, st)
		}
	}
}

// --- Test 12: Concurrent nested Mkdir ---

func TestConcurrentNestedMkdir(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	var wg sync.WaitGroup
	var ok1, ok2 atomic.Bool

	// Create /parent1 first (with proper mode)
	input := &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: rootInode}, Mode: 0o755}
	var out fuse.EntryOut
	if st := fs.Mkdir(nil, input, "parent1", &out); st != fuse.OK {
		t.Fatalf("create parent1: %v", st)
	}
	parent1Ino := out.NodeId

	wg.Add(2)
	go func() {
		defer wg.Done()
		input := &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: parent1Ino}, Mode: 0o755}
		var out fuse.EntryOut
		if fs.Mkdir(nil, input, "childA", &out) == fuse.OK {
			ok1.Store(true)
		}
	}()
	go func() {
		defer wg.Done()
		input := &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: parent1Ino}, Mode: 0o755}
		var out fuse.EntryOut
		if fs.Mkdir(nil, input, "childB", &out) == fuse.OK {
			ok2.Store(true)
		}
	}()
	wg.Wait()

	if !ok1.Load() {
		t.Error("childA Mkdir should succeed")
	}
	if !ok2.Load() {
		t.Error("childB Mkdir should succeed")
	}

	for _, name := range []string{"parent1/childA", "parent1/childB"} {
		if _, err := os.Stat(filepath.Join(fs.backingDir, name)); err != nil {
			t.Errorf("%s should exist: %v", name, err)
		}
	}
}

// helpers

func countPaths(fs *passthroughFS) map[string]int {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	m := make(map[string]int)
	for _, p := range fs.nodePaths {
		m[p]++
	}
	return m
}

func snapshotPaths(fs *passthroughFS) map[string]uint64 {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	m := make(map[string]uint64, len(fs.nodePaths))
	for ino, p := range fs.nodePaths {
		if p != "/" {
			m[p] = ino
		}
	}
	return m
}

// Compile-time check that passthroughFS implements fuse.RawFileSystem
var _ fuse.RawFileSystem = (*passthroughFS)(nil)

func init() {
	_ = syscall.ENOENT
}
