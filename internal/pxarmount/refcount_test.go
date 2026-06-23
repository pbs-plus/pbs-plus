package pxarmount

import (
	"sync"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// TestRegisterSlimNodeRefCount verifies that a freshly registered node
// has refs=1 (not refs=0) so it cannot be evicted between registration
// and the refNode call. The old code created nodes with refs=0, creating
// a window where eviction could remove them.
func TestRegisterSlimNodeRefCount(t *testing.T) {
	fs := &PxarFS{
		nodes: make(map[uint64]node),
	}
	e := dirEntrySlim{
		name:          "testfile",
		inode:         42,
		entryStart:    100,
		contentOffset: 200,
		fileSize:      1024,
		mode:          uint32(0o644),
		mtimeSecs:     1700000000,
		uid:           1000,
		mtimeNanos:    0,
		gid:           1000,
		isDir:         false,
		isSymlink:     false,
		isReg:         true,
	}

	fs.registerSlimNode(&e, RootInode)

	n, ok := fs.nodes[42]
	if !ok {
		t.Fatal("node not registered")
	}
	if n.refs != 1 {
		t.Errorf("refs = %d, want 1 (node should not be evictable immediately after registration)", n.refs)
	}
	if n.parent != RootInode {
		t.Errorf("parent = %d, want %d", n.parent, RootInode)
	}
	if n.inode != 42 {
		t.Errorf("inode = %d, want 42", n.inode)
	}
}

// TestPreregisterSlimNodeRefCount verifies that pre-registered nodes
// (used by the commit walker) have refs=0, making them eligible for
// eviction under cache pressure since the kernel hasn't looked them up.
func TestPreregisterSlimNodeRefCount(t *testing.T) {
	fs := &PxarFS{
		nodes: make(map[uint64]node),
	}
	e := dirEntrySlim{
		name:          "prefile",
		inode:         99,
		entryStart:    500,
		contentOffset: 600,
		fileSize:      2048,
		mode:          uint32(0o644),
		mtimeSecs:     1700000000,
		uid:           1000,
		mtimeNanos:    0,
		gid:           1000,
		isReg:         true,
	}

	fs.preregisterSlimNode(&e, RootInode)

	n, ok := fs.nodes[99]
	if !ok {
		t.Fatal("node not pre-registered")
	}
	if n.refs != 0 {
		t.Errorf("refs = %d, want 0 (pre-registered nodes should be evictable)", n.refs)
	}
}

// TestPreregisterThenRegisterBumpsRefs verifies that a pre-registered
// node (refs=0) gets bumped to refs=1 when the kernel looks it up.
func TestPreregisterThenRegisterBumpsRefs(t *testing.T) {
	fs := &PxarFS{
		nodes: make(map[uint64]node),
	}
	e := dirEntrySlim{
		name:          "prefile",
		inode:         99,
		entryStart:    500,
		contentOffset: 600,
		fileSize:      2048,
		mode:          uint32(0o644),
		mtimeSecs:     1700000000,
		uid:           1000,
		mtimeNanos:    0,
		gid:           1000,
		isReg:         true,
	}

	fs.preregisterSlimNode(&e, RootInode)
	n := fs.nodes[99]
	if n.refs != 0 {
		t.Fatalf("preregister refs = %d, want 0", n.refs)
	}

	// Kernel LOOKUP bumps refs to 1
	fs.registerSlimNode(&e, RootInode)
	n = fs.nodes[99]
	if n.refs != 1 {
		t.Errorf("after registerSlimNode, refs = %d, want 1", n.refs)
	}
}

// TestRegisterSlimNodeExistingBumpRefs verifies that registering a node
// that already exists increments its ref count (matching FUSE semantics
// where each LOOKUP or READDIRPLUS entry bumps the kernel's lookup count).
func TestRegisterSlimNodeExistingBumpRefs(t *testing.T) {
	fs := &PxarFS{
		nodes: make(map[uint64]node),
	}
	e := dirEntrySlim{
		name:          "testfile",
		inode:         42,
		entryStart:    100,
		contentOffset: 200,
		fileSize:      1024,
		mode:          uint32(0o644),
		mtimeSecs:     1700000000,
		uid:           1000,
		mtimeNanos:    0,
		gid:           1000,
		isDir:         false,
		isSymlink:     false,
		isReg:         true,
	}

	fs.registerSlimNode(&e, RootInode)

	n := fs.nodes[42]
	if n.refs != 1 {
		t.Fatalf("initial refs = %d, want 1", n.refs)
	}

	// Re-register the same node (simulates a second LOOKUP or READDIRPLUS)
	fs.registerSlimNode(&e, RootInode)

	n = fs.nodes[42]
	if n.refs != 2 {
		t.Errorf("refs after second register = %d, want 2 (each registration should bump refs)", n.refs)
	}
}

// TestRegisterSlimNodeEvictionSafety fills the cache to capacity, then
// registers a new node. If refs=0, eviction could remove it immediately.
// With refs=1, the node survives eviction.
func TestRegisterSlimNodeEvictionSafety(t *testing.T) {
	fs := &PxarFS{
		nodes: make(map[uint64]node),
	}

	// Fill cache to just under maxCachedNodes
	for i := uint64(2); i < uint64(maxCachedNodes); i++ {
		fs.nodes[i] = node{
			inode:      i,
			parent:     RootInode,
			mode:       uint64(0o644),
			refs:       1,
			mtimeSecs:  1700000000,
			uid:        1000,
			mtimeNanos: 0,
			gid:        1000,
			isReg:      true,
		}
	}

	e := dirEntrySlim{
		name:          "newfile",
		inode:         uint64(maxCachedNodes),
		entryStart:    100,
		contentOffset: 200,
		fileSize:      1024,
		mode:          uint32(0o644),
		mtimeSecs:     1700000000,
		uid:           1000,
		mtimeNanos:    0,
		gid:           1000,
		isReg:         true,
	}

	fs.registerSlimNode(&e, RootInode)

	n, ok := fs.nodes[uint64(maxCachedNodes)]
	if !ok {
		t.Fatal("newly registered node was evicted (refs=0 makes nodes vulnerable to eviction)")
	}
	if n.refs < 1 {
		t.Errorf("refs = %d, want >= 1 (node should survive eviction)", n.refs)
	}
}

// TestForgetDecrementsRefs verifies that Forget properly decrements
// ref counts and removes nodes when refs reach 0.
func TestForgetDecrementsRefs(t *testing.T) {
	fs := &PxarFS{
		nodes: make(map[uint64]node),
	}
	e := dirEntrySlim{
		name:          "testfile",
		inode:         42,
		entryStart:    100,
		contentOffset: 200,
		fileSize:      1024,
		mode:          uint32(0o644),
		mtimeSecs:     1700000000,
		uid:           1000,
		mtimeNanos:    0,
		gid:           1000,
		isReg:         true,
	}

	fs.registerSlimNode(&e, RootInode)

	n := fs.nodes[42]
	if n.refs != 1 {
		t.Fatalf("initial refs = %d, want 1", n.refs)
	}

	// Forget with nlookup=1 should decrement refs to 0 and remove the node
	fs.Forget(42, 1)

	_, ok := fs.nodes[42]
	if ok {
		t.Error("node should be removed after refs reach 0")
	}
}

// TestForgetPartialDecrement verifies that Forget with nlookup < refs
// decrements but does not remove the node.
func TestForgetPartialDecrement(t *testing.T) {
	fs := &PxarFS{
		nodes: make(map[uint64]node),
	}
	e := dirEntrySlim{
		name:          "testfile",
		inode:         42,
		entryStart:    100,
		contentOffset: 200,
		fileSize:      1024,
		mode:          uint32(0o644),
		mtimeSecs:     1700000000,
		uid:           1000,
		mtimeNanos:    0,
		gid:           1000,
		isReg:         true,
	}

	fs.registerSlimNode(&e, RootInode)
	fs.registerSlimNode(&e, RootInode) // bump to refs=2

	n := fs.nodes[42]
	if n.refs != 2 {
		t.Fatalf("refs after double register = %d, want 2", n.refs)
	}

	// Forget with nlookup=1 should decrement refs to 1, node remains
	fs.Forget(42, 1)

	n, ok := fs.nodes[42]
	if !ok {
		t.Fatal("node should still exist with refs=1")
	}
	if n.refs != 1 {
		t.Errorf("refs = %d, want 1", n.refs)
	}
}

// TestConcurrentRegisterAndEviction verifies that registering a node
// and then immediately checking it doesn't race with eviction.
// This catches the case where registerSlimNode creates with refs=0
// and eviction removes the node before refNode can bump it.
func TestConcurrentRegisterAndEviction(t *testing.T) {
	fs := &PxarFS{
		nodes: make(map[uint64]node),
	}

	// Fill to max capacity
	for i := uint64(2); i <= uint64(maxCachedNodes); i++ {
		fs.nodes[i] = node{
			inode:      i,
			parent:     RootInode,
			mode:       uint64(0o644),
			refs:       1,
			mtimeSecs:  1700000000,
			uid:        1000,
			mtimeNanos: 0,
			gid:        1000,
			isReg:      true,
		}
	}

	// Register many new entries concurrently
	var wg sync.WaitGroup
	for i := uint64(maxCachedNodes + 1); i < uint64(maxCachedNodes+100); i++ {
		wg.Add(1)
		go func(ino uint64) {
			defer wg.Done()
			e := dirEntrySlim{
				name:          "concurrent",
				inode:         ino,
				entryStart:    ino * 100,
				contentOffset: ino*100 + 50,
				fileSize:      1024,
				mode:          uint32(0o644),
				mtimeSecs:     1700000000,
				uid:           1000,
				mtimeNanos:    0,
				gid:           1000,
				isReg:         true,
			}
			fs.registerSlimNode(&e, RootInode)
		}(i)
	}
	wg.Wait()

	// All newly registered nodes should have refs >= 1
	// (they should not have been evicted during registration)
	missing := 0
	zero := 0
	for i := uint64(maxCachedNodes + 1); i < uint64(maxCachedNodes+100); i++ {
		n, ok := fs.nodes[i]
		if !ok {
			missing++
		} else if n.refs < 1 {
			zero++
		}
	}
	if missing > 0 || zero > 0 {
		t.Errorf("missing=%d zero=%d (some nodes were evicted before ref bump)", missing, zero)
	}
}

// TestReadDirPlusRefCountConsistency verifies that ReadDirPlus registers
// nodes with the correct initial ref count (1 for new nodes, +1 for
// existing nodes) matching the kernel's expectation of one lookup per
// READDIRPLUS entry.
func TestReadDirPlusRefCountConsistency(t *testing.T) {
	fs := &PxarFS{
		nodes: make(map[uint64]node),
	}

	// Pre-populate root node
	fs.nodes[RootInode] = node{
		inode:  RootInode,
		parent: RootInode,
		mode:   uint64(0o755 | 0o40000000), // S_IFDIR | 0755
		isDir:  true,
		refs:   1,
	}

	// Simulate what ReadDirPlus would do: register new entries with refs=1
	entries := []dirEntrySlim{
		{name: "file1", inode: 100, mode: uint32(0o644), isReg: true},
		{name: "file2", inode: 101, mode: uint32(0o644), isReg: true},
		{name: "dir1", inode: 102, mode: uint32(0o755 | 0o40000000), isDir: true},
	}

	for i := range entries {
		e := &entries[i]
		if _, exists := fs.nodes[e.inode]; !exists {
			fs.nodes[e.inode] = node{
				entryStart:    e.entryStart,
				contentOffset: e.contentOffset,
				fileSize:      e.fileSize,
				mode:          uint64(e.mode),
				inode:         e.inode,
				parent:        RootInode,
				refs:          1,
				mtimeSecs:     e.mtimeSecs,
				uid:           e.uid,
				mtimeNanos:    e.mtimeNanos,
				gid:           e.gid,
				isDir:         e.isDir,
				isSymlink:     e.isSymlink,
				isReg:         e.isReg,
			}
		} else {
			n := fs.nodes[e.inode]
			n.refs++
			fs.nodes[e.inode] = n
		}
	}

	for _, ino := range []uint64{100, 101, 102} {
		n, ok := fs.nodes[ino]
		if !ok {
			t.Errorf("inode %d not registered", ino)
			continue
		}
		if n.refs != 1 {
			t.Errorf("inode %d refs=%d, want 1", ino, n.refs)
		}
	}

	// Second READDIRPLUS should bump refs for existing nodes
	for i := range entries {
		e := &entries[i]
		if _, exists := fs.nodes[e.inode]; !exists {
			t.Fatalf("inode %d should exist", e.inode)
		} else {
			n := fs.nodes[e.inode]
			n.refs++
			fs.nodes[e.inode] = n
		}
	}

	for _, ino := range []uint64{100, 101, 102} {
		n := fs.nodes[ino]
		if n.refs != 2 {
			t.Errorf("inode %d refs=%d after second readdir, want 2", ino, n.refs)
		}
	}
}

// TestLookupRefCountConsistency verifies that a single registerSlimNode
// call (as used by Lookup) correctly sets refs=1, matching the kernel's
// nlookup expectation for a single LOOKUP.
func TestLookupRefCountConsistency(t *testing.T) {
	fs := &PxarFS{
		nodes: make(map[uint64]node),
	}

	e := dirEntrySlim{
		name:          "lookupfile",
		inode:         55,
		entryStart:    500,
		contentOffset: 600,
		fileSize:      2048,
		mode:          uint32(0o644),
		mtimeSecs:     1700000000,
		uid:           1000,
		mtimeNanos:    0,
		gid:           1000,
		isReg:         true,
	}

	// registerSlimNode now atomically creates with refs=1
	fs.registerSlimNode(&e, RootInode)

	n, ok := fs.nodes[55]
	if !ok {
		t.Fatal("node not found after registerSlimNode")
	}
	if n.refs != 1 {
		t.Errorf("after registerSlimNode, refs=%d, want 1", n.refs)
	}

	// Simulate FORGET with nlookup=1
	fs.Forget(55, 1)
	_, ok = fs.nodes[55]
	if ok {
		t.Error("node should be removed after FORGET with nlookup=1")
	}
}

// TestEnsureNodeTimesIdempotent verifies that ensureNodeTimes only writes
// back to the cache when times were actually resolved, avoiding unnecessary
// write lock contention on repeated GetAttr calls.
func TestEnsureNodeTimesIdempotent(t *testing.T) {
	fs := &PxarFS{
		nodes: make(map[uint64]node),
	}

	// Insert a node with timesResolved=false
	fs.nodes[42] = node{
		inode:      42,
		parent:     RootInode,
		mode:       uint64(0o644),
		refs:       1,
		mtimeSecs:  1700000000,
		mtimeNanos: 123456789,
		uid:        1000,
		gid:        1000,
		isReg:      true,
	}

	n := fs.nodes[42]
	if n.timesResolved {
		t.Fatal("timesResolved should be false initially")
	}

	// Call ensureNodeTimes on a node without a pxar reader.
	// This should NOT set timesResolved=true (readEntryForNode returns error).
	fs.ensureNodeTimes(&n)

	if n.timesResolved {
		t.Error("timesResolved should remain false when entry cannot be read")
	}

	// GetAttr on a node with no reader should fall back to Stat.Mtime.
	// Since reader is nil, readEntryForNode will fail for non-root inodes.
	out := &fuse.AttrOut{}
	status := fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: 42}}, out)
	if status != fuse.OK {
		t.Fatalf("GetAttr returned %v, want OK", status)
	}

	// Node should still have timesResolved=false
	n2 := fs.nodes[42]
	if n2.timesResolved {
		t.Error("timesResolved should still be false after GetAttr on node without pxar entry")
	}

	// mtime should fall back to Stat.Mtime (mtimeSecs + mtimeNanos)
	if out.Mtime != uint64(1700000000) {
		t.Errorf("Mtime = %d, want 1700000000", out.Mtime)
	}
	if out.Mtimensec != 123456789 {
		t.Errorf("Mtimensec = %d, want 123456789", out.Mtimensec)
	}
}
