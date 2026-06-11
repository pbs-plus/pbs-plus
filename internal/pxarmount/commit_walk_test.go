package pxarmount

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"
	"github.com/puzpuzpuz/xsync/v4"
)

// createTestArchive builds a split pxar archive on disk with this structure:
//
//	/
//	├── file_root.txt          ("root file content")
//	├── dir1/
//	│   ├── file_dir1.txt      ("dir1 file content")
//	│   └── dir2/
//	│       ├── file_dir2a.txt ("dir2a content")
//	│       └── file_dir2b.txt ("dir2b content")
//	└── dir3/
//	    └── file_dir3.txt      ("dir3 content")
//
// Returns (pbsStoreDir, metaDidxPath, payloadDidxPath).
func createTestArchive(t *testing.T) (string, string, string) {
	t.Helper()
	dir := t.TempDir()

	config, _ := buzhash.NewConfig(4096)
	ls, err := backupproxy.NewLocalStore(dir, config, false)
	if err != nil {
		t.Fatal(err)
	}

	sess, err := ls.StartSession(context.TODO(), backupproxy.BackupConfig{
		BackupType: datastore.BackupVM,
		BackupID:   "test",
	})
	if err != nil {
		t.Fatal(err)
	}

	writer := transfer.NewSessionWriter(context.TODO(), sess, "root.mpxar.didx", "root.ppxar.didx")

	rootMeta := pxar.DirMetadata(0o755).Build()
	if err := writer.Begin(&rootMeta, transfer.Options{Format: format.FormatVersion2}); err != nil {
		t.Fatal(err)
	}

	fileMeta := pxar.FileMetadata(0o644).Build()
	if err := writer.WriteEntry(&pxar.Entry{
		Path:     "file_root.txt",
		Kind:     pxar.KindFile,
		Metadata: fileMeta,
		FileSize: 17,
	}, []byte("root file content")); err != nil {
		t.Fatal(err)
	}

	dirMeta := pxar.DirMetadata(0o755).Build()
	if err := writer.BeginDirectory("dir1", &dirMeta); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteEntry(&pxar.Entry{
		Path:     "file_dir1.txt",
		Kind:     pxar.KindFile,
		Metadata: fileMeta,
		FileSize: 17,
	}, []byte("dir1 file content")); err != nil {
		t.Fatal(err)
	}

	if err := writer.BeginDirectory("dir2", &dirMeta); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteEntry(&pxar.Entry{
		Path:     "file_dir2a.txt",
		Kind:     pxar.KindFile,
		Metadata: fileMeta,
		FileSize: 13,
	}, []byte("dir2a content")); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteEntry(&pxar.Entry{
		Path:     "file_dir2b.txt",
		Kind:     pxar.KindFile,
		Metadata: fileMeta,
		FileSize: 13,
	}, []byte("dir2b content")); err != nil {
		t.Fatal(err)
	}
	if err := writer.EndDirectory(); err != nil {
		t.Fatal(err)
	}
	if err := writer.EndDirectory(); err != nil {
		t.Fatal(err)
	}

	if err := writer.BeginDirectory("dir3", &dirMeta); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteEntry(&pxar.Entry{
		Path:     "file_dir3.txt",
		Kind:     pxar.KindFile,
		Metadata: fileMeta,
		FileSize: 13,
	}, []byte("dir3 content")); err != nil {
		t.Fatal(err)
	}
	if err := writer.EndDirectory(); err != nil {
		t.Fatal(err)
	}

	if err := writer.Finish(); err != nil {
		t.Fatal(err)
	}
	if _, err := sess.Finish(context.TODO()); err != nil {
		t.Fatal(err)
	}

	return dir, filepath.Join(dir, "root.mpxar.didx"), filepath.Join(dir, "root.ppxar.didx")
}

// openTestArchive opens the test archive as a PxarFS.
func openTestArchive(t *testing.T, pbsStoreDir, metaPath, payloadPath string) *PxarFS {
	t.Helper()
	metaData, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	payloadData, err := os.ReadFile(payloadPath)
	if err != nil {
		t.Fatal(err)
	}

	store, err := datastore.NewChunkStore(pbsStoreDir)
	if err != nil {
		t.Fatal(err)
	}
	source := datastore.NewChunkStoreSource(store)

	reader, err := transfer.NewSplitReader(metaData, payloadData, source)
	if err != nil {
		t.Fatalf("NewSplitReader: %v", err)
	}

	fs, err := NewPxarFS(reader)
	if err != nil {
		t.Fatalf("NewPxarFS: %v", err)
	}
	return fs
}

// newTestMFS creates a MutableFS with a real pxarFS and a fresh journal.
func newTestMFS(t *testing.T, pxarFS *PxarFS) *MutableFS {
	t.Helper()
	journalDir := filepath.Join(t.TempDir(), "journal")
	journal, err := OpenJournal(journalDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { journal.Close() })

	mutableDir := t.TempDir()
	mfs := NewMutableFS(pxarFS, journal, mutableDir)
	return mfs
}

// TestCommitWalkUncachedNodesEmptyDirectories proves the bug: when PxarFS
// nodes are not registered (simulating directories that were never accessed
// via FUSE), commitWalk produces empty directories for the uncached levels.
func TestCommitWalkUncachedNodesEmptyDirectories(t *testing.T) {
	pbsStoreDir, metaPath, payloadPath := createTestArchive(t)
	pxarFS := openTestArchive(t, pbsStoreDir, metaPath, payloadPath)
	mfs := newTestMFS(t, pxarFS)

	// Simulate "never browsed" state: clear all nodes except root.
	pxarFS.mu.Lock()
	root := pxarFS.nodes[RootInode]
	pxarFS.nodes = make(map[uint64]node)
	pxarFS.nodes[RootInode] = root
	pxarFS.mu.Unlock()

	// Run commitWalk with the tracking writer.
	w := &trackingWriter{}
	ow := &commitWalkState{
		mfs:           mfs,
		writer:        w,
		prog:          &noopProgress{},
		xattrCache:    make(map[int64][]format.XAttr),
		backedHashes:  make(map[string]uint64),
		redirectCache: make(map[string]*pxar.Entry),
		pendingRefs:   make([]commitEntry, 0, 64),
	}

	if err := ow.commitWalk(1, RootInode, "/"); err != nil {
		t.Fatalf("commitWalk failed: %v", err)
	}

	t.Logf("ops: %v", w.ops)
	t.Logf("dirOpens=%d dirCloses=%d refs=%d symlinks=%d emptyFiles=%d backedFiles=%d",
		w.dirOpens, w.dirCloses, len(w.refs), len(w.symlinks), len(w.emptyFiles), len(w.backedFiles))

	// The archive has 5 files total across 3 directories.
	totalFiles := len(w.refs) + len(w.backedFiles) + len(w.emptyFiles)
	if totalFiles < 5 {
		t.Errorf("BUG REPRODUCED: expected at least 5 files in output, got %d", totalFiles)
		t.Errorf("ops: %v", w.ops)
	}

	// Verify dir2's files specifically (the deepest level).
	hasDir2a := false
	hasDir2b := false
	for _, r := range w.refs {
		if r.name == "file_dir2a.txt" {
			hasDir2a = true
		}
		if r.name == "file_dir2b.txt" {
			hasDir2b = true
		}
	}
	for _, f := range w.emptyFiles {
		if f == "file_dir2a.txt" {
			hasDir2a = true
		}
		if f == "file_dir2b.txt" {
			hasDir2b = true
		}
	}
	if !hasDir2a || !hasDir2b {
		t.Errorf("BUG REPRODUCED: dir2 files missing (dir2a=%v dir2b=%v)", hasDir2a, hasDir2b)
	}
}

// TestReadDirRawRequiresNodeCache proves that PxarFS.ReadDirRaw fails
// for inodes not in the node cache. This is the root cause of the empty
// directories bug during commit.
func TestReadDirRawRequiresNodeCache(t *testing.T) {
	pbsStoreDir, metaPath, payloadPath := createTestArchive(t)
	pxarFS := openTestArchive(t, pbsStoreDir, metaPath, payloadPath)

	// Step 1: Read root entries and manually register dir1.
	rootEntries, err := pxarFS.ReadDirRaw(RootInode)
	if err != nil {
		t.Fatalf("ReadDirRaw(root): %v", err)
	}
	var dir1Slim *dirEntrySlim
	for i := range rootEntries {
		if rootEntries[i].name == "dir1" {
			dir1Slim = &rootEntries[i]
			break
		}
	}
	if dir1Slim == nil {
		t.Fatal("dir1 not found in root entries")
	}

	// Manually register dir1 so ReadDirRaw(dir1Inode) works.
	pxarFS.RegisterSlimNode(dir1Slim, RootInode)

	entries, err := pxarFS.ReadDirRaw(dir1Slim.inode)
	if err != nil {
		t.Fatalf("ReadDirRaw(dir1) with manually registered node: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("dir1 should have children when node is registered")
	}
	t.Logf("dir1 has %d entries when registered", len(entries))

	// Step 2: Clear ALL non-root nodes to simulate commit walk state.
	pxarFS.mu.Lock()
	rootNode := pxarFS.nodes[RootInode]
	pxarFS.nodes = make(map[uint64]node)
	pxarFS.nodes[RootInode] = rootNode
	pxarFS.mu.Unlock()

	// Step 3: Re-read root (works because root is cached).
	rootEntries2, err := pxarFS.ReadDirRaw(RootInode)
	if err != nil {
		t.Fatalf("ReadDirRaw(root) after cache clear: %v", err)
	}

	// Step 4: Find dir1's entryStart, read full entry via ReadEntryAt
	// (exactly what processDeferredDir does).
	var dir1EntryStart uint64
	for _, e := range rootEntries2 {
		if e.name == "dir1" {
			dir1EntryStart = e.entryStart
			break
		}
	}

	pxarFS.readerMu.Lock()
	dir1Full, err := pxarFS.Reader().ReadEntryAt(int64(dir1EntryStart))
	pxarFS.readerMu.Unlock()
	if err != nil {
		t.Fatalf("ReadEntryAt for dir1: %v", err)
	}

	childIno := ToInode(dir1Full)
	t.Logf("dir1: entryStart=%d childIno=%d", dir1EntryStart, childIno)

	// Step 5: Try ReadDirRaw(childIno) — this is what commitWalk does.
	// ReadDirRaw itself is NOT fixed — it still requires cached nodes.
	// The fix is in processDeferredDir/emitPxarDir which now registers
	// nodes before recursing. This test documents the root cause.
	_, err = pxarFS.ReadDirRaw(childIno)
	if err != nil {
		t.Logf("Root cause confirmed: ReadDirRaw(childIno=%d) requires cached node: %v", childIno, err)
		t.Logf("The fix is in processDeferredDir/emitPxarDir which now register nodes " +
			"via registerPxarDir before the recursive commitWalk call.")
	} else {
		t.Errorf("ReadDirRaw succeeded for uncached inode — test precondition broken")
	}
}

// TestCommitWalkWithRegisteredNodes proves that when nodes ARE properly
// registered, commitWalk produces the correct output with all files present.
// This test should pass even before the fix, proving the test infrastructure works.
func TestCommitWalkWithRegisteredNodes(t *testing.T) {
	pbsStoreDir, metaPath, payloadPath := createTestArchive(t)
	pxarFS := openTestArchive(t, pbsStoreDir, metaPath, payloadPath)
	mfs := newTestMFS(t, pxarFS)

	// Pre-register all nodes by walking the pxar tree (simulating FUSE access).
	registerAllNodes(t, pxarFS, RootInode)

	w := &trackingWriter{}
	ow := &commitWalkState{
		mfs:           mfs,
		writer:        w,
		prog:          &noopProgress{},
		xattrCache:    make(map[int64][]format.XAttr),
		backedHashes:  make(map[string]uint64),
		redirectCache: make(map[string]*pxar.Entry),
		pendingRefs:   make([]commitEntry, 0, 64),
	}

	if err := ow.commitWalk(1, RootInode, "/"); err != nil {
		t.Fatalf("commitWalk failed: %v", err)
	}

	totalFiles := len(w.refs) + len(w.backedFiles) + len(w.emptyFiles)
	if totalFiles < 5 {
		t.Errorf("expected at least 5 files with registered nodes, got %d", totalFiles)
		t.Errorf("ops: %v", w.ops)
	}

	// Verify dir2 files are present.
	hasDir2a, hasDir2b := false, false
	for _, r := range w.refs {
		if r.name == "file_dir2a.txt" {
			hasDir2a = true
		}
		if r.name == "file_dir2b.txt" {
			hasDir2b = true
		}
	}
	if !hasDir2a || !hasDir2b {
		t.Errorf("dir2 files missing with registered nodes (dir2a=%v dir2b=%v)", hasDir2a, hasDir2b)
	}
}

// registerAllNodes recursively registers all pxar nodes in the cache.
func registerAllNodes(t *testing.T, fs *PxarFS, ino uint64) {
	t.Helper()
	entries, err := fs.ReadDirRaw(ino)
	if err != nil {
		return
	}
	for _, e := range entries {
		fs.RegisterSlimNode(&e, ino)
		if e.isDir {
			registerAllNodes(t, fs, e.inode)
		}
	}
}

// Ensure compile-time interface compliance.
var _ CommitProgress = (*noopProgress)(nil)

// Suppress unused import warnings — these are used by the real code.
var (
	_ *sync.Mutex
	_ *xsync.Map[string, uint64]
)
