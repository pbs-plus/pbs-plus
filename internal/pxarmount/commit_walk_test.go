package pxarmount

import (
	"context"
	"fmt"
	"io"
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

func newTestMFS(t *testing.T, pxarFS *PxarFS) *MutableFS {
	t.Helper()
	journalDir := filepath.Join(t.TempDir(), "journal")
	journal, err := OpenJournal(journalDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.Close() })

	mutableDir := t.TempDir()
	mfs := NewMutableFS(pxarFS, journal, mutableDir)
	return mfs
}

func TestCommitWalkUncachedNodesEmptyDirectories(t *testing.T) {
	pbsStoreDir, metaPath, payloadPath := createTestArchive(t)
	pxarFS := openTestArchive(t, pbsStoreDir, metaPath, payloadPath)
	mfs := newTestMFS(t, pxarFS)

	pxarFS.mu.Lock()
	root := pxarFS.nodes[RootInode]
	pxarFS.nodes = make(map[uint64]node)
	pxarFS.nodes[RootInode] = root
	pxarFS.mu.Unlock()

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

	totalFiles := len(w.refs) + len(w.backedFiles) + len(w.emptyFiles)
	if totalFiles < 5 {
		t.Errorf("BUG REPRODUCED: expected at least 5 files in output, got %d", totalFiles)
		t.Errorf("ops: %v", w.ops)
	}

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

func TestReadDirRawRequiresNodeCache(t *testing.T) {
	pbsStoreDir, metaPath, payloadPath := createTestArchive(t)
	pxarFS := openTestArchive(t, pbsStoreDir, metaPath, payloadPath)

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

	pxarFS.RegisterSlimNode(dir1Slim, RootInode)

	entries, err := pxarFS.ReadDirRaw(dir1Slim.inode)
	if err != nil {
		t.Fatalf("ReadDirRaw(dir1) with manually registered node: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("dir1 should have children when node is registered")
	}
	t.Logf("dir1 has %d entries when registered", len(entries))

	pxarFS.mu.Lock()
	rootNode := pxarFS.nodes[RootInode]
	pxarFS.nodes = make(map[uint64]node)
	pxarFS.nodes[RootInode] = rootNode
	pxarFS.mu.Unlock()

	rootEntries2, err := pxarFS.ReadDirRaw(RootInode)
	if err != nil {
		t.Fatalf("ReadDirRaw(root) after cache clear: %v", err)
	}

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

	_, err = pxarFS.ReadDirRaw(childIno)
	if err != nil {
		t.Logf("Root cause confirmed: ReadDirRaw(childIno=%d) requires cached node: %v", childIno, err)
		t.Logf("The fix is in processDeferredDir/emitPxarDir which now register nodes " +
			"via registerPxarDir before the recursive commitWalk call.")
	} else {
		t.Errorf("ReadDirRaw succeeded for uncached inode  -  test precondition broken")
	}
}

func TestCommitWalkWithRegisteredNodes(t *testing.T) {
	pbsStoreDir, metaPath, payloadPath := createTestArchive(t)
	pxarFS := openTestArchive(t, pbsStoreDir, metaPath, payloadPath)
	mfs := newTestMFS(t, pxarFS)

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

func TestCommitWalkDeepNesting(t *testing.T) {
	pbsStoreDir, metaPath, payloadPath := createDeepArchive(t)
	pxarFS := openTestArchive(t, pbsStoreDir, metaPath, payloadPath)
	mfs := newTestMFS(t, pxarFS)

	pxarFS.mu.Lock()
	root := pxarFS.nodes[RootInode]
	pxarFS.nodes = make(map[uint64]node)
	pxarFS.nodes[RootInode] = root
	pxarFS.mu.Unlock()

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
	if totalFiles < 4 {
		t.Errorf("expected 4 files in deep archive, got %d", totalFiles)
		t.Errorf("ops: %v", w.ops)
	}

	hasDeep := false
	for _, r := range w.refs {
		if r.name == "deep.txt" {
			hasDeep = true
		}
	}
	if !hasDeep {
		t.Errorf("deepest file missing (dir1/dir2/dir3/deep.txt)")
	}
}

func createDeepArchive(t *testing.T) (string, string, string) {
	t.Helper()
	dir := t.TempDir()

	config, _ := buzhash.NewConfig(4096)
	ls, err := backupproxy.NewLocalStore(dir, config, false)
	if err != nil {
		t.Fatal(err)
	}

	sess, err := ls.StartSession(context.TODO(), backupproxy.BackupConfig{
		BackupType: datastore.BackupVM,
		BackupID:   "deep",
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
	dirMeta := pxar.DirMetadata(0o755).Build()

	if err := writer.BeginDirectory("dir1", &dirMeta); err != nil {
		t.Fatal(err)
	}
	if err := writer.BeginDirectory("dir2", &dirMeta); err != nil {
		t.Fatal(err)
	}
	if err := writer.BeginDirectory("dir3", &dirMeta); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"deep.txt", "a.txt", "b.txt", "c.txt"} {
		if err := writer.WriteEntry(&pxar.Entry{
			Path:     name,
			Kind:     pxar.KindFile,
			Metadata: fileMeta,
			FileSize: 5,
		}, []byte("data")); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.EndDirectory(); err != nil {
		t.Fatal(err)
	}
	if err := writer.EndDirectory(); err != nil {
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

func TestJournalDirMergesPxarChildren(t *testing.T) {
	pbsStoreDir, metaPath, payloadPath := createTestArchive(t)
	pxarFS := openTestArchive(t, pbsStoreDir, metaPath, payloadPath)
	mfs := newTestMFS(t, pxarFS)

	pxarFS.mu.Lock()
	root := pxarFS.nodes[RootInode]
	pxarFS.nodes = make(map[uint64]node)
	pxarFS.nodes[RootInode] = root
	pxarFS.mu.Unlock()

	dir1ID, err := mfs.journal.CreateNodeEdgeAndWhiteout(
		1,
		"dir1",
		&GraphNode{
			Kind: NodeDir,
			Mode: 0o755,
			UID:  0,
			GID:  0,
		},
		false,
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = mfs.journal.CreateNodeEdgeAndWhiteout(
		dir1ID,
		"newfile.txt",
		&GraphNode{
			Kind: NodeFile,
			Mode: 0o644,
			UID:  0,
			GID:  0,
			Size: 0,
		},
		false,
	)
	if err != nil {
		t.Fatal(err)
	}

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

	hasPxrFile := false
	hasNewFile := false
	hasDir2 := false
	for _, r := range w.refs {
		if r.name == "file_dir1.txt" {
			hasPxrFile = true
		}
		if r.name == "newfile.txt" {
			hasNewFile = true
		}
	}
	for _, f := range w.emptyFiles {
		if f == "file_dir1.txt" {
			hasPxrFile = true
		}
		if f == "newfile.txt" {
			hasNewFile = true
		}
	}
	if w.dirOpens >= 3 {
		hasDir2 = true
	}

	if !hasPxrFile {
		t.Errorf("BUG: pxar file file_dir1.txt missing from dir1  -  " +
			"journal dir overlays pxar dir but pxar children were dropped")
	}
	if !hasNewFile {
		t.Errorf("journal file newfile.txt missing from dir1")
	}
	if !hasDir2 {
		t.Errorf("pxar subdir dir2 missing from dir1")
	}
}

func TestReadDirRawErrorSwallowed(t *testing.T) {
	pbsStoreDir, metaPath, payloadPath := createTestArchive(t)
	pxarFS := openTestArchive(t, pbsStoreDir, metaPath, payloadPath)

	registerAllNodes(t, pxarFS, RootInode)

	pxarFS.mu.Lock()
	for ino, n := range pxarFS.nodes {
		if ino != RootInode && n.isDir {
			n.contentOffset = 999999999
			pxarFS.nodes[ino] = n
			break
		}
	}
	pxarFS.mu.Unlock()

	mfs := newTestMFS(t, pxarFS)

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

	err := ow.commitWalk(1, RootInode, "/")
	if err != nil {
		t.Fatalf("commitWalk should not return error even with corrupted inode, got: %v", err)
	}

	t.Logf("ops: %v", w.ops)

	if len(w.ops) == 0 {
		t.Error("expected some ops from root-level entries")
	}

	hasAllFiles := len(w.refs) >= 5
	if hasAllFiles {
		t.Log("all 5 refs present  -  corrupted dir's children came from somewhere")
	}

	t.Logf("confirmed: commitWalk returns nil error even when ReadDirRaw fails on corrupted inode")
	t.Logf("refs=%d backed=%d emptyFiles=%d dirOpens=%d",
		len(w.refs), len(w.backedFiles), len(w.emptyFiles), w.dirOpens)
}

func TestLastRefPayloadOffsetNotStaleInSimpleArchive(t *testing.T) {
	pbsStoreDir, metaPath, payloadPath := createTestArchive(t)
	pxarFS := openTestArchive(t, pbsStoreDir, metaPath, payloadPath)
	mfs := newTestMFS(t, pxarFS)

	pxarFS.mu.Lock()
	root := pxarFS.nodes[RootInode]
	pxarFS.nodes = make(map[uint64]node)
	pxarFS.nodes[RootInode] = root
	pxarFS.mu.Unlock()

	w := &monotonicRefWriter{}
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
	t.Logf("refs=%d backed=%d emptyFiles=%d",
		len(w.refs), len(w.backedFiles), len(w.emptyFiles))

	totalFiles := len(w.refs) + len(w.backedFiles) + len(w.emptyFiles)
	if totalFiles != 5 {
		t.Errorf("expected 5 total files, got %d", totalFiles)
	}

	if len(w.backedFiles) > 0 {
		t.Logf("PERFORMANCE: %d files re-encoded  -  prevRefOffset staleness detected", len(w.backedFiles))
	} else {
		t.Log("no re-encodes  -  archive structure maintains monotonic offsets")
	}
}

type monotonicRefWriter struct {
	trackingWriter
	payloadPos uint64
	prevRefOff uint64
	hasPrev    bool
}

func (w *monotonicRefWriter) WriteEntryRef(entry *pxar.Entry, offset uint64) error {
	if offset < w.payloadPos {
		return fmt.Errorf("payload offset %d < write pos %d", offset, w.payloadPos)
	}
	if w.hasPrev && offset <= w.prevRefOff {
		return fmt.Errorf("payload offset %d <= prev ref %d", offset, w.prevRefOff)
	}

	w.refs = append(w.refs, refRecord{entry.Path, offset})
	w.ops = append(w.ops, "ref:"+entry.Path)
	w.prevRefOff = offset
	w.hasPrev = true
	return nil
}

func (w *monotonicRefWriter) WriteEntryReader(entry *pxar.Entry, r io.Reader, size uint64) error {
	w.payloadPos += size + 64
	w.backedFiles = append(w.backedFiles, entry.Path)
	w.ops = append(w.ops, "backed:"+entry.Path)
	return nil
}

func TestRegisterPxarDirNodesRefCountZero(t *testing.T) {
	pbsStoreDir, metaPath, payloadPath := createTestArchive(t)
	pxarFS := openTestArchive(t, pbsStoreDir, metaPath, payloadPath)
	mfs := newTestMFS(t, pxarFS)

	rootEntries, err := pxarFS.ReadDirRaw(RootInode)
	if err != nil {
		t.Fatalf("ReadDirRaw(root): %v", err)
	}

	var childEntry *dirEntrySlim
	for i := range rootEntries {
		if rootEntries[i].isDir {
			childEntry = &rootEntries[i]
			break
		}
	}
	if childEntry == nil {
		t.Fatal("no child dir found")
	}

	pxarFS.mu.Lock()
	root := pxarFS.nodes[RootInode]
	pxarFS.nodes = make(map[uint64]node)
	pxarFS.nodes[RootInode] = root
	pxarFS.mu.Unlock()

	pxarFS.mu.RLock()
	_, ok := pxarFS.nodes[childEntry.inode]
	pxarFS.mu.RUnlock()
	if ok {
		t.Fatal("child should not be cached after clearing")
	}

	pxarFS.readerMu.Lock()
	pxarEntry, err := pxarFS.Reader().ReadEntryAt(int64(childEntry.entryStart))
	pxarFS.readerMu.Unlock()
	if err != nil {
		t.Fatalf("ReadEntryAt: %v", err)
	}

	ow := &commitWalkState{
		mfs:           mfs,
		writer:        &trackingWriter{},
		prog:          &noopProgress{},
		xattrCache:    make(map[int64][]format.XAttr),
		backedHashes:  make(map[string]uint64),
		redirectCache: make(map[string]*pxar.Entry),
		pendingRefs:   make([]commitEntry, 0, 64),
	}
	ow.registerPxarDir(pxarEntry)

	pxarFS.mu.RLock()
	n, ok := pxarFS.nodes[childEntry.inode]
	pxarFS.mu.RUnlock()
	if !ok {
		t.Fatal("registerPxarDir did not cache the node")
	}

	if n.refs != 0 {
		t.Fatalf("expected refs=0, got refs=%d", n.refs)
	}

	t.Logf("confirmed: registered node inode=%d has refs=0  -  eligible for eviction if cache > 1M", childEntry.inode)
}

var _ CommitProgress = (*noopProgress)(nil)

var (
	_ *sync.Mutex
	_ *xsync.Map[string, uint64]
)
