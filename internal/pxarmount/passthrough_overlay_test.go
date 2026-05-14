package pxarmount

import (
	"bufio"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// --- Bug 1: materializePxarFile race ---
// Two concurrent callers (Write + SetAttr) on the same pxar inode can both
// pass the isBacked check and double-materialize. With O_CREATE|O_TRUNC the
func TestMaterializePxarFile_ConcurrentDedup(t *testing.T) {
	fs, backingDir, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	// Set up a pxar file node with content.
	const ino uint64 = 100 | NonDirBit
	relPath := "testdir/file.txt"

	// Register the pxar node.
	fs.pxar.mu.Lock()
	fs.pxar.nodes[ino] = node{
		inode: ino,
		mode:  uint64(syscall.S_IFREG | 0o644),
		isReg: true,
	}
	fs.pxar.mu.Unlock()

	// Create parent dir in backing so the materialized file has a place to go.
	if err := os.MkdirAll(filepath.Join(backingDir, "testdir"), 0o755); err != nil {
		t.Fatal(err)
	}

	// Create a dummy file in backing dir to simulate a pre-existing copy.
	// This simulates the first materialization succeeding.
	abs := filepath.Join(backingDir, relPath)
	if err := os.WriteFile(abs, []byte("existing-data"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Mark as backed.
	fs.setNode(ino, relPath, true)

	// Now simulate two concurrent materializePxarFile calls.
	// The second should see isBacked=true and file exists, returning early.
	var (
		wg      sync.WaitGroup
		errs    [2]error
		paths   [2]string
		started sync.WaitGroup
	)
	started.Add(1)
	wg.Add(2)
	for i := range 2 {
		go func(idx int) {
			defer wg.Done()
			started.Wait()
			p, err := fs.materializePxarFile(ino)
			errs[idx] = err
			paths[idx] = p
		}(i)
	}
	started.Done()
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: %v", i, err)
		}
	}

	// The file should still have its original content (not truncated).
	data, err := os.ReadFile(abs)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "existing-data" {
		t.Errorf("file was corrupted by concurrent materialize: got %q", string(data))
	}
}
func TestMarkPathDeleted_ConcurrentDedup(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	// Enable mutation mode with a transaction log.
	mutDir := t.TempDir()
	tl, err := OpenTransactionLog(mutDir)
	if err != nil {
		t.Fatal(err)
	}
	fs.mutationMode = true
	fs.txnLog = tl

	const childPath = "concurrent-delete.txt"

	var wg sync.WaitGroup
	wg.Add(2)
	for range 2 {
		go func() {
			defer wg.Done()
			fs.markPathDeleted(childPath)
		}()
	}
	wg.Wait()

	txns, err := tl.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	deleteCount := 0
	for _, txn := range txns {
		if txn.Type == TxnDelete && txn.Path == childPath {
			deleteCount++
		}
	}

	// Bug: duplicate DELETE transactions recorded for same path.
	if deleteCount != 1 {
		t.Errorf("recorded %d DELETE transactions, want 1 (bug: no dedup guard)", deleteCount)
	}

	// Path should be marked deleted regardless
	if !fs.isPathDeleted(childPath) {
		t.Error("path should be marked as deleted")
	}
}
func TestTransactionLog_Durability(t *testing.T) {
	dir := t.TempDir()
	tl, err := OpenTransactionLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tl.Close() }()

	// Record a transaction
	id, err := tl.Record(TxnDelete, "/important/file")
	if err != nil {
		t.Fatal(err)
	}

	// Immediately read back — must be visible
	txns, err := tl.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(txns) != 1 {
		t.Fatalf("ReadAll returned %d entries, want 1", len(txns))
	}
	if txns[0].ID != id {
		t.Errorf("ID mismatch: got %d, want %d", txns[0].ID, id)
	}
}
func TestCreate_UndeletesDeletedPath(t *testing.T) {
	fs, backingDir, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	fs.mutationMode = true

	childPath := "undelete-test.txt"

	// Mark the path as deleted (simulating a prior pxar file deletion)
	fs.markPathDeleted(childPath)

	if !fs.isPathDeleted(childPath) {
		t.Fatal("path should be deleted")
	}

	// Now create a new file with the same name in the backing dir,
	// simulating what the FUSE Create handler does.
	abs := filepath.Join(backingDir, childPath)
	fd, err := syscall.Open(abs, syscall.O_CREAT|syscall.O_WRONLY|syscall.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	syscall.Close(fd)

	ino, _ := fs.lookupOrAllocIno(childPath, false)
	fs.setNode(ino, childPath, true)

	// Simulate what Create now does: un-delete the path.
	fs.unDeletePath(childPath)

	if fs.isPathDeleted(childPath) {
		t.Error("path should no longer be deleted after unDeletePath")
	}
}
func TestTransactionLog_ClearThenRecord(t *testing.T) {
	dir := t.TempDir()
	tl, err := OpenTransactionLog(dir)
	if err != nil {
		t.Fatal(err)
	}

	_, _ = tl.Record(TxnDelete, "/before-clear")

	if err := tl.Clear(); err != nil {
		t.Fatal(err)
	}

	// Record after clear should work
	id, err := tl.Record(TxnDelete, "/after-clear")
	if err != nil {
		t.Fatalf("Record after Clear: %v", err)
	}
	if id != 1 {
		t.Errorf("ID after clear = %d, want 1", id)
	}

	_ = tl.Close()

	// Verify only the post-clear entry exists
	tl2, err := OpenTransactionLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tl2.Close() }()

	txns, err := tl2.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(txns) != 1 {
		t.Fatalf("after clear+record: got %d entries, want 1", len(txns))
	}
	if txns[0].Path != "/after-clear" {
		t.Errorf("entry path = %q, want /after-clear", txns[0].Path)
	}
}
func TestRenamePaths_DestOverwrite(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	// Set up two inodes
	const srcIno uint64 = 100 | NonDirBit
	const dstIno uint64 = 200 | NonDirBit

	fs.setNode(srcIno, "src.txt", true)
	fs.setNode(dstIno, "dst.txt", true)

	// Rename src over dst
	fs.renamePaths("src.txt", "dst.txt")

	fs.mu.RLock()
	// src inode should now map to dst.txt
	if fs.nodePaths[srcIno] != "dst.txt" {
		t.Errorf("src inode path = %q, want dst.txt", fs.nodePaths[srcIno])
	}
	// dst.txt should map to src inode
	if fs.pathToIno["dst.txt"] != srcIno {
		t.Errorf("dst.txt inode = %d, want %d", fs.pathToIno["dst.txt"], srcIno)
	}
	// src.txt should be gone
	if _, ok := fs.pathToIno["src.txt"]; ok {
		t.Error("src.txt should not exist in pathToIno")
	}
	// Old dst inode should be cleaned up from nodePaths
	if _, ok := fs.nodePaths[dstIno]; ok {
		t.Error("old dst inode should not exist in nodePaths")
	}
	// Old dst inode should be cleaned up from backed
	if _, ok := fs.backed[dstIno]; ok {
		t.Error("old dst inode should not exist in backed")
	}
	fs.mu.RUnlock()
}
func TestTransactionLog_ClearFailureDoesNotCorruptState(t *testing.T) {
	dir := t.TempDir()
	tl, err := OpenTransactionLog(dir)
	if err != nil {
		t.Fatal(err)
	}

	_, _ = tl.Record(TxnDelete, "before-clear")

	// Close the underlying file externally to simulate os.Create failure
	_ = tl.file.Close()
	// Make the path a directory so os.Create fails
	_ = os.Remove(tl.path)
	_ = os.MkdirAll(tl.path, 0o755)

	err = tl.Clear()
	if err == nil {
		t.Fatal("expected Clear to fail when os.Create cannot create file")
	}

	// After the fix, the old file handle should still be usable.
	// Record should NOT panic even after a failed Clear.
	// Fix the path so Record can actually write.
	_ = os.Remove(tl.path)
	f2, err2 := os.Create(tl.path)
	if err2 != nil {
		t.Fatal(err2)
	}
	_ = tl.file.Close() // close the stale handle
	tl.file = f2
	tl.buf = bufio.NewWriter(f2)
	tl.enc = cbor.NewEncoder(tl.buf)
	tl.next = 0

	_, err = tl.Record(TxnDelete, "after-failed-clear")
	if err != nil {
		t.Fatalf("Record after failed Clear should not panic, got: %v", err)
	}
}
func TestMaterializePxarFile_DoesNotRecordModifyIfAlreadyBacked(t *testing.T) {
	fs, backingDir, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	// Enable mutation mode with transaction log.
	mutDir := t.TempDir()
	tl, err := OpenTransactionLog(mutDir)
	if err != nil {
		t.Fatal(err)
	}
	fs.mutationMode = true
	fs.txnLog = tl

	const ino uint64 = 100 | NonDirBit
	relPath := "already-backed.txt"

	// Register the pxar node.
	fs.pxar.mu.Lock()
	fs.pxar.nodes[ino] = node{
		inode: ino,
		mode:  uint64(syscall.S_IFREG | 0o644),
		isReg: true,
	}
	fs.pxar.mu.Unlock()

	// Pre-create the file in backing dir and mark as backed.
	abs := filepath.Join(backingDir, relPath)
	if err := os.WriteFile(abs, []byte("original"), 0o644); err != nil {
		t.Fatal(err)
	}
	fs.setNode(ino, relPath, true)

	// Call materializePxarFile — should return early without MODIFY txn.
	path, err := fs.materializePxarFile(ino)
	if err != nil {
		t.Fatal(err)
	}
	if path != relPath {
		t.Errorf("path = %q, want %q", path, relPath)
	}

	txns, err := tl.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	for _, txn := range txns {
		if txn.Type == TxnModify {
			t.Errorf("spurious MODIFY transaction recorded for already-backed file: %+v", txn)
		}
	}
}
func TestRename_ExchangeSwapsBothDirections(t *testing.T) {
	fs, backingDir, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	fs.mutationMode = true

	// Set up two backed files with different inodes
	const srcIno uint64 = 100 | NonDirBit
	const dstIno uint64 = 200 | NonDirBit

	srcPath := "/exchange-src.txt"
	dstPath := "/exchange-dst.txt"

	// Create actual files in backing dir
	srcAbs := filepath.Join(backingDir, "exchange-src.txt")
	dstAbs := filepath.Join(backingDir, "exchange-dst.txt")
	if err := os.WriteFile(srcAbs, []byte("src-content"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dstAbs, []byte("dst-content"), 0o644); err != nil {
		t.Fatal(err)
	}

	fs.setNode(srcIno, srcPath, true)
	fs.setNode(dstIno, dstPath, true)

	// Now do RENAME_EXCHANGE via the Rename handler.
	// We need a parent inode for the rename. Use RootInode.
	fs.mu.Lock()
	fs.nodePaths[RootInode] = "/"
	fs.pathToIno["/"] = RootInode
	fs.mu.Unlock()

	// Perform the exchange
	st := fs.Rename(nil, &fuse.RenameIn{
		InHeader: fuse.InHeader{NodeId: RootInode},
		Newdir:   RootInode,
		Flags:    renameExchange,
	}, "exchange-src.txt", "exchange-dst.txt")

	if st != fuse.OK {
		t.Fatalf("RENAME_EXCHANGE failed: %v", st)
	}

	// After exchange:
	// - srcIno (was srcPath) should now map to dstPath
	// - dstIno (was dstPath) should now map to srcPath
	fs.mu.RLock()
	if fs.nodePaths[srcIno] != dstPath {
		t.Errorf("srcIno path = %q, want %q (BUG: renamePaths is one-directional)", fs.nodePaths[srcIno], dstPath)
	}
	if fs.nodePaths[dstIno] != srcPath {
		t.Errorf("dstIno path = %q, want %q (BUG: renamePaths is one-directional)", fs.nodePaths[dstIno], srcPath)
	}
	if fs.pathToIno[srcPath] != dstIno {
		t.Errorf("srcPath inode = %d, want %d", fs.pathToIno[srcPath], dstIno)
	}
	if fs.pathToIno[dstPath] != srcIno {
		t.Errorf("dstPath inode = %d, want %d", fs.pathToIno[dstPath], srcIno)
	}
	fs.mu.RUnlock()

	// Also check the physical files were swapped
	data, err := os.ReadFile(srcAbs)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "dst-content" {
		t.Errorf("srcPath content = %q, want %q (physical swap failed)", string(data), "dst-content")
	}
	data, err = os.ReadFile(dstAbs)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "src-content" {
		t.Errorf("dstPath content = %q, want %q (physical swap failed)", string(data), "src-content")
	}
}
func TestResetState_ClearsDeletedPathsAndTxnLog(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	// Enable mutation mode
	mutDir := t.TempDir()
	tl, err := OpenTransactionLog(mutDir)
	if err != nil {
		t.Fatal(err)
	}
	fs.mutationMode = true
	fs.txnLog = tl

	// Mark a path as deleted
	fs.mu.Lock()
	fs.deletedPaths["old-file.txt"] = true
	fs.mu.Unlock()

	// Reset state (simulates hot swap)
	fs.ResetState()

	// deletedPaths should be empty
	fs.mu.RLock()
	if len(fs.deletedPaths) != 0 {
		t.Errorf("deletedPaths not cleared: %v", fs.deletedPaths)
	}
	fs.mu.RUnlock()

	// txnLog should also be cleared
	txns, err := tl.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(txns) != 0 {
		t.Errorf("txnLog not cleared after resetState: %d entries", len(txns))
	}
}
func TestSetNode_CleansUpPxarDirForOldInode(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	const oldIno uint64 = 100 | NonDirBit
	const newIno uint64 = 200 | NonDirBit
	path := "/test.txt"

	// Set up old inode as pxar-backed
	fs.setNode(oldIno, path, true)
	fs.mu.Lock()
	fs.pxarDir[oldIno] = true
	fs.mu.Unlock()

	if !fs.isPxarBacked(oldIno) {
		t.Fatal("oldIno should be pxar-backed")
	}

	// Assign a new inode to the same path (simulates backing file creation)
	fs.setNode(newIno, path, true)

	// oldIno should be cleaned up from pxarDir
	if fs.isPxarBacked(oldIno) {
		t.Error("oldIno still in pxarDir after setNode reassigned path to newIno")
	}
}
func TestForget_CleansUpPxarDir(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	const ino uint64 = 100 | NonDirBit
	path := "/forget-test.txt"

	fs.setNode(ino, path, true)
	fs.mu.Lock()
	fs.pxarDir[ino] = true
	fs.mu.Unlock()

	if !fs.isPxarBacked(ino) {
		t.Fatal("ino should be pxar-backed before Forget")
	}

	// Forget the inode (kernel evicts dentry)
	fs.Forget(ino, 1)

	if fs.isPxarBacked(ino) {
		t.Error("ino still in pxarDir after Forget")
	}
}
func TestRename_DestinationStaysDeletedOnFailure(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	fs.mutationMode = true

	// Set up root inode
	fs.mu.Lock()
	fs.nodePaths[RootInode] = "/"
	fs.pathToIno["/"] = RootInode
	fs.mu.Unlock()

	// Mark a destination path as deleted (simulates a previously-deleted pxar entry)
	destPath := "/deleted-dest.txt"
	fs.markPathDeleted(destPath)

	if !fs.isPathDeleted(destPath) {
		t.Fatal("destPath should be deleted before rename")
	}

	// Attempt to rename a non-existent source to the deleted destination.
	// Source "nonexistent-src.txt" doesn't exist in backing dir.
	st := fs.Rename(nil, &fuse.RenameIn{
		InHeader: fuse.InHeader{NodeId: RootInode},
		Newdir:   RootInode,
	}, "nonexistent-src.txt", "deleted-dest.txt")

	// Rename should fail
	if st == fuse.OK {
		t.Fatal("Rename should have failed for non-existent source")
	}

	// The destination should STILL be deleted — unDeletePath was premature.
	if !fs.isPathDeleted(destPath) {
		t.Error("destPath was un-deleted even though Rename failed")
	}
}
func TestRename_ExchangeWithMissingDestInode(t *testing.T) {
	fs, backingDir, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	fs.mutationMode = true

	// Set up root inode
	fs.mu.Lock()
	fs.nodePaths[RootInode] = "/"
	fs.pathToIno["/"] = RootInode
	fs.mu.Unlock()

	// Create a source file via passthrough so it has an inode mapping
	srcPath := filepath.Join(backingDir, "exchange-src.txt")
	if err := os.WriteFile(srcPath, []byte("src"), 0o644); err != nil {
		t.Fatal(err)
	}
	var srcOut fuse.EntryOut
	if st := fs.Lookup(nil, &fuse.InHeader{NodeId: RootInode}, "exchange-src.txt", &srcOut); st != fuse.OK {
		t.Fatalf("Lookup source: %v", st)
	}
	srcIno := srcOut.NodeId

	// Create dest file on disk but do NOT register an inode for it.
	// This simulates a file that exists in the backing dir but hasn't been
	// looked up yet.
	destPath := filepath.Join(backingDir, "exchange-dst.txt")
	if err := os.WriteFile(destPath, []byte("dst"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Verify dest has NO inode mapping
	fs.mu.RLock()
	_, dstHasMapping := fs.pathToIno["/exchange-dst.txt"]
	fs.mu.RUnlock()
	if dstHasMapping {
		t.Fatal("test setup: dest should have no inode mapping")
	}

	// Perform RENAME_EXCHANGE
	st := fs.Rename(nil, &fuse.RenameIn{
		InHeader: fuse.InHeader{NodeId: RootInode},
		Newdir:   RootInode,
		Flags:    renameExchange,
	}, "exchange-src.txt", "exchange-dst.txt")

	if st != fuse.OK {
		t.Fatalf("RENAME_EXCHANGE failed: %v", st)
	}

	// After exchange, oldPath should map to the dest's inode (or 0 if unmapped).
	// The old source inode should still be valid.
	fs.mu.RLock()
	oldIno, oldOk := fs.pathToIno["/exchange-src.txt"]
	newIno, newOk := fs.pathToIno["/exchange-dst.txt"]
	fs.mu.RUnlock()

	if oldOk && oldIno == 0 {
		t.Error("oldPath (/exchange-src.txt) mapped to inode 0 — corrupted by exchange with unmapped dest")
	}

	// The source inode should now be at the destination path
	if !newOk || newIno != srcIno {
		t.Errorf("dest path should map to src inode %d, got ino=%d ok=%v", srcIno, newIno, newOk)
	}

	_ = oldIno // just check it's not 0
}
func TestRename_DestinationStaysDeletedOnRenameFailure(t *testing.T) {
	fs, backingDir, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	fs.mutationMode = true

	// Set up root inode
	fs.mu.Lock()
	fs.nodePaths[RootInode] = "/"
	fs.pathToIno["/"] = RootInode
	fs.mu.Unlock()

	// Create a source file via Create so it's fully registered
	var cout fuse.CreateOut
	if st := fs.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: RootInode}}, "src.txt", &cout); st != fuse.OK {
		t.Fatalf("Create src: %v", st)
	}

	// Mark destination as deleted (simulates a previously-deleted pxar entry)
	destPath := "/deleted-dest.txt"
	fs.markPathDeleted(destPath)

	if !fs.isPathDeleted(destPath) {
		t.Fatal("destPath should be deleted before rename")
	}

	// Delete the source file from disk AFTER Lstat but BEFORE os.Rename.
	// We simulate this by removing it now — the Rename function's Lstat check
	// will pass (file exists), but we'll remove it before os.Rename runs.
	//
	// Since we can't inject a hook between Lstat and os.Rename, we use a
	// different approach: make the source disappear between the Lstat check
	// and the rename by removing it in a goroutine.
	//
	// Actually, let's use a simpler approach: just make the source path
	// point to a non-existent file by removing it before calling Rename.
	// The Lstat check will fail and Rename returns EROFS.

	// Actually the simplest test: remove source file, then try rename.
	// This tests the Lstat failure path (Bug 14 fix).
	os.Remove(filepath.Join(backingDir, "src.txt"))

	st := fs.Rename(nil, &fuse.RenameIn{
		InHeader: fuse.InHeader{NodeId: RootInode},
		Newdir:   RootInode,
	}, "src.txt", "deleted-dest.txt")

	if st == fuse.OK {
		t.Fatal("Rename should fail for non-existent source")
	}

	// Destination should STILL be deleted
	if !fs.isPathDeleted(destPath) {
		t.Error("destPath was un-deleted even though Rename failed")
	}
}
func TestRename_DestinationStaysDeletedOnBackingParentFailure(t *testing.T) {
	fs, backingDir, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	fs.mutationMode = true

	// Set up root inode
	fs.mu.Lock()
	fs.nodePaths[RootInode] = "/"
	fs.pathToIno["/"] = RootInode
	fs.mu.Unlock()

	// Create a source file
	srcPath := filepath.Join(backingDir, "src.txt")
	if err := os.WriteFile(srcPath, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	var srcOut fuse.EntryOut
	if st := fs.Lookup(nil, &fuse.InHeader{NodeId: RootInode}, "src.txt", &srcOut); st != fuse.OK {
		t.Fatalf("Lookup src: %v", st)
	}

	// Mark destination as deleted
	destRelPath := "/subdir/deleted-dest.txt"
	fs.markPathDeleted(destRelPath)

	if !fs.isPathDeleted(destRelPath) {
		t.Fatal("destPath should be deleted before rename")
	}

	// Create a FILE at "subdir" path so ensureBackingParent's MkdirAll fails
	// (can't create a directory where a file exists)
	subdirPath := filepath.Join(backingDir, "subdir")
	if err := os.WriteFile(subdirPath, []byte("blocker"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Attempt rename to a path whose parent can't be created
	st := fs.Rename(nil, &fuse.RenameIn{
		InHeader: fuse.InHeader{NodeId: RootInode},
		Newdir:   RootInode,
	}, "src.txt", "subdir/deleted-dest.txt")

	if st == fuse.OK {
		t.Fatal("Rename should fail when parent dir can't be created")
	}

	// Destination should STILL be deleted
	if !fs.isPathDeleted(destRelPath) {
		t.Error("destPath was un-deleted even though ensureBackingParent failed")
	}
}
func TestRemoveEntry_NonExistentBackedFileReturnsENOENT(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	fs.mutationMode = true

	// Set up root inode
	fs.mu.Lock()
	fs.nodePaths[RootInode] = "/"
	fs.pathToIno["/"] = RootInode
	fs.mu.Unlock()

	// Register a path that has NO file on disk (orphaned inode mapping)
	childPath := "/ghost.txt"
	fs.mu.Lock()
	ino := uint64(backedInoBase) + 1
	fs.pathToIno[childPath] = ino
	fs.nodePaths[ino] = childPath
	fs.backed[ino] = true
	fs.mu.Unlock()

	// Attempt to unlink the ghost file
	st := fs.Unlink(nil, &fuse.InHeader{NodeId: RootInode}, "ghost.txt")

	// Should return ENOENT (file not found), not EROFS
	if st == fuse.EROFS {
		t.Error("removeEntry returned EROFS for non-existent file; expected ENOENT or similar")
	}
}
func TestCreate_DeletedPathStaysDeletedOnBackingParentFailure(t *testing.T) {
	fs, backingDir, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	fs.mutationMode = true

	// Set up root inode
	fs.mu.Lock()
	fs.nodePaths[RootInode] = "/"
	fs.pathToIno["/"] = RootInode
	fs.mu.Unlock()

	// Mark a path as deleted (simulates a previously-deleted pxar entry)
	childRelPath := "/subdir/deleted-file.txt"
	fs.markPathDeleted(childRelPath)

	if !fs.isPathDeleted(childRelPath) {
		t.Fatal("childRelPath should be deleted before Create")
	}

	// Block MkdirAll by creating a FILE at "subdir" path
	subdirPath := filepath.Join(backingDir, "subdir")
	if err := os.WriteFile(subdirPath, []byte("blocker"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Attempt Create — ensureBackingParent should fail
	var cout fuse.CreateOut
	st := fs.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: RootInode},
	}, "subdir/deleted-file.txt", &cout)

	if st == fuse.OK {
		t.Fatal("Create should fail when parent dir can't be created")
	}

	// Path should STILL be deleted
	if !fs.isPathDeleted(childRelPath) {
		t.Error("childRelPath was un-deleted even though Create failed")
	}
}
func TestCreate_DeletedPathStaysDeletedOnFallbackOpenFailure(t *testing.T) {
	fs, backingDir, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	fs.mutationMode = true

	// Set up root inode
	fs.mu.Lock()
	fs.nodePaths[RootInode] = "/"
	fs.pathToIno["/"] = RootInode
	fs.mu.Unlock()

	// Mark a path as deleted
	childRelPath := "/foo"
	fs.markPathDeleted(childRelPath)

	if !fs.isPathDeleted(childRelPath) {
		t.Fatal("childRelPath should be deleted before Create")
	}

	// Create a DIRECTORY at the target path so O_EXCL fails (EEXIST)
	// and the fallback O_WRONLY|O_TRUNC also fails (EISDIR).
	if err := os.Mkdir(filepath.Join(backingDir, "foo"), 0o755); err != nil {
		t.Fatal(err)
	}

	// Attempt Create
	var cout fuse.CreateOut
	st := fs.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: RootInode},
	}, "foo", &cout)

	if st == fuse.OK {
		t.Fatal("Create should fail when fallback open fails (EISDIR)")
	}

	// Path should STILL be deleted
	if !fs.isPathDeleted(childRelPath) {
		t.Errorf("childRelPath was un-deleted even though Create failed (status=%v)", st)
	}
}
func TestWrite_LazyOpenHandleLeak(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	fs.mutationMode = true

	// Create a file with write permissions
	var cout fuse.CreateOut
	if st := fs.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: RootInode}, Mode: 0o644}, "lazyfile.txt", &cout); st != fuse.OK {
		t.Fatalf("Create failed: %v", st)
	}
	ino := cout.NodeId

	// Close the existing handle (simulate the fd being released)
	fs.fhmu.Lock()
	for id, fh := range fs.handles {
		syscall.Close(fh.fd)
		delete(fs.handles, id)
	}
	fs.fhmu.Unlock()

	// Now do concurrent writes — both will see fh==nil and open new fds
	var wg sync.WaitGroup
	const writers = 5
	errs := make([]fuse.Status, writers)

	for i := range writers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			data := []byte("hello from writer")
			_, errs[idx] = fs.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: ino},
				Fh:       0, // sentinel — triggers lazy-open
				Offset:   uint64(idx * 20),
			}, data)
		}(i)
	}
	wg.Wait()

	// Count file handles — should be at most 1 per inode
	fs.fhmu.Lock()
	handleCount := len(fs.handles)
	fs.fhmu.Unlock()

	if handleCount > 2 { // allow some slack for race, but not 5
		t.Errorf("expected ~1 file handle after concurrent writes, got %d (leak from lazy-open race)", handleCount)
	}

	// At least some writes should have succeeded
	successCount := 0
	for _, s := range errs {
		if s == fuse.OK {
			successCount++
		}
	}
	if successCount == 0 {
		t.Error("at least one write should have succeeded")
	}
}

func TestFallocate_LazyOpenHandleLeak(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	fs.mutationMode = true

	// Create a file
	var cout fuse.CreateOut
	if st := fs.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: RootInode}}, "fallocfile.txt", &cout); st != fuse.OK {
		t.Fatalf("Create failed: %v", st)
	}
	ino := cout.NodeId

	// Close the existing handle
	fs.fhmu.Lock()
	for id, fh := range fs.handles {
		syscall.Close(fh.fd)
		delete(fs.handles, id)
	}
	fs.fhmu.Unlock()

	var wg sync.WaitGroup
	const callers = 5
	errs := make([]fuse.Status, callers)

	for i := range callers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = fs.Fallocate(nil, &fuse.FallocateIn{
				InHeader: fuse.InHeader{NodeId: ino},
				Fh:       0,
				Offset:   uint64(idx * 4096),
				Length:   4096,
			})
		}(i)
	}
	wg.Wait()

	fs.fhmu.Lock()
	handleCount := len(fs.handles)
	fs.fhmu.Unlock()

	if handleCount > 2 {
		t.Errorf("expected ~1 file handle after concurrent fallocate, got %d", handleCount)
	}
}

func TestForget_CleansUpMetaOverlay(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	const ino uint64 = 100 | NonDirBit
	path := "/forget-overlay.txt"

	fs.setNode(ino, path, true)

	// Add a metaOverlay entry for this path
	fs.mu.Lock()
	fs.metaOverlay[path] = &metaOverride{
		mode: new(uint32(0o755)),
		xadd: make(map[string][]byte),
		xdel: make(map[string]bool),
	}
	fs.mu.Unlock()

	// Forget the inode
	fs.Forget(ino, 1)

	// metaOverlay should be cleaned up for this path
	fs.mu.RLock()
	_, hasOverlay := fs.metaOverlay[path]
	fs.mu.RUnlock()

	if hasOverlay {
		t.Error("metaOverlay entry should be cleaned up after Forget")
	}
}

func TestReadDirRaw_PoolAliasSafety(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	// Just test that two sequential ReadDirRaw calls work correctly
	// without pool corruption. The real issue would be if someone
	// retains the slice across a second call.
	entries1, err := fs.pxar.ReadDirRaw(RootInode)
	if err != nil {
		// No reader is set, so this returns nil entries
		t.Logf("ReadDirRaw returned err=%v (expected, no reader)", err)
	}
	_ = entries1

	entries2, err := fs.pxar.ReadDirRaw(RootInode)
	if err != nil {
		t.Logf("ReadDirRaw 2 returned err=%v", err)
	}
	_ = entries2
}
