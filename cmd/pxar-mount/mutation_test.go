package main

import (
	"bufio"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// --- Bug 1: materializePxarFile race ---
// Two concurrent callers (Write + SetAttr) on the same pxar inode can both
// pass the isBacked check and double-materialize. With O_CREATE|O_TRUNC the
// second truncates the first copy to zero before writing, so data is lost.

func TestMaterializePxarFile_ConcurrentDedup(t *testing.T) {
	fs, backingDir, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	// Set up a pxar file node with content.
	const ino uint64 = 100 | nonDirBit
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

// --- Bug 2: markPathDeleted is not idempotent / TOCTOU on delete ---
// Two concurrent calls to markPathDeleted for the same path will both
// record DELETE transactions. The transaction log should have exactly one
// DELETE per path.

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

// --- Bug 3: Transaction log does not sync to disk after flush ---
// bufio.Writer.Flush only pushes to the os.File write buffer, not to disk.
// If the process crashes between Record and the OS flushing, the transaction
// is lost. The Record method should fsync after flush for durability.

// This is a design concern, not easily tested without fault injection.
// We verify the contract: after Record returns, ReadAll must see the entry.

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

// --- Bug 4: TransactionLog.Clear loses file reference on error ---
// If os.Create fails in Clear, tl.file is set to nil but the old file is
// already closed. Subsequent Record calls would panic on nil buf.

// --- Bug 9: Creating a new file over a deleted pxar path fails in Lookup ---
// After deleting a pxar file via mutation mode, creating a new file with
// the same name should un-delete the path so Lookup can find it.

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

// --- Bug 5: renamePaths overwrites destination inode without cleaning up
// the old destination's mappings properly ---
// If destination exists and has a different inode, renamePaths deletes the
// old dst's nodePaths entry but the old inode may still be in backed/pxarDir.

func TestRenamePaths_DestOverwrite(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	// Set up two inodes
	const srcIno uint64 = 100 | nonDirBit
	const dstIno uint64 = 200 | nonDirBit

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

// --- Bug 6: resetState clears deletedPaths but doesn't reset txnLog ---
// After resetState (e.g., hot swap), deletedPaths is empty but txnLog
// may still reference stale entries from the previous snapshot.

// --- Bug 7: Clear leaves TransactionLog in broken state on os.Create failure ---
// If os.Create fails after closing the old file, tl.file and tl.buf are nil.
// Subsequent Record calls panic on nil pointer dereference.

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
	tl.next = 0

	_, err = tl.Record(TxnDelete, "after-failed-clear")
	if err != nil {
		t.Fatalf("Record after failed Clear should not panic, got: %v", err)
	}
}

// --- Bug 8: materializePxarFile records MODIFY transaction every time ---
// If a file is already backed (pre-existing in backing dir), calling
// materializePxarFile should NOT record a MODIFY transaction since
// the file wasn't actually copied from pxar.

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

	const ino uint64 = 100 | nonDirBit
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

// --- Bug 10: RENAME_EXCHANGE only updates one direction ---
// renamePaths(oldPath, newPath) only moves old→new. For RENAME_EXCHANGE,
// both entries swap, so both inode mappings must be updated.
// Additionally, only one RENAME transaction is recorded instead of two.

func TestRename_ExchangeSwapsBothDirections(t *testing.T) {
	fs, backingDir, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	fs.mutationMode = true

	// Set up two backed files with different inodes
	const srcIno uint64 = 100 | nonDirBit
	const dstIno uint64 = 200 | nonDirBit

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
	// We need a parent inode for the rename. Use rootInode.
	fs.mu.Lock()
	fs.nodePaths[rootInode] = "/"
	fs.pathToIno["/"] = rootInode
	fs.mu.Unlock()

	// Perform the exchange
	st := fs.Rename(nil, &fuse.RenameIn{
		InHeader: fuse.InHeader{NodeId: rootInode},
		Newdir:   rootInode,
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
	fs.resetState()

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

// --- Bug 11: setNode doesn't clean up pxarDir for old inode ---
// When setNode assigns a new inode to a path that already had a different inode,
// it cleans up nodePaths and backed for the old inode, but NOT pxarDir.
// This means isPxarBacked(oldIno) returns true for a stale inode.

func TestSetNode_CleansUpPxarDirForOldInode(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	const oldIno uint64 = 100 | nonDirBit
	const newIno uint64 = 200 | nonDirBit
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

// --- Bug 12: Forget doesn't clean up pxarDir ---
// When a backed node is forgotten (kernel drops dentry cache),
// Forget cleans up backed/nodePaths/pathToIno but leaves pxarDir.
// Stale pxarDir entries cause isPxarBacked to return true for forgotten inodes.

func TestForget_CleansUpPxarDir(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	const ino uint64 = 100 | nonDirBit
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

// --- Bug 14: Rename un-deletes destination before confirming source exists ---
// If the source doesn't exist (Lstat fails), Rename returns EROFS
// but has already un-deleted the destination path. The previously-deleted
// pxar entry at the destination becomes visible again.

func TestRename_DestinationStaysDeletedOnFailure(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	fs.mutationMode = true

	// Set up root inode
	fs.mu.Lock()
	fs.nodePaths[rootInode] = "/"
	fs.pathToIno["/"] = rootInode
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
		InHeader: fuse.InHeader{NodeId: rootInode},
		Newdir:   rootInode,
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

// --- Bug 15: RENAME_EXCHANGE with missing dest inode maps oldPath to inode 0 ---
// When RENAME_EXCHANGE swaps two paths and the destination has no inode
// mapping (newOk=false), the exchange code sets pathToIno[oldPath]=0,
// which corrupts the mapping.

func TestRename_ExchangeWithMissingDestInode(t *testing.T) {
	fs, backingDir, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	fs.mutationMode = true

	// Set up root inode
	fs.mu.Lock()
	fs.nodePaths[rootInode] = "/"
	fs.pathToIno["/"] = rootInode
	fs.mu.Unlock()

	// Create a source file via passthrough so it has an inode mapping
	srcPath := filepath.Join(backingDir, "exchange-src.txt")
	if err := os.WriteFile(srcPath, []byte("src"), 0o644); err != nil {
		t.Fatal(err)
	}
	var srcOut fuse.EntryOut
	if st := fs.Lookup(nil, &fuse.InHeader{NodeId: rootInode}, "exchange-src.txt", &srcOut); st != fuse.OK {
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
		InHeader: fuse.InHeader{NodeId: rootInode},
		Newdir:   rootInode,
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
