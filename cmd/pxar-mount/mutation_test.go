package main

import (
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
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
