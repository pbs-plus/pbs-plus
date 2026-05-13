package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

//go:fix inline
func uint32Ptr(v uint32) *uint32 { return new(v) }

func TestTransactionLogRecord(t *testing.T) {
	dir := t.TempDir()
	tl, err := OpenTransactionLog(dir)
	if err != nil {
		t.Fatalf("OpenTransactionLog: %v", err)
	}
	defer func() { _ = tl.Close() }()

	id, err := tl.Record(TxnDelete, "/foo/bar.txt")
	if err != nil {
		t.Fatalf("Record: %v", err)
	}
	if id != 1 {
		t.Errorf("first ID = %d, want 1", id)
	}

	id2, err := tl.Record(TxnModify, "/baz/qux.txt")
	if err != nil {
		t.Fatalf("Record: %v", err)
	}
	if id2 != 2 {
		t.Errorf("second ID = %d, want 2", id2)
	}
}

func TestTransactionLogReadAll(t *testing.T) {
	dir := t.TempDir()
	tl, err := OpenTransactionLog(dir)
	if err != nil {
		t.Fatalf("OpenTransactionLog: %v", err)
	}
	defer func() { _ = tl.Close() }()

	_, _ = tl.Record(TxnDelete, "/a")
	_, _ = tl.RecordRename("/b", "/c")
	_, _ = tl.RecordSetAttr("/d", &TxnAttrs{Mode: uint32Ptr(0o755)})

	txns, err := tl.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(txns) != 3 {
		t.Fatalf("len(txns) = %d, want 3", len(txns))
	}

	if txns[0].Type != TxnDelete || txns[0].Path != "/a" {
		t.Errorf("txn[0] = %+v, want DELETE /a", txns[0])
	}
	if txns[1].Type != TxnRename || txns[1].Path != "/b" || txns[1].NewPath != "/c" {
		t.Errorf("txn[1] = %+v, want RENAME /b -> /c", txns[1])
	}
	if txns[2].Type != TxnSetAttr || txns[2].Attrs == nil || txns[2].Attrs.Mode == nil || *txns[2].Attrs.Mode != 0o755 {
		t.Errorf("txn[2] = %+v, want SETATTR /d mode=0755", txns[2])
	}
}

func TestTransactionLogClear(t *testing.T) {
	dir := t.TempDir()
	tl, err := OpenTransactionLog(dir)
	if err != nil {
		t.Fatalf("OpenTransactionLog: %v", err)
	}
	defer func() { _ = tl.Close() }()

	_, _ = tl.Record(TxnDelete, "/x")
	_, _ = tl.Record(TxnDelete, "/y")

	if err := tl.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}

	txns, err := tl.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll after clear: %v", err)
	}
	if len(txns) != 0 {
		t.Errorf("len(txns) after clear = %d, want 0", len(txns))
	}

	// Should be able to record again after clear
	id, err := tl.Record(TxnDelete, "/z")
	if err != nil {
		t.Fatalf("Record after clear: %v", err)
	}
	if id != 1 {
		t.Errorf("ID after clear = %d, want 1", id)
	}
}

func TestTransactionLogJSONLFormat(t *testing.T) {
	dir := t.TempDir()
	tl, err := OpenTransactionLog(dir)
	if err != nil {
		t.Fatalf("OpenTransactionLog: %v", err)
	}

	_, _ = tl.Record(TxnDelete, "/test")
	_ = tl.Close()

	// Read raw file and verify it's valid JSONL
	data, err := os.ReadFile(filepath.Join(dir, "transactions.jsonl"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	var txn Txn
	if err := json.Unmarshal(data[:len(data)-1], &txn); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if txn.Type != TxnDelete || txn.Path != "/test" || txn.ID != 1 {
		t.Errorf("txn = %+v, want DELETE /test id=1", txn)
	}
}

func TestTransactionLogAppend(t *testing.T) {
	dir := t.TempDir()

	// Create and write some entries
	tl1, err := OpenTransactionLog(dir)
	if err != nil {
		t.Fatalf("Open 1: %v", err)
	}
	_, _ = tl1.Record(TxnDelete, "/first")
	_ = tl1.Close()

	// Re-open: should append, not overwrite
	tl2, err := OpenTransactionLog(dir)
	if err != nil {
		t.Fatalf("Open 2: %v", err)
	}
	_, _ = tl2.Record(TxnDelete, "/second")
	_ = tl2.Close()

	// Verify both entries exist
	tl3, err := OpenTransactionLog(dir)
	if err != nil {
		t.Fatalf("Open 3: %v", err)
	}
	defer func() { _ = tl3.Close() }()

	txns, err := tl3.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(txns) != 2 {
		t.Errorf("len(txns) = %d, want 2", len(txns))
	}
	if txns[0].Path != "/first" {
		t.Errorf("txn[0].Path = %q, want /first", txns[0].Path)
	}
	if txns[1].Path != "/second" {
		t.Errorf("txn[1].Path = %q, want /second", txns[1].Path)
	}
	// IDs should be sequential even across reopens
	if txns[0].ID != 1 || txns[1].ID != 2 {
		t.Errorf("IDs = %d, %d, want 1, 2", txns[0].ID, txns[1].ID)
	}
}

func TestTransactionTypes(t *testing.T) {
	types := []TxnType{TxnDelete, TxnRename, TxnModify, TxnSetAttr}
	for _, typ := range types {
		if typ == "" {
			t.Errorf("empty TxnType")
		}
	}
}

func TestTxnAttrsNilFields(t *testing.T) {
	attrs := &TxnAttrs{}
	data, err := json.Marshal(attrs)
	if err != nil {
		t.Fatalf("marshal empty attrs: %v", err)
	}
	// Nil fields should be omitted
	if string(data) != "{}" {
		t.Errorf("empty attrs = %s, want {}", data)
	}

	attrs2 := &TxnAttrs{
		Mode: uint32Ptr(0o644),
		GID:  uint32Ptr(100),
	}
	data2, err := json.Marshal(attrs2)
	if err != nil {
		t.Fatalf("marshal partial attrs: %v", err)
	}
	var parsed TxnAttrs
	if err := json.Unmarshal(data2, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if parsed.Mode == nil || *parsed.Mode != 0o644 {
		t.Error("Mode not preserved")
	}
	if parsed.UID != nil {
		t.Error("UID should be nil")
	}
	if parsed.GID == nil || *parsed.GID != 100 {
		t.Error("GID not preserved")
	}
}
