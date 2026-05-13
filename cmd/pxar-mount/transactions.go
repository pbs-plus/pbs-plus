package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// TxnType identifies the kind of filesystem mutation.
type TxnType string

const (
	TxnDelete  TxnType = "DELETE"  // file/dir removed (unlink/rmdir)
	TxnRename  TxnType = "RENAME"  // file/dir moved (rename)
	TxnModify  TxnType = "MODIFY"  // file content modified (copy-on-write)
	TxnSetAttr TxnType = "SETATTR" // metadata changed (chmod/chown/utimes/xattr)
)

// Txn represents a single filesystem mutation recorded for later replay.
type Txn struct {
	ID        uint64    `json:"id"`
	Type      TxnType   `json:"type"`
	Path      string    `json:"path"`            // original pxar path (before mutation)
	NewPath   string    `json:"new_path"`        // only for RENAME
	Timestamp int64     `json:"timestamp"`       // unix seconds
	Attrs     *TxnAttrs `json:"attrs,omitempty"` // only for SETATTR
	Backed    bool      `json:"backed"`          // true if materialized to backing dir
}

// TxnAttrs captures metadata changes from a SETATTR operation.
type TxnAttrs struct {
	Mode  *uint32 `json:"mode,omitempty"`
	UID   *uint32 `json:"uid,omitempty"`
	GID   *uint32 `json:"gid,omitempty"`
	Size  *uint64 `json:"size,omitempty"`
	Mtime *int64  `json:"mtime,omitempty"`
}

// TransactionLog manages an append-only JSONL file of mutations.
type TransactionLog struct {
	mu   sync.Mutex
	file *os.File
	buf  *bufio.Writer
	next uint64
	path string
}

// OpenTransactionLog opens or creates the transaction log directory and file.
func OpenTransactionLog(dir string) (*TransactionLog, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create transactions dir: %w", err)
	}

	logPath := filepath.Join(dir, "transactions.jsonl")

	// Count existing entries to set next ID
	var next uint64
	if data, err := os.ReadFile(logPath); err == nil {
		for _, line := range splitLines(data) {
			if len(line) > 0 {
				next++
			}
		}
	}

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open transaction log: %w", err)
	}

	tl := &TransactionLog{
		file: f,
		buf:  bufio.NewWriter(f),
		path: logPath,
		next: next,
	}

	return tl, nil
}

// splitLines splits raw bytes into lines (without trailing newlines).
func splitLines(data []byte) [][]byte {
	var lines [][]byte
	start := 0
	for i, b := range data {
		if b == '\n' {
			if i > start {
				lines = append(lines, data[start:i])
			}
			start = i + 1
		}
	}
	if start < len(data) {
		lines = append(lines, data[start:])
	}
	return lines
}

// Record appends a transaction to the log and returns its ID.
func (tl *TransactionLog) Record(typ TxnType, path string) (uint64, error) {
	return tl.record(&Txn{
		Type:      typ,
		Path:      path,
		Timestamp: time.Now().Unix(),
	})
}

// RecordRename appends a rename transaction.
func (tl *TransactionLog) RecordRename(oldPath, newPath string) (uint64, error) {
	return tl.record(&Txn{
		Type:      TxnRename,
		Path:      oldPath,
		NewPath:   newPath,
		Timestamp: time.Now().Unix(),
	})
}

// RecordSetAttr appends a setattr transaction.
func (tl *TransactionLog) RecordSetAttr(path string, attrs *TxnAttrs) (uint64, error) {
	return tl.record(&Txn{
		Type:      TxnSetAttr,
		Path:      path,
		Attrs:     attrs,
		Timestamp: time.Now().Unix(),
	})
}

func (tl *TransactionLog) record(txn *Txn) (uint64, error) {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	tl.next++
	txn.ID = tl.next

	data, err := json.Marshal(txn)
	if err != nil {
		return 0, fmt.Errorf("marshal transaction: %w", err)
	}

	if _, err := tl.buf.Write(data); err != nil {
		return 0, fmt.Errorf("write transaction: %w", err)
	}
	if err := tl.buf.WriteByte('\n'); err != nil {
		return 0, fmt.Errorf("write newline: %w", err)
	}
	if err := tl.buf.Flush(); err != nil {
		return 0, fmt.Errorf("flush transaction log: %w", err)
	}

	return txn.ID, nil
}

// Close flushes and closes the transaction log.
func (tl *TransactionLog) Close() error {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	if tl.buf != nil {
		_ = tl.buf.Flush()
	}
	if tl.file != nil {
		return tl.file.Close()
	}
	return nil
}

// ReadAll reads all transactions from the log. Used during commit to
// apply mutations to the new snapshot.
func (tl *TransactionLog) ReadAll() ([]Txn, error) {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	// Flush any buffered writes first
	if tl.buf != nil {
		_ = tl.buf.Flush()
	}

	f, err := os.Open(tl.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var txns []Txn
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var txn Txn
		if err := json.Unmarshal(line, &txn); err != nil {
			continue // skip malformed lines
		}
		txns = append(txns, txn)
	}
	return txns, scanner.Err()
}

// Clear truncates the transaction log after a successful commit.
func (tl *TransactionLog) Clear() error {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	if tl.buf != nil {
		_ = tl.buf.Flush()
	}

	// Truncate the file. Open the new file BEFORE closing the old one
	// so we never leave tl.file/tl.buf in a nil state on error.
	f, err := os.Create(tl.path)
	if err != nil {
		return err
	}

	if tl.file != nil {
		_ = tl.file.Close()
	}

	tl.file = f
	tl.buf = bufio.NewWriter(f)
	tl.next = 0
	return nil
}
