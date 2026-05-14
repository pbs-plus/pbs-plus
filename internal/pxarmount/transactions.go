package pxarmount

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fxamacker/cbor/v2"
)

// TxnType identifies the kind of filesystem mutation.
type TxnType string

const (
	TxnDelete      TxnType = "DELETE"
	TxnRename      TxnType = "RENAME"
	TxnModify      TxnType = "MODIFY"
	TxnSetAttr     TxnType = "SETATTR"
	TxnSetXAttr    TxnType = "SETXATTR"
	TxnRemoveXAttr TxnType = "REMXATTR"
)

// Txn represents a single filesystem mutation recorded for later replay.
type Txn struct {
	ID        uint64    `cbor:"1,keyasint"`
	Type      TxnType   `cbor:"2,keyasint"`
	Path      string    `cbor:"3,keyasint"`
	NewPath   string    `cbor:"4,keyasint,omitempty"`
	Timestamp int64     `cbor:"5,keyasint"`
	Attrs     *TxnAttrs `cbor:"6,keyasint,omitempty"`
	Backed    bool      `cbor:"7,keyasint"`
	XAttr     *TxnXAttr `cbor:"9,keyasint,omitempty"`
}

// TxnAttrs captures metadata changes from a SETATTR operation.
type TxnAttrs struct {
	Mode  *uint32 `cbor:"1,keyasint,omitempty"`
	UID   *uint32 `cbor:"2,keyasint,omitempty"`
	GID   *uint32 `cbor:"3,keyasint,omitempty"`
	Size  *uint64 `cbor:"4,keyasint,omitempty"`
	Mtime *int64  `cbor:"5,keyasint,omitempty"`
	Atime *int64  `cbor:"8,keyasint,omitempty"`
}

// TxnXAttr captures xattr changes for SETXATTR/REMOVEXATTR operations.
type TxnXAttr struct {
	Name  string `cbor:"1,keyasint"`
	Value []byte `cbor:"2,keyasint,omitempty"`
}

// TransactionLog manages an append-only CBOR file of mutations.
// Each record is a self-contained CBOR data item, enabling streaming
// decode without length-prefix framing.
type TransactionLog struct {
	mu   sync.Mutex
	file *os.File
	enc  *cbor.Encoder
	buf  *bufio.Writer
	next uint64
	path string
}

// OpenTransactionLog opens or creates the transaction log.
func OpenTransactionLog(dir string) (*TransactionLog, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create transactions dir: %w", err)
	}

	logPath := filepath.Join(dir, "transactions.cbor")

	var next uint64
	if f, err := os.Open(logPath); err == nil {
		dec := cbor.NewDecoder(f)
		for {
			var txn Txn
			if err := dec.Decode(&txn); err != nil {
				break
			}
			next++
		}
		f.Close()
	}

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open transaction log: %w", err)
	}

	buf := bufio.NewWriterSize(f, 64*1024)
	return &TransactionLog{
		file: f,
		buf:  buf,
		enc:  cbor.NewEncoder(buf),
		path: logPath,
		next: next,
	}, nil
}

// Record appends a transaction to the log.
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

// RecordSetXAttr appends a setxattr transaction.
func (tl *TransactionLog) RecordSetXAttr(path, name string, value []byte) (uint64, error) {
	return tl.record(&Txn{
		Type:      TxnSetXAttr,
		Path:      path,
		XAttr:     &TxnXAttr{Name: name, Value: value},
		Timestamp: time.Now().Unix(),
	})
}

// RecordRemoveXAttr appends a removexattr transaction.
func (tl *TransactionLog) RecordRemoveXAttr(path, name string) (uint64, error) {
	return tl.record(&Txn{
		Type:      TxnRemoveXAttr,
		Path:      path,
		XAttr:     &TxnXAttr{Name: name},
		Timestamp: time.Now().Unix(),
	})
}

func (tl *TransactionLog) record(txn *Txn) (uint64, error) {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	tl.next++
	txn.ID = tl.next

	if err := tl.enc.Encode(txn); err != nil {
		return 0, fmt.Errorf("encode transaction: %w", err)
	}

	return txn.ID, nil
}

// Sync flushes buffered transactions to disk and fsyncs the file.
func (tl *TransactionLog) Sync() error {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	return tl.syncLocked()
}

func (tl *TransactionLog) syncLocked() error {
	if tl.buf != nil {
		if err := tl.buf.Flush(); err != nil {
			return err
		}
	}
	if tl.file != nil {
		return tl.file.Sync()
	}
	return nil
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

// ReadAll reads all transactions from the log.
func (tl *TransactionLog) ReadAll() ([]Txn, error) {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	if tl.buf != nil {
		_ = tl.buf.Flush()
	}
	if tl.file != nil {
		_ = tl.file.Sync()
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
	dec := cbor.NewDecoder(f)
	for {
		var txn Txn
		if err := dec.Decode(&txn); err != nil {
			break
		}
		txns = append(txns, txn)
	}
	return txns, nil
}

// Clear truncates the transaction log after a successful commit.
func (tl *TransactionLog) Clear() error {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	if tl.buf != nil {
		_ = tl.buf.Flush()
	}

	f, err := os.Create(tl.path)
	if err != nil {
		return err
	}

	if tl.file != nil {
		_ = tl.file.Close()
	}

	tl.file = f
	tl.buf = bufio.NewWriter(f)
	tl.enc = cbor.NewEncoder(tl.buf)
	tl.next = 0
	return nil
}
