//go:build windows

package vssfs

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/go-git/go-billy/v5"
	nfs "github.com/willscott/go-nfs"
)

const (
	RootHandleID = uint64(0) // Reserved ID for root directory
)

type VSSIDHandler struct {
	nfs.Handler
	vssFS         *VSSFS
	handlesDb     *badger.DB
	handlesDbPath string
}

func NewVSSIDHandler(vssFS *VSSFS, underlyingHandler nfs.Handler) (
	*VSSIDHandler, error,
) {
	// Construct a directory path for the database
	dbPath := filepath.Join(os.TempDir(),
		fmt.Sprintf("/pbs-vssfs/handlers-%s-%d",
			vssFS.snapshot.DriveLetter, time.Now().Unix()))
	err := os.MkdirAll(dbPath, 0755)
	if err != nil {
		return nil, err
	}

	opts := badger.DefaultOptions(dbPath).
		WithLogger(nil)

	opts.NumMemtables = 1
	opts.NumLevelZeroTables = 1
	opts.NumLevelZeroTablesStall = 2
	opts.NumCompactors = 1
	opts.BaseTableSize = 2 << 20     // 2 MB.
	opts.ValueLogFileSize = 16 << 20 // 16 MB.

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &VSSIDHandler{
		Handler:       underlyingHandler,
		vssFS:         vssFS,
		handlesDb:     db,
		handlesDbPath: dbPath,
	}, nil
}

func (h *VSSIDHandler) getHandle(key uint64) (string, bool) {
	// Serialize key as 8-byte big-endian.
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, key)

	var result string
	err := h.handlesDb.View(func(txn *badger.Txn) error {
		item, err := txn.Get(k)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			result = string(val)
			return nil
		})
	})
	// If key is not found or an error occurs, return false.
	if err == badger.ErrKeyNotFound || result == "" {
		return "", false
	} else if err != nil {
		return "", false
	}
	return result, true
}

func (h *VSSIDHandler) storeHandle(key uint64, path string) error {
	// Serialize key as 8-byte big-endian.
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, key)

	return h.handlesDb.Update(func(txn *badger.Txn) error {
		return txn.Set(k, []byte(path))
	})
}

func (h *VSSIDHandler) ToHandle(f billy.Filesystem, path []string) []byte {
	vssFS, ok := f.(*VSSFS)
	if !ok || vssFS != h.vssFS {
		return nil
	}

	// Special-case for the root directory.
	if len(path) == 0 || (len(path) == 1 && path[0] == "") {
		return h.createHandle(RootHandleID, vssFS.Root())
	}

	winPath := filepath.Join(path...)
	fullPath := filepath.Join(vssFS.Root(), winPath)

	info, err := vssFS.Stat(winPath)
	if err != nil {
		return nil
	}

	fileID := info.(*VSSFileInfo).stableID
	return h.createHandle(fileID, fullPath)
}

func (h *VSSIDHandler) createHandle(fileID uint64, fullPath string) []byte {
	// If the handle mapping doesn’t exist, store it.
	if _, exists := h.getHandle(fileID); !exists {
		_ = h.storeHandle(fileID, fullPath)
	}

	// Return the fileID as an 8-byte handle.
	handle := make([]byte, 8)
	binary.BigEndian.PutUint64(handle, fileID)
	return handle
}

func (h *VSSIDHandler) FromHandle(handle []byte) (
	billy.Filesystem, []string, error,
) {
	if len(handle) != 8 {
		return nil, nil, fmt.Errorf("invalid handle length")
	}

	fileID := binary.BigEndian.Uint64(handle)
	if fileID == RootHandleID {
		return h.vssFS, []string{}, nil
	}

	fullPath, exists := h.getHandle(fileID)
	if !exists {
		return nil, nil, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusStale}
	}

	relativePath := strings.TrimPrefix(
		filepath.ToSlash(fullPath),
		filepath.ToSlash(h.vssFS.Root())+"/",
	)

	var parts []string
	if relativePath != "" {
		parts = strings.Split(relativePath, "/")
	}
	return h.vssFS, parts, nil
}

func (h *VSSIDHandler) HandleLimit() int {
	return math.MaxInt
}

func (h *VSSIDHandler) InvalidateHandle(fs billy.Filesystem, handle []byte) error {
	return nil
}

// ClearHandles closes the current BadgerDB instance and then
// completely deletes the underlying database directory.
func (h *VSSIDHandler) ClearHandles() error {
	if err := h.handlesDb.Close(); err != nil {
		return err
	}

	// RemoveAll deletes the directory and any contents.
	if err := os.RemoveAll(h.handlesDbPath); err != nil {
		return err
	}

	h.handlesDb = nil
	h.handlesDbPath = ""
	return nil
}
