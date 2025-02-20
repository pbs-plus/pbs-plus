//go:build windows

package vssfs

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-git/go-billy/v5"
	nfs "github.com/willscott/go-nfs"
	bolt "go.etcd.io/bbolt"
)

const (
	RootHandleID = uint64(0) // Reserved ID for root directory
)

var handlesBucket = []byte("handles")

type VSSIDHandler struct {
	nfs.Handler
	vssFS     *VSSFS
	handlesDb *bolt.DB
}

func NewVSSIDHandler(vssFS *VSSFS, underlyingHandler nfs.Handler) (
	*VSSIDHandler, error,
) {
	dbPath := filepath.Join(os.TempDir(), "/pbs-vssfs/handlers.db")
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(handlesBucket)
		return err
	})
	if err != nil {
		return nil, err
	}

	return &VSSIDHandler{
		Handler:   underlyingHandler,
		vssFS:     vssFS,
		handlesDb: db,
	}, nil
}

func (h *VSSIDHandler) getHandle(key uint64) (string, bool) {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, key)

	var path []byte
	err := h.handlesDb.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(handlesBucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", handlesBucket)
		}
		path = bucket.Get(k)
		return nil
	})
	if err != nil || path == nil {
		return "", false
	}
	return string(path), true
}

func (h *VSSIDHandler) storeHandle(key uint64, path string) error {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, key)

	return h.handlesDb.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(handlesBucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", handlesBucket)
		}
		return bucket.Put(k, []byte(path))
	})
}

func (h *VSSIDHandler) ToHandle(f billy.Filesystem, path []string) []byte {
	vssFS, ok := f.(*VSSFS)
	if !ok || vssFS != h.vssFS {
		return nil
	}

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
	if _, exists := h.getHandle(fileID); !exists {
		_ = h.storeHandle(fileID, fullPath)
	}

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

func (h *VSSIDHandler) ClearHandles() {
	_ = h.handlesDb.Update(func(tx *bolt.Tx) error {
		_ = tx.DeleteBucket(handlesBucket)
		_, err := tx.CreateBucket(handlesBucket)
		return err
	})
}
