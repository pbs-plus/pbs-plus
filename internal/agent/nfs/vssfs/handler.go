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

	"github.com/cockroachdb/pebble"
	"github.com/go-git/go-billy/v5"
	nfs "github.com/willscott/go-nfs"
)

const (
	RootHandleID = uint64(0) // Reserved ID for root directory
)

type VSSIDHandler struct {
	nfs.Handler
	vssFS         *VSSFS
	handlesDb     *pebble.DB
	handlesDbPath string
}

func NewVSSIDHandler(vssFS *VSSFS, underlyingHandler nfs.Handler) (*VSSIDHandler, error) {
	// Create a unique directory for the Pebble DB.
	dbPath := filepath.Join(os.TempDir(),
		fmt.Sprintf("/pbs-vssfs/handlers-%s-%d", vssFS.snapshot.DriveLetter, time.Now().Unix()))
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, err
	}

	opts := &pebble.Options{
		Logger: nil,
	}

	db, err := pebble.Open(dbPath, opts)
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
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, key)

	value, closer, err := h.handlesDb.Get(k)
	if err == pebble.ErrNotFound {
		return "", false
	} else if err != nil {
		return "", false
	}

	result := string(append([]byte(nil), value...))
	closer.Close()
	return result, true
}

func (h *VSSIDHandler) storeHandle(key uint64, path string) error {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, key)
	return h.handlesDb.Set(k, []byte(path), nil)
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

func (h *VSSIDHandler) FromHandle(handle []byte) (billy.Filesystem, []string, error) {
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

func (h *VSSIDHandler) ClearHandles() error {
	if err := h.handlesDb.Close(); err != nil {
		return err
	}

	if err := os.RemoveAll(h.handlesDbPath); err != nil {
		return err
	}

	h.handlesDb = nil
	h.handlesDbPath = ""

	return nil
}
