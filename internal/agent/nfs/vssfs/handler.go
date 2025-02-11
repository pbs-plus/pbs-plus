//go:build windows

package vssfs

import (
	"encoding/binary"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"sync"

	"github.com/go-git/go-billy/v5"
	nfs "github.com/willscott/go-nfs"
)

const (
	RootHandleID = uint64(0) // Reserved ID for root directory
)

// VSSIDHandler uses VSSFS's stable file IDs for handle management
type VSSIDHandler struct {
	nfs.Handler
	vssFS           *VSSFS
	activeHandlesMu sync.RWMutex
	activeHandles   map[uint64]string
}

func NewVSSIDHandler(vssFS *VSSFS, underlyingHandler nfs.Handler) (*VSSIDHandler, error) {
	return &VSSIDHandler{
		Handler:       underlyingHandler,
		vssFS:         vssFS,
		activeHandles: make(map[uint64]string),
	}, nil
}

func (h *VSSIDHandler) getHandle(key uint64) (string, bool) {
	h.activeHandlesMu.RLock()
	defer h.activeHandlesMu.RUnlock()

	handle, ok := h.activeHandles[key]
	return handle, ok
}

func (h *VSSIDHandler) storeHandle(key uint64, relativePath string) {
	h.activeHandlesMu.Lock()
	defer h.activeHandlesMu.Unlock()

	h.activeHandles[key] = relativePath
}

func (h *VSSIDHandler) ToHandle(f billy.Filesystem, path []string) []byte {
	vssFS, ok := f.(*VSSFS)
	if !ok || vssFS != h.vssFS {
		return nil
	}

	// Handle root directory specially.
	// For the root, we store an empty relative path.
	if len(path) == 0 || (len(path) == 1 && path[0] == "") {
		return h.createHandle(RootHandleID, "")
	}

	// Compute the relative path from the filesystem's root.
	relativePath := filepath.Join(path...)

	// Get the stable file ID using the relative path.
	info, err := vssFS.Stat(relativePath)
	if err != nil {
		return nil
	}

	fileID := info.(*VSSFileInfo).stableID
	return h.createHandle(fileID, relativePath)
}

func (h *VSSIDHandler) createHandle(fileID uint64, relativePath string) []byte {
	// Add to cache if not already present.
	if _, exists := h.getHandle(fileID); !exists {
		h.storeHandle(fileID, relativePath)
	}

	// Convert fileID to an 8-byte handle.
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

	// Retrieve the stored relative path.
	relativePath, exists := h.getHandle(fileID)
	if !exists {
		return nil, nil, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusStale}
	}

	var parts []string
	if relativePath != "" {
		// Convert to slash format and split into components.
		parts = strings.Split(filepath.ToSlash(relativePath), "/")
	}
	return h.vssFS, parts, nil
}

func (h *VSSIDHandler) HandleLimit() int {
	return math.MaxInt
}

func (h *VSSIDHandler) InvalidateHandle(fs billy.Filesystem, handle []byte) error {
	// No-op for read-only filesystem.
	return nil
}

func (h *VSSIDHandler) ClearHandles() {
	h.activeHandlesMu.Lock()
	defer h.activeHandlesMu.Unlock()

	h.activeHandles = make(map[uint64]string)
}
