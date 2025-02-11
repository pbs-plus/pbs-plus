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

// VSSIDHandler uses VSSFS's stable file IDs for handle management.
// It caches relative paths (from the filesystem root) and recreates the full
// path (by joining vssFS.Root()) when needed.
type VSSIDHandler struct {
	nfs.Handler
	vssFS *VSSFS

	activeHandlesMu sync.RWMutex
	// Maps fileID to the file's relative path.
	activeHandles map[uint64]string
}

func NewVSSIDHandler(vssFS *VSSFS, underlyingHandler nfs.Handler) (
	*VSSIDHandler, error,
) {
	return &VSSIDHandler{
		Handler:       underlyingHandler,
		vssFS:         vssFS,
		activeHandles: make(map[uint64]string),
	}, nil
}

func (h *VSSIDHandler) getHandle(key uint64) (string, bool) {
	h.activeHandlesMu.RLock()
	defer h.activeHandlesMu.RUnlock()

	path, ok := h.activeHandles[key]
	return path, ok
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

	// Special-case the root directory: store an empty relative path.
	if len(path) == 0 || (len(path) == 1 && path[0] == "") {
		return h.createHandle(RootHandleID, "")
	}

	// Compute the relative path from the provided NFS path components.
	relativePath := filepath.Join(path...)
	// Build the full path by joining the filesystem root with the relative path.
	fullPath := filepath.Join(vssFS.Root(), relativePath)

	// vssFS methods require the full path.
	info, err := vssFS.Stat(fullPath)
	if err != nil {
		return nil
	}

	fileID := info.(*VSSFileInfo).stableID
	// Only store the relative path in the cache.
	return h.createHandle(fileID, relativePath)
}

func (h *VSSIDHandler) createHandle(fileID uint64, relativePath string) []byte {
	if _, exists := h.getHandle(fileID); !exists {
		h.storeHandle(fileID, relativePath)
	}

	// Convert the 64-bit fileID to an 8-byte handle.
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

	relativePath, exists := h.getHandle(fileID)
	if !exists {
		return nil, nil, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusStale}
	}

	// Recreate the full path when needed.
	fullPath := filepath.Join(h.vssFS.Root(), relativePath)

	// Strip the filesystem root from the full path to get the NFS components.
	nfsRelative := strings.TrimPrefix(
		filepath.ToSlash(fullPath),
		filepath.ToSlash(h.vssFS.Root())+"/",
	)

	var parts []string
	if nfsRelative != "" {
		parts = strings.Split(nfsRelative, "/")
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
