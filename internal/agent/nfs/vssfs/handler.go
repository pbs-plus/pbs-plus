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

func (h *VSSIDHandler) storeHandle(key uint64, path string) {
	h.activeHandlesMu.Lock()
	defer h.activeHandlesMu.Unlock()

	h.activeHandles[key] = path
}

func (h *VSSIDHandler) ToHandle(f billy.Filesystem, path []string) []byte {
	vssFS, ok := f.(*VSSFS)
	if !ok || vssFS != h.vssFS {
		return nil
	}

	// Handle root directory specially
	if len(path) == 0 || (len(path) == 1 && path[0] == "") {
		return h.createHandle(RootHandleID, vssFS.Root())
	}

	// Convert NFS path to Windows format
	winPath := filepath.Join(path...)
	fullPath := filepath.Join(vssFS.Root(), winPath)

	// Get or create stable ID
	info, err := vssFS.Stat(winPath)
	if err != nil {
		return nil
	}

	fileID := info.(*VSSFileInfo).stableID
	return h.createHandle(fileID, fullPath)
}

func (h *VSSIDHandler) createHandle(fileID uint64, fullPath string) []byte {
	// Add to cache if not exists
	if _, exists := h.getHandle(fileID); !exists {
		h.storeHandle(fileID, fullPath)
	}

	// Convert ID to 8-byte handle
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

	// Retrieve cached path
	fullPath, exists := h.getHandle(fileID)
	if !exists {
		return nil, nil, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusStale}
	}

	// Convert Windows path to NFS components
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
	// No-op for read-only filesystem
	return nil
}

func (h *VSSIDHandler) ClearHandles() {
	h.activeHandlesMu.Lock()
	defer h.activeHandlesMu.Unlock()

	h.activeHandles = make(map[uint64]string)
}
