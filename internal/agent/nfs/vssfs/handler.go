//go:build windows

package vssfs

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/go-git/go-billy/v5"
	nfs "github.com/willscott/go-nfs"
)

// VSSIDCachingHandler uses VSSFS's PathToID and IDToPath for handle management.
type VSSIDCachingHandler struct {
	nfs.Handler
	vssFS *VSSFS
}

// NewVSSIDCachingHandler initializes the handler with a reference to VSSFS.
func NewVSSIDCachingHandler(vssFS *VSSFS, underlyingHandler nfs.Handler) *VSSIDCachingHandler {
	return &VSSIDCachingHandler{
		Handler: underlyingHandler,
		vssFS:   vssFS,
	}
}

// ToHandle converts a filesystem path to a stable ID handle.
func (h *VSSIDCachingHandler) ToHandle(f billy.Filesystem, path []string) []byte {
	vssFS, ok := f.(*VSSFS)
	if !ok || vssFS != h.vssFS {
		return nil
	}

	// Construct the path with forward slashes to match VSSFS's keys.
	joinedPath := "/" + strings.Join(path, "/")
	if len(path) == 0 {
		joinedPath = "/"
	}

	vssFS.CacheMu.RLock()
	stableID, exists := vssFS.PathToID[joinedPath]
	vssFS.CacheMu.RUnlock()

	if !exists {
		return nil
	}

	// Encode stableID as 8-byte handle.
	handle := make([]byte, 8)
	binary.BigEndian.PutUint64(handle, stableID)
	return handle
}

// FromHandle converts a stable ID handle back to the filesystem and path.
func (h *VSSIDCachingHandler) FromHandle(handle []byte) (billy.Filesystem, []string, error) {
	if len(handle) != 8 {
		return nil, nil, fmt.Errorf("invalid handle length")
	}

	stableID := binary.BigEndian.Uint64(handle)
	h.vssFS.CacheMu.RLock()
	path, exists := h.vssFS.IDToPath[stableID]
	h.vssFS.CacheMu.RUnlock()

	if !exists {
		return nil, nil, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusStale}
	}

	// Split the stored path into components.
	var parts []string
	if path != "/" {
		parts = strings.Split(strings.TrimPrefix(path, "/"), "/")
	}
	return h.vssFS, parts, nil
}

// HandleLimit returns the number of precomputed handles.
func (h *VSSIDCachingHandler) HandleLimit() int {
	h.vssFS.CacheMu.RLock()
	defer h.vssFS.CacheMu.RUnlock()
	return len(h.vssFS.IDToPath)
}

// InvalidateHandle is a no-op as handles are immutable.
func (h *VSSIDCachingHandler) InvalidateHandle(fs billy.Filesystem, handle []byte) error {
	return nil
}
