//go:build windows

package vssfs

import (
	"encoding/binary"
	"fmt"
	"math"
	"path/filepath"
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

func (h *VSSIDCachingHandler) ToHandle(f billy.Filesystem, path []string) []byte {
	vssFS, ok := f.(*VSSFS)
	if !ok || vssFS != h.vssFS {
		return nil
	}

	joinedPath := strings.Join(path, "/")
	windowsPath := filepath.Join(f.Root(), strings.Join(path, "\\"))

	// Get through VSSFileInfo to ensure cache population
	if _, err := vssFS.Stat(joinedPath); err != nil {
		return nil
	}

	if id, exists := vssFS.PathToID.Load(windowsPath); exists {
		handle := make([]byte, 8)
		binary.BigEndian.PutUint64(handle, id.(uint64))
		return handle
	}

	return nil
}

func (h *VSSIDCachingHandler) FromHandle(handle []byte) (billy.Filesystem, []string, error) {
	if len(handle) != 8 {
		return nil, nil, fmt.Errorf("invalid handle")
	}
	stableID := binary.BigEndian.Uint64(handle)

	if winPath, exists := h.vssFS.IDToPath.Load(stableID); exists {
		// Split path into components using the normalized version
		path := strings.ReplaceAll(strings.TrimPrefix(winPath.(string), h.vssFS.root), "\\", "/")
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}

		var parts []string
		if path != "/" {
			parts = strings.Split(strings.TrimPrefix(path, "/"), "/")
		}
		return h.vssFS, parts, nil
	}

	return nil, nil, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusStale}
}

// HandleLimit returns the number of precomputed handles.
func (h *VSSIDCachingHandler) HandleLimit() int {
	return math.MaxInt
}

// InvalidateHandle is a no-op as handles are immutable.
func (h *VSSIDCachingHandler) InvalidateHandle(fs billy.Filesystem, handle []byte) error {
	return nil
}
