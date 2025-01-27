//go:build windows

package vssfs

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/go-git/go-billy/v5"
	nfs "github.com/willscott/go-nfs"
)

// VSSIDHandler uses VSSFS's PathToID and IDToPath for handle management.
type VSSIDHandler struct {
	nfs.Handler
	vssFS *VSSFS
}

// NewVSSIDHandler initializes the handler with a reference to VSSFS.
func NewVSSIDHandler(vssFS *VSSFS, underlyingHandler nfs.Handler) *VSSIDHandler {
	return &VSSIDHandler{
		Handler: underlyingHandler,
		vssFS:   vssFS,
	}
}

func (h *VSSIDHandler) ToHandle(f billy.Filesystem, path []string) []byte {
	vssFS, ok := f.(*VSSFS)
	if !ok || vssFS != h.vssFS {
		return nil
	}

	// Convert NFS path to Windows format for internal storage
	windowsPath := filepath.Join(path...)
	fullWindowsPath := filepath.Join(vssFS.Root(), windowsPath)

	if fullWindowsPath == vssFS.root {
		handle := make([]byte, 8)
		binary.BigEndian.PutUint64(handle, 0)
		return handle
	}

	// Ensure path exists in cache
	if _, err := vssFS.Stat(strings.Join(path, "/")); err != nil {
		return nil
	}

	if id, exists := vssFS.PathToID.Get(fullWindowsPath); exists {
		handle := make([]byte, 8)
		binary.BigEndian.PutUint64(handle, id)
		return handle
	}

	return nil
}

func (h *VSSIDHandler) FromHandle(handle []byte) (billy.Filesystem, []string, error) {
	if len(handle) != 8 {
		return nil, nil, fmt.Errorf("invalid handle")
	}

	stableID := binary.BigEndian.Uint64(handle)

	if stableID == 0 {
		return h.vssFS, []string{}, nil
	}

	if winPath, exists := h.vssFS.IDToPath.Get(stableID); exists {
		// Convert Windows path back to NFS format
		relativePath := strings.TrimPrefix(
			filepath.ToSlash(winPath),
			filepath.ToSlash(h.vssFS.Root()),
		)

		// Clean and split path components
		cleanPath := filepath.ToSlash(filepath.Clean(relativePath))
		if cleanPath == "." {
			return h.vssFS, []string{}, nil
		}

		var parts []string
		if cleanPath != "" {
			parts = strings.Split(strings.TrimPrefix(cleanPath, "/"), "/")
		}
		return h.vssFS, parts, nil
	}

	return nil, nil, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusStale}
}

// HandleLimit returns the number of precomputed handles.
func (h *VSSIDHandler) HandleLimit() int {
	return CacheLimit
}

// InvalidateHandle is a no-op as handles are immutable.
func (h *VSSIDHandler) InvalidateHandle(fs billy.Filesystem, handle []byte) error {
	return nil
}
