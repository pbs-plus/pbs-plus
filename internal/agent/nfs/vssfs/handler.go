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

	// Convert NFS path to Windows format for internal storage
	windowsPath := filepath.Join(path...)
	fullWindowsPath := filepath.Join(vssFS.Root(), windowsPath)

	// Ensure path exists in cache
	if _, err := vssFS.Stat(strings.Join(path, "/")); err != nil {
		return nil
	}

	if id, exists := vssFS.PathToID.Load(fullWindowsPath); exists {
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
		// Convert Windows path back to NFS format
		relativePath := strings.TrimPrefix(
			filepath.ToSlash(winPath.(string)),
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
func (h *VSSIDCachingHandler) HandleLimit() int {
	return math.MaxInt
}

// InvalidateHandle is a no-op as handles are immutable.
func (h *VSSIDCachingHandler) InvalidateHandle(fs billy.Filesystem, handle []byte) error {
	return nil
}
