//go:build windows

package vssfs

import (
	"encoding/binary"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/go-git/go-billy/v5"
	lru "github.com/hashicorp/golang-lru/v2"
	nfs "github.com/willscott/go-nfs"
)

const (
	RootHandleID = uint64(0) // Reserved ID for root directory
)

// VSSIDHandler uses VSSFS's stable file IDs for handle management
type VSSIDHandler struct {
	nfs.Handler
	vssFS            *VSSFS
	activeHandles    *lru.Cache[uint64, string]
	activeVerfifiers *lru.Cache[uint64, []fs.FileInfo]
}

func NewVSSIDHandler(vssFS *VSSFS, underlyingHandler nfs.Handler) (*VSSIDHandler, error) {
	cache, err := lru.New[uint64, string](CacheLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to create handle cache: %w", err)
	}

	verifiers, err := lru.New[uint64, []fs.FileInfo](CacheLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to create verifier cache: %w", err)
	}

	return &VSSIDHandler{
		Handler:          underlyingHandler,
		vssFS:            vssFS,
		activeHandles:    cache,
		activeVerfifiers: verifiers,
	}, nil
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
	if _, exists := h.activeHandles.Get(fileID); !exists {
		h.activeHandles.Add(fileID, fullPath)
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
	fullPath, exists := h.activeHandles.Get(fileID)
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
	return CacheLimit
}

func (h *VSSIDHandler) InvalidateHandle(fs billy.Filesystem, handle []byte) error {
	// No-op for read-only filesystem
	return nil
}

// VerifierFor implements directory cookie verification using stable IDs
func (h *VSSIDHandler) VerifierFor(path string, contents []fs.FileInfo) uint64 {
	// Get stable ID for directory
	handle := h.ToHandle(h.vssFS, strings.Split(path, "/"))
	if handle == nil {
		return 0
	}

	dirID := binary.BigEndian.Uint64(handle)

	// Cache directory contents for DataForVerifier
	h.activeVerfifiers.Add(dirID, contents)
	return dirID
}

// DataForVerifier retrieves cached directory contents
func (h *VSSIDHandler) DataForVerifier(path string, verifier uint64) []fs.FileInfo {
	if contents, exists := h.activeVerfifiers.Get(verifier); exists {
		return contents
	}
	return nil
}
