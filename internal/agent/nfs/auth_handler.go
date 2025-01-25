//go:build windows

package nfs

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/nfs/vssfs"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	nfs "github.com/willscott/go-nfs"
	"golang.org/x/sys/windows"
)

type NFSHandler struct {
	mu      sync.Mutex
	session *NFSSession
}

// Verify Handler interface implementation
var _ nfs.Handler = (*NFSHandler)(nil)

// ToHandle converts a filesystem path to an opaque handle
func (h *NFSHandler) ToHandle(fs billy.Filesystem, path []string) []byte {
	vssFS, ok := fs.(*vssfs.VSSFS)
	if !ok {
		return nil
	}
	fullPath := "/" + filepath.ToSlash(filepath.Join(path...))

	vssFS.CacheMu.RLock()
	stableID, exists := vssFS.PathToID[fullPath]
	vssFS.CacheMu.RUnlock()

	if !exists {
		return nil
	}

	handle := make([]byte, 8)
	binary.LittleEndian.PutUint64(handle, stableID)
	return handle
}

// FromHandle converts an opaque handle back to a filesystem and path
func (h *NFSHandler) FromHandle(fh []byte) (billy.Filesystem, []string, error) {
	h.session.handleUsageMu.Lock()
	h.session.HandleUsage++
	h.session.handleUsageMu.Unlock()

	if len(fh) != 8 {
		return nil, nil, fmt.Errorf("invalid handle")
	}
	stableID := binary.LittleEndian.Uint64(fh)

	vssFS, ok := h.session.FS.(*vssfs.VSSFS)
	if !ok {
		return nil, nil, fmt.Errorf("invalid filesystem")
	}

	vssFS.CacheMu.RLock()
	pathStr, exists := vssFS.IDToPath[stableID]
	vssFS.CacheMu.RUnlock()

	if !exists {
		return nil, nil, fmt.Errorf("handle not found")
	}

	// Split path into components (e.g., "/dir/file" → ["dir", "file"])
	var path []string
	if pathStr != "/" {
		path = strings.Split(strings.TrimPrefix(pathStr, "/"), "/")
	}
	return vssFS, path, nil
}

func (h *NFSHandler) HandleLimit() int {
	// Use a fixed maximum that's within typical system limits
	const maxHandles = 1000000 // 1 million handles
	if h.session == nil || h.session.FS == nil {
		return 0
	}

	vssFS := h.session.FS.(*vssfs.VSSFS)
	vssFS.CacheMu.RLock()
	fileCount := len(vssFS.PathToID)
	vssFS.CacheMu.RUnlock()

	// Return whichever is smaller: actual file count or safe maximum
	if fileCount > maxHandles {
		syslog.L.Warnf("Handle count capped to %d (actual: %d files)", maxHandles, fileCount)
		return maxHandles
	}

	return fileCount
}

// InvalidateHandle - Required by interface but no-op in read-only FS
func (h *NFSHandler) InvalidateHandle(fs billy.Filesystem, fh []byte) error {
	// In read-only FS, handles never become invalid
	return nil
}

func (h *NFSHandler) validateConnection(conn net.Conn) error {
	remoteAddr := conn.RemoteAddr().String()

	clientIP, _, _ := net.SplitHostPort(remoteAddr)
	serverIPs, _ := net.LookupHost(h.session.serverURL.Hostname())
	for _, ip := range serverIPs {
		if clientIP == ip {
			return nil
		}
	}

	return fmt.Errorf("unregistered client attempted to connect: %s", remoteAddr)
}

func (h *NFSHandler) Mount(ctx context.Context, conn net.Conn, req nfs.MountRequest) (nfs.MountStatus, billy.Filesystem, []nfs.AuthFlavor) {
	syslog.L.Infof("[NFS.Mount] Received mount request for path: %s from %s",
		string(req.Dirpath), conn.RemoteAddr().String())

	if err := h.validateConnection(conn); err != nil {
		syslog.L.Errorf("[NFS.Mount] Connection validation failed: %v", err)
		return nfs.MountStatusErrPerm, nil, nil
	}

	syslog.L.Infof("[NFS.Mount] Mount successful, serving from: %s", h.session.Snapshot.SnapshotPath)
	return nfs.MountStatusOk, h.session.FS, []nfs.AuthFlavor{nfs.AuthFlavorNull}
}

func (h *NFSHandler) Change(fs billy.Filesystem) billy.Change {
	return nil
}

func (h *NFSHandler) FSStat(ctx context.Context, fs billy.Filesystem, stat *nfs.FSStat) error {
	driveLetter := h.session.Snapshot.DriveLetter
	drivePath := driveLetter + `:\`

	var totalBytes uint64
	err := windows.GetDiskFreeSpaceEx(
		windows.StringToUTF16Ptr(drivePath),
		nil,
		&totalBytes,
		nil,
	)
	if err != nil {
		return err
	}

	stat.TotalSize = totalBytes
	stat.FreeSize = 0
	stat.AvailableSize = 0
	stat.TotalFiles = 1 << 20
	stat.FreeFiles = 0
	stat.AvailableFiles = 0
	stat.CacheHint = time.Minute

	return nil
}
