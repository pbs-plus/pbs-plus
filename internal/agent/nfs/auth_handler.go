//go:build windows

package nfs

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/nfs/vssfs"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	nfs "github.com/willscott/go-nfs"
)

type NFSHandler struct {
	mu        sync.Mutex
	session   *NFSSession
	currentFS billy.Filesystem
}

// Verify Handler interface implementation
var _ nfs.Handler = (*NFSHandler)(nil)

// HandleLimit returns the maximum number of handles that can be stored
func (h *NFSHandler) HandleLimit() int {
	return math.MaxInt32
}

// ToHandle converts a filesystem path to a stable, unique handle
func (h *NFSHandler) ToHandle(fs billy.Filesystem, path []string) []byte {
	// Create hash of filesystem pointer and path for uniqueness
	hash := sha256.New()
	hash.Write([]byte(fmt.Sprintf("%p", fs)))
	hash.Write([]byte(strings.Join(path, "/")))
	return hash.Sum(nil)
}

// FromHandle converts handle back to filesystem and path
func (h *NFSHandler) FromHandle(fh []byte) (billy.Filesystem, []string, error) {
	// Since we don't cache, we can only validate handle format
	if len(fh) != sha256.Size {
		return nil, nil, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusStale}
	}

	// Always return current filesystem since we're stateless
	return h.currentFS, []string{}, nil
}

// InvalidateHandle is no-op since we don't cache
func (h *NFSHandler) InvalidateHandle(fs billy.Filesystem, fh []byte) error {
	return nil
}

func (h *NFSHandler) validateConnection(conn net.Conn) error {
	server, err := url.Parse(h.session.serverURL)
	if err != nil {
		return fmt.Errorf("failed to parse server IP: %w", err)
	}

	remoteAddr := conn.RemoteAddr().String()

	if !strings.Contains(remoteAddr, server.Hostname()) {
		return fmt.Errorf("unregistered client attempted to connect: %s", remoteAddr)
	}
	return nil
}

func (h *NFSHandler) Mount(ctx context.Context, conn net.Conn, req nfs.MountRequest) (nfs.MountStatus, billy.Filesystem, []nfs.AuthFlavor) {
	syslog.L.Infof("[NFS.Mount] Received mount request for path: %s from %s",
		string(req.Dirpath), conn.RemoteAddr().String())

	if err := h.validateConnection(conn); err != nil {
		syslog.L.Errorf("[NFS.Mount] Connection validation failed: %v", err)
		return nfs.MountStatusErrPerm, nil, nil
	}

	fs := vssfs.NewVSSFS(h.session.Snapshot, h.session.ExcludedPaths, h.session.PartialFiles)
	syslog.L.Infof("[NFS.Mount] Mount successful, serving from: %s", h.session.Snapshot.SnapshotPath)

	h.currentFS = fs

	return nfs.MountStatusOk, fs, []nfs.AuthFlavor{nfs.AuthFlavorNull}
}

func (h *NFSHandler) Change(fs billy.Filesystem) billy.Change {
	return nil
}

func (h *NFSHandler) FSStat(ctx context.Context, fs billy.Filesystem, stat *nfs.FSStat) error {
	stat.TotalSize = 1 << 40
	stat.FreeSize = 0
	stat.AvailableSize = 0
	stat.TotalFiles = 1 << 20
	stat.FreeFiles = 0
	stat.AvailableFiles = 0
	stat.CacheHint = time.Minute

	return nil
}
