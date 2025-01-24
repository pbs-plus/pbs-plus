//go:build windows

package nfs

import (
	"context"
	"fmt"
	"net"
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

// HandleLimit returns the maximum number of handles that can be stored
func (h *NFSHandler) HandleLimit() int {
	return -1
}

// ToHandle converts a filesystem path to an opaque handle
func (h *NFSHandler) ToHandle(fs billy.Filesystem, path []string) []byte {
	return []byte{}
}

// FromHandle converts an opaque handle back to a filesystem and path
func (h *NFSHandler) FromHandle(fh []byte) (billy.Filesystem, []string, error) {
	return nil, []string{}, nil
}

// InvalidateHandle removes a file handle from the cache
func (h *NFSHandler) InvalidateHandle(fs billy.Filesystem, fh []byte) error {
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

	fs := vssfs.NewVSSFS(h.session.Snapshot, "/", h.session.ExcludedPaths, h.session.PartialFiles)
	syslog.L.Infof("[NFS.Mount] Mount successful, serving from: %s", h.session.Snapshot.SnapshotPath)
	return nfs.MountStatusOk, fs, []nfs.AuthFlavor{nfs.AuthFlavorNull}
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
