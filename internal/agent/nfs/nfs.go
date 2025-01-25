//go:build windows
// +build windows

package nfs

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"sync"

	"github.com/go-git/go-billy/v5"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/nfs/vssfs"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/registry"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/pattern"
	nfs "github.com/willscott/go-nfs"
)

type NFSSession struct {
	Context     context.Context
	ctxCancel   context.CancelFunc
	Snapshot    *snapshots.WinVSSSnapshot
	DriveLetter string
	listener    net.Listener
	connections sync.WaitGroup
	isRunning   bool
	serverURL   *url.URL
	FS          billy.Filesystem
	statusMu    sync.RWMutex
}

func NewNFSSession(ctx context.Context, snapshot *snapshots.WinVSSSnapshot, driveLetter string, excludedPaths *pattern.Matcher, partialFiles *pattern.Matcher) *NFSSession {
	cancellableCtx, cancel := context.WithCancel(ctx)

	urlStr, err := registry.GetEntry(registry.CONFIG, "ServerURL", false)
	if err != nil {
		syslog.L.Errorf("[NewNFSSession] unable to get server url: %v", err)

		cancel()
		return nil
	}

	parsedURL, _ := url.Parse(urlStr.Value)

	return &NFSSession{
		Context:     cancellableCtx,
		Snapshot:    snapshot,
		DriveLetter: driveLetter,
		ctxCancel:   cancel,
		isRunning:   true,
		serverURL:   parsedURL,
		FS: vssfs.NewVSSFS(
			snapshot,
			"/",
			excludedPaths, // Initialize from config/registry
			partialFiles,  // Initialize from config/registry
		),
	}
}

func (s *NFSSession) Close() {
	s.statusMu.Lock()
	s.isRunning = false
	s.statusMu.Unlock()

	s.ctxCancel()
	if s.listener != nil {
		s.listener.Close()
	}
	s.connections.Wait()
	s.Snapshot.Close()
}

func (s *NFSSession) Serve() error {
	port, err := utils.DriveLetterPort([]rune(s.DriveLetter)[0])
	if err != nil {
		return fmt.Errorf("unable to determine port number: %v", err)
	}

	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", port))
	if err != nil {
		return fmt.Errorf("failed to start listener: %v", err)
	}
	s.listener = listener
	defer listener.Close()

	handler := &NFSHandler{
		session: s,
	}

	// nfs.SetLogger(&nfsLogger{})

	syslog.L.Infof("[NFS.Serve] Serving NFS on port %s", port)

	return nfs.Serve(listener, handler)
}

func (s *NFSSession) IsRunning() bool {
	s.statusMu.RLock()
	defer s.statusMu.RUnlock()

	return s.isRunning
}
