//go:build windows
// +build windows

package nfs

import (
	"context"
	"fmt"
	"net"
	"regexp"
	"sync"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/registry"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
	nfs "github.com/willscott/go-nfs"
	nfshelper "github.com/willscott/go-nfs/helpers"
)

type NFSSession struct {
	Context       context.Context
	ctxCancel     context.CancelFunc
	Snapshot      *snapshots.WinVSSSnapshot
	DriveLetter   string
	listener      net.Listener
	connections   sync.WaitGroup
	sem           chan struct{}
	isRunning     bool
	mu            sync.Mutex // Protects isRunning
	serverURL     string
	ExcludedPaths []regexp.Regexp
	PartialFiles  []regexp.Regexp
}

func NewNFSSession(ctx context.Context, snapshot *snapshots.WinVSSSnapshot, driveLetter string) *NFSSession {
	cancellableCtx, cancel := context.WithCancel(ctx)

	url, err := registry.GetEntry(registry.CONFIG, "ServerURL", false)
	if err != nil {
		syslog.L.Errorf("[NewNFSSession] unable to get server url: %v", err)

		cancel()
		return nil
	}

	return &NFSSession{
		Context:     cancellableCtx,
		Snapshot:    snapshot,
		DriveLetter: driveLetter,
		ctxCancel:   cancel,
		isRunning:   true,
		serverURL:   url.Value,
	}
}

func (s *NFSSession) Close() {
	s.mu.Lock()
	s.isRunning = false
	s.mu.Unlock()

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

	cachedHandler := nfshelper.NewCachingHandler(handler, 1000)

	return nfs.Serve(listener, cachedHandler)
}
