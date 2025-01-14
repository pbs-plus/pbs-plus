//go:build windows
// +build windows

package sftp

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pkg/sftp"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
	"golang.org/x/crypto/ssh"
)

const (
	maxConcurrentConnections = 100
	connectionTimeout        = 5 * time.Minute
	sshHandshakeTimeout      = 30 * time.Second
	maxRetries               = 5
	retryInterval            = 5 * time.Second
)

type SFTPSession struct {
	Context     context.Context
	ctxCancel   context.CancelFunc
	Snapshot    *snapshots.WinVSSSnapshot
	DriveLetter string
	Config      *SFTPConfig
	listener    net.Listener
	connections sync.WaitGroup
	sem         chan struct{} // Connection semaphore
}

func NewSFTPSession(ctx context.Context, snapshot *snapshots.WinVSSSnapshot, driveLetter string) *SFTPSession {
	cancellableCtx, cancel := context.WithCancel(ctx)

	anyConfig, ok := InitializedConfigs.Load(driveLetter)
	if !ok {
		cancel()
		return nil
	}

	sftpConfig, isValid := anyConfig.(*SFTPConfig)
	if !isValid {
		cancel()
		return nil
	}

	return &SFTPSession{
		Context:     cancellableCtx,
		Snapshot:    snapshot,
		DriveLetter: driveLetter,
		ctxCancel:   cancel,
		Config:      sftpConfig,
		sem:         make(chan struct{}, maxConcurrentConnections),
	}
}

func (s *SFTPSession) Close() {
	s.ctxCancel()
	if s.listener != nil {
		s.listener.Close()
	}
	// Wait for all connections to finish
	s.connections.Wait()
	s.Snapshot.Close()
}

func (s *SFTPSession) setupListener(port string) error {
	listenAddr := fmt.Sprintf("0.0.0.0:%s", port)
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", listenAddr, err)
	}
	s.listener = listener
	return nil
}

func (s *SFTPSession) Serve() {
	defer s.Close()

	port, err := utils.DriveLetterPort([]rune(s.DriveLetter)[0])
	if err != nil {
		syslog.L.Errorf("Unable to determine port number: %v", err)
		return
	}

	// Setup listener with retries
	for attempt := 0; attempt < maxRetries; attempt++ {
		err := s.setupListener(port)
		if err == nil {
			break
		}
		if attempt == maxRetries-1 {
			syslog.L.Errorf("Failed to start listener after %d attempts: %v", maxRetries, err)
			return
		}
		select {
		case <-s.Context.Done():
			return
		case <-time.After(retryInterval):
			continue
		}
	}

	// Setup graceful shutdown
	shutdown := make(chan struct{})
	defer close(shutdown)

	go func() {
		<-s.Context.Done()
		s.listener.Close()
		close(shutdown)
	}()

	s.acceptConnections()
}

func (s *SFTPSession) acceptConnections() {
	for {
		select {
		case <-s.Context.Done():
			return
		case s.sem <- struct{}{}: // Acquire semaphore
			conn, err := s.listener.Accept()
			if err != nil {
				<-s.sem // Release semaphore on error
				if !strings.Contains(err.Error(), "use of closed network connection") {
					syslog.L.Errorf("Failed to accept connection: %v", err)
				}
				return
			}

			s.connections.Add(1)
			go func() {
				defer func() {
					<-s.sem // Release semaphore
					s.connections.Done()
				}()
				s.handleConnection(conn)
			}()
		}
	}
}

func (s *SFTPSession) handleConnection(conn net.Conn) {
	defer conn.Close()

	// Set connection deadline
	if err := conn.SetDeadline(time.Now().Add(connectionTimeout)); err != nil {
		syslog.L.Errorf("Failed to set connection deadline: %v", err)
		return
	}

	if err := s.validateConnection(conn); err != nil {
		syslog.L.Error(err.Error())
		return
	}

	// Create context with timeout for SSH handshake
	handshakeCtx, cancel := context.WithTimeout(s.Context, sshHandshakeTimeout)
	defer cancel()

	// Create error channel for handshake
	handshakeErr := make(chan error, 1)
	var sconn *ssh.ServerConn
	var chans <-chan ssh.NewChannel
	var reqs <-chan *ssh.Request

	// Perform SSH handshake with timeout
	go func() {
		var err error
		sconn, chans, reqs, err = ssh.NewServerConn(conn, s.Config.ServerConfig)
		handshakeErr <- err
	}()

	select {
	case err := <-handshakeErr:
		if err != nil {
			syslog.L.Errorf("Failed to perform SSH handshake: %v", err)
			return
		}
	case <-handshakeCtx.Done():
		syslog.L.Error("SSH handshake timed out")
		return
	}

	defer sconn.Close()

	// Handle SSH requests with context awareness
	go func() {
		select {
		case <-s.Context.Done():
			sconn.Close()
		case req := <-reqs:
			if req != nil {
				ssh.DiscardRequests(reqs)
			}
		}
	}()

	s.handleChannels(chans)
}

func (s *SFTPSession) validateConnection(conn net.Conn) error {
	server, err := url.Parse(s.Config.Server)
	if err != nil {
		return fmt.Errorf("failed to parse server IP: %w", err)
	}

	if !strings.Contains(conn.RemoteAddr().String(), server.Hostname()) {
		return fmt.Errorf("WARNING: unregistered client attempted to connect: %s", conn.RemoteAddr().String())
	}
	return nil
}

func (s *SFTPSession) handleChannels(chans <-chan ssh.NewChannel) {
	for newChannel := range chans {
		select {
		case <-s.Context.Done():
			return
		default:
			if newChannel.ChannelType() != "session" {
				newChannel.Reject(ssh.UnknownChannelType, "unknown channel type")
				continue
			}

			channel, requests, err := newChannel.Accept()
			if err != nil {
				continue
			}

			go s.handleChannel(channel, requests)
		}
	}
}

func (s *SFTPSession) handleChannel(channel ssh.Channel, requests <-chan *ssh.Request) {
	defer channel.Close()

	sftpRequested := make(chan bool, 1)
	go handleRequests(s.Context, requests, sftpRequested)

	select {
	case requested, ok := <-sftpRequested:
		if ok && requested {
			s.handleSFTP(channel)
		}
	case <-s.Context.Done():
		return
	}
}

func handleRequests(ctx context.Context, requests <-chan *ssh.Request, sftpRequest chan<- bool) {
	defer close(sftpRequest)

	for {
		select {
		case req, ok := <-requests:
			if !ok {
				return
			}
			if req.Type == "subsystem" && string(req.Payload[4:]) == "sftp" {
				sftpRequest <- true
				req.Reply(true, nil)
				return
			}
			req.Reply(false, nil)
		case <-ctx.Done():
			return
		}
	}
}

func (s *SFTPSession) handleSFTP(channel ssh.Channel) {
	sftpHandler, err := NewSftpHandler(s.Context, s.Snapshot)
	if err != nil {
		syslog.L.Errorf("Failed to initialize handler: %v", err)
		return
	}

	server := sftp.NewRequestServer(channel, *sftpHandler)
	defer server.Close()

	// Ensure server is closed when context is done
	go func() {
		<-s.Context.Done()
		server.Close()
	}()

	if err := server.Serve(); err != nil {
		syslog.L.Errorf("SFTP server error: %v", err)
	}
}
