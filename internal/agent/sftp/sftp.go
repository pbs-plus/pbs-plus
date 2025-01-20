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
	reconnectDelay           = 1 * time.Second
	maxReconnectAttempts     = 3
)

type SFTPSession struct {
	Context     context.Context
	ctxCancel   context.CancelFunc
	Snapshot    *snapshots.WinVSSSnapshot
	DriveLetter string
	Config      *SFTPConfig
	listener    net.Listener
	connections sync.WaitGroup
	sem         chan struct{}
	isRunning   bool
	mu          sync.Mutex // Protects isRunning
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
		isRunning:   true,
	}
}

func (s *SFTPSession) Close() {
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

	for {
		s.mu.Lock()
		if !s.isRunning {
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()

		if err := s.serveOnce(); err != nil {
			if strings.Contains(err.Error(), "use of closed network connection") {
				return
			}
			syslog.L.Errorf("SFTP session error, attempting reconnect: %v", err)
			select {
			case <-s.Context.Done():
				return
			case <-time.After(reconnectDelay):
				continue
			}
		}
	}
}

func (s *SFTPSession) serveOnce() error {
	port, err := utils.DriveLetterPort([]rune(s.DriveLetter)[0])
	if err != nil {
		return fmt.Errorf("unable to determine port number: %v", err)
	}

	// Setup listener with retries
	var listenerErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := s.setupListener(port); err != nil {
			listenerErr = err
			select {
			case <-s.Context.Done():
				return err
			case <-time.After(retryInterval):
				continue
			}
		}
		listenerErr = nil
		break
	}
	if listenerErr != nil {
		return fmt.Errorf("failed to start listener after %d attempts: %v", maxRetries, listenerErr)
	}

	return s.acceptConnections()
}

func (s *SFTPSession) acceptConnections() error {
	for {
		select {
		case <-s.Context.Done():
			return nil
		case s.sem <- struct{}{}:
			conn, err := s.listener.Accept()
			if err != nil {
				<-s.sem
				return fmt.Errorf("accept error: %v", err)
			}

			s.connections.Add(1)
			go func() {
				defer func() {
					<-s.sem
					s.connections.Done()
				}()
				s.handleConnectionWithRetry(conn)
			}()
		}
	}
}

func (s *SFTPSession) handleConnectionWithRetry(conn net.Conn) {
	defer conn.Close()

	for attempt := 0; attempt < maxReconnectAttempts; attempt++ {
		if err := s.handleConnection(conn); err != nil {
			syslog.L.Errorf("Connection handling failed (attempt %d/%d): %v",
				attempt+1, maxReconnectAttempts, err)

			select {
			case <-s.Context.Done():
				return
			case <-time.After(reconnectDelay):
				continue
			}
		}
		return
	}
}

func (s *SFTPSession) handleConnection(conn net.Conn) error {
	if err := conn.SetDeadline(time.Now().Add(connectionTimeout)); err != nil {
		return fmt.Errorf("failed to set connection deadline: %v", err)
	}

	if err := s.validateConnection(conn); err != nil {
		return err
	}

	handshakeCtx, cancel := context.WithTimeout(s.Context, sshHandshakeTimeout)
	defer cancel()

	sconn, chans, reqs, err := s.performSSHHandshake(handshakeCtx, conn)
	if err != nil {
		return fmt.Errorf("SSH handshake failed: %v", err)
	}
	defer sconn.Close()

	// Handle SSH requests with context awareness
	go s.handleSSHRequests(sconn, reqs)

	return s.handleChannels(chans)
}

func (s *SFTPSession) performSSHHandshake(ctx context.Context, conn net.Conn) (*ssh.ServerConn, <-chan ssh.NewChannel, <-chan *ssh.Request, error) {
	handshakeErr := make(chan error, 1)
	var result struct {
		sconn *ssh.ServerConn
		chans <-chan ssh.NewChannel
		reqs  <-chan *ssh.Request
	}

	go func() {
		var err error
		result.sconn, result.chans, result.reqs, err = ssh.NewServerConn(conn, s.Config.ServerConfig)
		handshakeErr <- err
	}()

	select {
	case err := <-handshakeErr:
		if err != nil {
			return nil, nil, nil, err
		}
		return result.sconn, result.chans, result.reqs, nil
	case <-ctx.Done():
		return nil, nil, nil, fmt.Errorf("SSH handshake timed out")
	}
}

func (s *SFTPSession) handleSSHRequests(sconn *ssh.ServerConn, reqs <-chan *ssh.Request) {
	go func() {
		for {
			select {
			case <-s.Context.Done():
				sconn.Close()
				return
			case req, ok := <-reqs:
				if !ok {
					return
				}
				if req != nil {
					go ssh.DiscardRequests(reqs)
				}
			}
		}
	}()
}

func (s *SFTPSession) validateConnection(conn net.Conn) error {
	server, err := url.Parse(s.Config.Server)
	if err != nil {
		return fmt.Errorf("failed to parse server IP: %w", err)
	}

	remoteAddr := conn.RemoteAddr().String()
	if !strings.Contains(remoteAddr, server.Hostname()) {
		return fmt.Errorf("unregistered client attempted to connect: %s", remoteAddr)
	}
	return nil
}

func (s *SFTPSession) handleChannels(chans <-chan ssh.NewChannel) error {
	for {
		select {
		case <-s.Context.Done():
			return nil
		case newChannel, ok := <-chans:
			if !ok {
				return nil
			}

			if newChannel.ChannelType() != "session" {
				newChannel.Reject(ssh.UnknownChannelType, "unknown channel type")
				continue
			}

			channel, requests, err := newChannel.Accept()
			if err != nil {
				syslog.L.Errorf("Failed to accept channel: %v", err)
				continue
			}

			go func() {
				if err := s.handleChannel(channel, requests); err != nil {
					syslog.L.Errorf("Channel handling error: %v", err)
				}
			}()
		}
	}
}

func (s *SFTPSession) handleChannel(channel ssh.Channel, requests <-chan *ssh.Request) error {
	defer channel.Close()

	sftpRequested := make(chan bool, 1)
	errChan := make(chan error, 1)

	go func() {
		err := s.handleRequests(requests, sftpRequested)
		if err != nil {
			errChan <- err
		}
	}()

	select {
	case <-s.Context.Done():
		return fmt.Errorf("context cancelled")
	case err := <-errChan:
		return fmt.Errorf("request handling error: %w", err)
	case requested, ok := <-sftpRequested:
		if !ok {
			return fmt.Errorf("SFTP request channel closed")
		}
		if requested {
			return s.handleSFTP(channel)
		}
		return fmt.Errorf("SFTP not requested")
	}
}

func (s *SFTPSession) handleRequests(requests <-chan *ssh.Request, sftpRequest chan<- bool) error {
	defer close(sftpRequest)

	for {
		select {
		case <-s.Context.Done():
			return fmt.Errorf("context cancelled")
		case req, ok := <-requests:
			if !ok {
				return nil
			}

			if req.Type == "subsystem" && string(req.Payload[4:]) == "sftp" {
				sftpRequest <- true
				if err := req.Reply(true, nil); err != nil {
					return fmt.Errorf("failed to reply to SFTP request: %w", err)
				}
				return nil
			}

			if err := req.Reply(false, nil); err != nil {
				return fmt.Errorf("failed to reply to non-SFTP request: %w", err)
			}
		}
	}
}

func (s *SFTPSession) handleSFTP(channel ssh.Channel) error {
	sftpHandler, err := NewSftpHandler(s.Context, s.Snapshot)
	if err != nil {
		return fmt.Errorf("failed to initialize SFTP handler: %w", err)
	}

	server := sftp.NewRequestServer(channel, *sftpHandler)
	defer server.Close()

	// Handle server shutdown when context is done
	go func() {
		<-s.Context.Done()
		server.Close()
	}()

	// Start serving with error tracking
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve()
	}()

	// Wait for either context cancellation or server error
	select {
	case <-s.Context.Done():
		return fmt.Errorf("context cancelled")
	case err := <-serveDone:
		if err != nil && !strings.Contains(err.Error(), "EOF") {
			return fmt.Errorf("SFTP server error: %w", err)
		}
		return nil
	}
}
