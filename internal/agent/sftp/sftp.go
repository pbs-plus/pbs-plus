//go:build windows
// +build windows

package sftp

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/pkg/sftp"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
	"golang.org/x/crypto/ssh"
)

type SFTPSession struct {
	Context     context.Context
	ctxCancel   context.CancelFunc
	Snapshot    *snapshots.WinVSSSnapshot
	DriveLetter string
	Config      *SFTPConfig
	listener    net.Listener
}

func NewSFTPSession(ctx context.Context, snapshot *snapshots.WinVSSSnapshot, driveLetter string) *SFTPSession {
	cancellableCtx, cancel := context.WithCancel(ctx)

	return &SFTPSession{
		Context:     cancellableCtx,
		Snapshot:    snapshot,
		DriveLetter: driveLetter,
		ctxCancel:   cancel,
	}
}

func (s *SFTPSession) Close() {
	s.ctxCancel()
	if s.listener != nil {
		s.listener.Close()
	}
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

func (s *SFTPSession) Serve(errChan chan string) {
	defer s.Close()

	sftpConfig, err := InitializeSFTPConfig(s.DriveLetter)
	if err != nil {
		errChan <- fmt.Sprintf("Unable to initialize SFTP config: %v", err)
		return
	}

	if err := sftpConfig.PopulateKeys(); err != nil {
		errChan <- fmt.Sprintf("Unable to populate SFTP keys: %v", err)
		return
	}

	port, err := utils.DriveLetterPort([]rune(s.DriveLetter)[0])
	if err != nil {
		errChan <- fmt.Sprintf("Unable to determine port number: %v", err)
		return
	}

	const maxRetries = 5
	const retryInterval = 5 * time.Second

	for attempt := 0; attempt < maxRetries; attempt++ {
		err := s.setupListener(port)
		if err == nil {
			break
		}
		if attempt == maxRetries-1 {
			errChan <- fmt.Sprintf("Failed to start listener after %d attempts: %v", maxRetries, err)
			return
		}
		select {
		case <-s.Context.Done():
			return
		case <-time.After(retryInterval):
			continue
		}
	}

	s.acceptConnections(errChan, sftpConfig)
}

func (s *SFTPSession) acceptConnections(errChan chan string, sftpConfig *SFTPConfig) {
	for {
		select {
		case <-s.Context.Done():
			return
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				if !strings.Contains(err.Error(), "use of closed network connection") {
					errChan <- fmt.Sprintf("Failed to accept connection: %v", err)
				}
				return
			}
			go s.handleConnection(errChan, conn, sftpConfig)
		}
	}
}

func (s *SFTPSession) handleConnection(errChan chan string, conn net.Conn, sftpConfig *SFTPConfig) {
	defer conn.Close()

	if err := s.validateConnection(conn, sftpConfig); err != nil {
		errChan <- err.Error()
		return
	}

	sconn, chans, reqs, err := ssh.NewServerConn(conn, sftpConfig.ServerConfig)
	if err != nil {
		errChan <- fmt.Sprintf("Failed to perform SSH handshake: %v", err)
		return
	}
	defer sconn.Close()

	go ssh.DiscardRequests(reqs)
	s.handleChannels(errChan, chans)
}

func (s *SFTPSession) validateConnection(conn net.Conn, sftpConfig *SFTPConfig) error {
	server, err := url.Parse(sftpConfig.Server)
	if err != nil {
		return fmt.Errorf("failed to parse server IP: %w", err)
	}

	if !strings.Contains(conn.RemoteAddr().String(), server.Hostname()) {
		return fmt.Errorf("WARNING: unregistered client attempted to connect: %s", conn.RemoteAddr().String())
	}
	return nil
}

func (s *SFTPSession) handleChannels(errChan chan string, chans <-chan ssh.NewChannel) {
	for newChannel := range chans {
		if newChannel.ChannelType() != "session" {
			newChannel.Reject(ssh.UnknownChannelType, "unknown channel type")
			continue
		}

		channel, requests, err := newChannel.Accept()
		if err != nil {
			continue
		}

		go s.handleChannel(errChan, channel, requests)
	}
}

func (s *SFTPSession) handleChannel(errChan chan string, channel ssh.Channel, requests <-chan *ssh.Request) {
	defer channel.Close()

	sftpRequested := make(chan bool, 1)
	go handleRequests(s.Context, requests, sftpRequested)

	select {
	case requested, ok := <-sftpRequested:
		if ok && requested {
			s.handleSFTP(errChan, channel)
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

func (s *SFTPSession) handleSFTP(errChan chan string, channel ssh.Channel) {
	sftpHandler, err := NewSftpHandler(s.Context, s.Snapshot)
	if err != nil {
		errChan <- fmt.Sprintf("Failed to initialize handler: %v", err)
		return
	}

	server := sftp.NewRequestServer(channel, *sftpHandler)
	defer server.Close()

	go func() {
		<-s.Context.Done()
		server.Close()
	}()

	if err := server.Serve(); err != nil {
		errChan <- fmt.Sprintf("SFTP server error: %v", err)
	}
}
