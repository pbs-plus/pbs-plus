//go:build windows
// +build windows

package sftp

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/pkg/sftp"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"golang.org/x/crypto/ssh"
)

func Serve(ctx context.Context, sftpConfig *SFTPConfig, address, port string, driveLetter string) {
	listenAt := fmt.Sprintf("%s:%s", address, port)
	listener, err := net.Listen("tcp", listenAt)
	if err != nil {
		logger.Error(fmt.Sprintf("Port is already in use! Failed to listen on %s: %v", listenAt, err))
		return
	}
	defer listener.Close()

	logger.Infof("Listening on %v\n", listener.Addr())

	for {
		select {
		case <-ctx.Done():
			logger.Info("Context cancelled. Terminating SFTP listener.")
			return
		default:
			conn, err := listener.Accept()
			if err != nil {
				logger.Error(fmt.Sprintf("failed to accept connection: %v", err))
				continue
			}

			go handleConnection(ctx, conn, sftpConfig, driveLetter)
		}
	}
}

func handleConnection(ctx context.Context, conn net.Conn, sftpConfig *SFTPConfig, driveLetter string) {
	defer conn.Close()

	server, err := url.Parse(sftpConfig.Server)
	if err != nil {
		logger.Error(fmt.Sprintf("failed to parse server IP: %v", err))
		return
	}

	if !strings.Contains(conn.RemoteAddr().String(), server.Hostname()) {
		logger.Error(fmt.Sprintf("WARNING: an unregistered client has attempted to connect: %s", conn.RemoteAddr().String()))
		return
	}

	sconn, chans, reqs, err := ssh.NewServerConn(conn, sftpConfig.ServerConfig)
	if err != nil {
		logger.Error(fmt.Sprintf("failed to perform SSH handshake: %v", err))
		return
	}
	defer sconn.Close()

	go ssh.DiscardRequests(reqs)

	for newChannel := range chans {
		if newChannel.ChannelType() != "session" {
			newChannel.Reject(ssh.UnknownChannelType, "unknown channel type")
			continue
		}

		channel, requests, err := newChannel.Accept()
		if err != nil {
			continue
		}

		sftpRequest := make(chan bool, 1)
		go handleRequests(ctx, requests, sftpRequest)

		if requested, ok := <-sftpRequest; ok && requested {
			go handleSFTP(ctx, channel, driveLetter)
		} else {
			channel.Close()
		}
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
			switch req.Type {
			case "subsystem":
				if string(req.Payload[4:]) == "sftp" {
					sftpRequest <- true
					req.Reply(true, nil)
				} else {
					sftpRequest <- false
					req.Reply(false, nil)
				}
			case "ping":
				sftpRequest <- false
				req.Reply(true, []byte("pong"))
			default:
				sftpRequest <- false
				req.Reply(false, nil)
			}
		case <-ctx.Done():
			return
		}
	}
}

func handleSFTP(ctx context.Context, channel ssh.Channel, driveLetter string) {
	defer channel.Close()

	snapshot, err := snapshots.Snapshot(driveLetter)
	if err != nil {
		logger.Error(fmt.Sprintf("failed to initialize snapshot: %v", err))
		return
	}

	sftpHandler, err := NewSftpHandler(ctx, driveLetter, snapshot)
	if err != nil {
		snapshot.Close()
		logger.Error(fmt.Sprintf("failed to initialize handler: %v", err))
		return
	}

	server := sftp.NewRequestServer(channel, *sftpHandler)

	go func() {
		<-ctx.Done()
		server.Close()
	}()

	if err := server.Serve(); err != nil {
		logger.Infof("sftp server completed with error: %s", err)
	}
}
