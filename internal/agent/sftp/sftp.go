//go:build windows
// +build windows

package sftp

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/url"
	"strings"
	"sync"

	"github.com/pkg/sftp"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"golang.org/x/crypto/ssh"
)

func Serve(ctx context.Context, wg *sync.WaitGroup, sftpConfig *SFTPConfig, address, port string, driveLetter string) {
	defer wg.Done()

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
			log.Println("Context cancelled. Terminating SFTP listener.")
			return
		default:
			conn, err := listener.Accept()
			if err != nil {
				logger.Error(fmt.Sprintf("failed to accept connection: %v", err))
				continue
			}

			go handleConnection(conn, sftpConfig, driveLetter)
		}
	}
}

func handleConnection(conn net.Conn, sftpConfig *SFTPConfig, driveLetter string) {
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

		// Create a channel to receive sftp request signals
		sftpRequest := make(chan bool, 1)
		go handleRequests(requests, sftpRequest)

		// Check the sftpRequest channel to determine if we should start SFTP
		if requested, ok := <-sftpRequest; ok && requested {
			go handleSFTP(channel, driveLetter)
		} else {
			channel.Close()
		}
	}
}

func handleRequests(requests <-chan *ssh.Request, sftpRequest chan<- bool) {
	for req := range requests {
		switch req.Type {
		case "subsystem":
			if string(req.Payload[4:]) == "sftp" {
				sftpRequest <- true // Signal that SFTP was requested
				req.Reply(true, nil)
			} else {
				sftpRequest <- false // Signal that an unknown subsystem was requested
				req.Reply(false, nil)
			}
		case "ping":
			sftpRequest <- false            // Signal that an unknown subsystem was requested
			req.Reply(true, []byte("pong")) // Reply to ping request
		default:
			sftpRequest <- false // Signal that an unknown subsystem was requested
			req.Reply(false, nil)
		}
	}
	// Close channel when done to signal no further requests
	close(sftpRequest)
}

func handleSFTP(channel ssh.Channel, driveLetter string) {
	defer channel.Close()

	snapshot, err := snapshots.Snapshot(driveLetter)
	if err != nil {
		logger.Error(fmt.Sprintf("failed to initialize snapshot: %v", err))
		return
	}

	ctx := context.Background()
	sftpHandler, err := NewSftpHandler(ctx, driveLetter, snapshot)
	if err != nil {
		snapshot.Close()
		logger.Error(fmt.Sprintf("failed to initialize handler: %v", err))
		return
	}

	server := sftp.NewRequestServer(channel, *sftpHandler)
	if err := server.Serve(); err != nil {
		logger.Infof("sftp server completed with error: %s", err)
	}
}
