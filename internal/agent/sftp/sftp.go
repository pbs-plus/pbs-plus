//go:build windows
// +build windows

package sftp

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/pkg/sftp"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/agent/serverlog"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/utils"
	"golang.org/x/crypto/ssh"
)

func Serve(ctx context.Context, wg *sync.WaitGroup, sftpConfig *SFTPConfig, address, port string, driveLetter string) {
	defer wg.Done()

	logger, _ := serverlog.InitializeLogger()

	listenAt := fmt.Sprintf("%s:%s", address, port)
	listener, err := net.Listen("tcp", listenAt)
	if err != nil {
		if logger != nil {
			logger.Print(fmt.Sprintf("Port is already in use! Failed to listen on %s: %v", listenAt, err))
		}
		utils.ShowMessageBox("Fatal Error", fmt.Sprintf("Port is already in use! Failed to listen on %s: %v", listenAt, err))
		os.Exit(1)
	}
	defer listener.Close()

	log.Printf("Listening on %v\n", listener.Addr())

	for {
		select {
		case <-ctx.Done():
			log.Println("Context cancelled. Terminating SFTP listener.")
			return
		default:
			conn, err := listener.Accept()
			if err != nil {
				if logger != nil {
					logger.Print(fmt.Sprintf("failed to accept connection: %v", err))
				}
				utils.ShowMessageBox("Error", fmt.Sprintf("failed to accept connection: %v", err))
				continue
			}

			go handleConnection(conn, sftpConfig, driveLetter)
		}
	}
}

func handleConnection(conn net.Conn, sftpConfig *SFTPConfig, driveLetter string) {
	defer conn.Close()
	logger, _ := serverlog.InitializeLogger()

	server, err := url.Parse(sftpConfig.Server)
	if err != nil {
		if logger != nil {
			logger.Print(fmt.Sprintf("failed to parse server IP: %v", err))
		}
		utils.ShowMessageBox("Error", fmt.Sprintf("failed to parse server IP: %v", err))
		return
	}

	if !strings.Contains(conn.RemoteAddr().String(), server.Hostname()) {
		if logger != nil {
			logger.Print(fmt.Sprintf("WARNING: an unregistered client has attempted to connect: %s", conn.RemoteAddr().String()))
		}
		utils.ShowMessageBox("Error", fmt.Sprintf("WARNING: an unregistered client has attempted to connect: %s", conn.RemoteAddr().String()))
		return
	}

	sconn, chans, reqs, err := ssh.NewServerConn(conn, sftpConfig.ServerConfig)
	if err != nil {
		if logger != nil {
			logger.Print(fmt.Sprintf("failed to perform SSH handshake: %v", err))
		}
		utils.ShowMessageBox("Error", fmt.Sprintf("failed to perform SSH handshake: %v", err))
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

		go handleRequests(requests)
		go handleSFTP(channel, driveLetter)
	}
}

func handleRequests(requests <-chan *ssh.Request) {
	for req := range requests {
		if req.Type == "subsystem" && string(req.Payload[4:]) == "sftp" {
			req.Reply(true, nil)
		} else if req.Type == "ping" {
			req.Reply(true, []byte("pong"))
		} else {
			req.Reply(false, nil)
		}
	}
}

func handleSFTP(channel ssh.Channel, driveLetter string) {
	defer channel.Close()
	logger, _ := serverlog.InitializeLogger()

	snapshot, err := snapshots.Snapshot(driveLetter)
	if err != nil {
		if logger != nil {
			logger.Print(fmt.Sprintf("failed to initialize snapshot: %v", err))
		}
		utils.ShowMessageBox("Fatal Error", fmt.Sprintf("failed to initialize snapshot: %s", err))
		os.Exit(1)
	}

	ctx := context.Background()
	sftpHandler, err := NewSftpHandler(ctx, driveLetter, snapshot)
	if err != nil {
		snapshot.Close()
		if logger != nil {
			logger.Print(fmt.Sprintf("failed to initialize handler: %v", err))
		}
		utils.ShowMessageBox("Fatal Error", fmt.Sprintf("failed to initialize handler: %s", err))
		os.Exit(1)
	}

	server := sftp.NewRequestServer(channel, *sftpHandler)
	if err := server.Serve(); err != nil {
		log.Printf("sftp server completed with error: %s\n", err)
	}
}
