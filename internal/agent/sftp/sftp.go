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

func Serve(ctx context.Context, errChan chan string, snapshot *snapshots.WinVSSSnapshot, driveLetter string) {
	sftpConfig, err := InitializeSFTPConfig(driveLetter)
	if err != nil {
		errChan <- fmt.Sprintf("Unable to initialize SFTP config: %s", err)
	}
	if err := sftpConfig.PopulateKeys(); err != nil {
		errChan <- fmt.Sprintf("Unable to populate SFTP keys: %s", err)
	}

	port, err := utils.DriveLetterPort([]rune(driveLetter)[0])
	if err != nil {
		errChan <- fmt.Sprintf("Unable to determine port number: %s", err)
	}

	var listener net.Listener
	listening := false

	listen := func() {
		var err error
		listenAt := fmt.Sprintf("0.0.0.0:%s", port)
		listener, err = net.Listen("tcp", listenAt)
		if err != nil {
			errChan <- fmt.Sprintf("Port is already in use! Failed to listen on %s: %v", listenAt, err)
			return
		}

		listening = true
	}

	listen()

	for !listening {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second * 5):
			listen()
		}
	}

	defer listener.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			conn, err := listener.Accept()
			if err != nil {
				errChan <- fmt.Sprintf("failed to accept connection: %v", err)
				continue
			}

			go handleConnection(ctx, errChan, conn, sftpConfig, snapshot)
		}
	}
}

func handleConnection(ctx context.Context, errChan chan string, conn net.Conn, sftpConfig *SFTPConfig, snapshot *snapshots.WinVSSSnapshot) {
	defer conn.Close()

	server, err := url.Parse(sftpConfig.Server)
	if err != nil {
		errChan <- fmt.Sprintf("failed to parse server IP: %v", err)
		return
	}

	if !strings.Contains(conn.RemoteAddr().String(), server.Hostname()) {
		errChan <- fmt.Sprintf("WARNING: an unregistered client has attempted to connect: %s", conn.RemoteAddr().String())
		return
	}

	sconn, chans, reqs, err := ssh.NewServerConn(conn, sftpConfig.ServerConfig)
	if err != nil {
		errChan <- fmt.Sprintf("failed to perform SSH handshake: %v", err)
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
			go handleSFTP(ctx, errChan, channel, snapshot)
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
			default:
				sftpRequest <- false
				req.Reply(false, nil)
			}
		case <-ctx.Done():
			return
		}
	}
}

func handleSFTP(ctx context.Context, errChan chan string, channel ssh.Channel, snapshot *snapshots.WinVSSSnapshot) {
	defer channel.Close()

	sftpHandler, err := NewSftpHandler(ctx, snapshot)
	if err != nil {
		errChan <- fmt.Sprintf("failed to initialize handler: %v", err)
		return
	}

	server := sftp.NewRequestServer(channel, *sftpHandler)

	go func() {
		<-ctx.Done()
		server.Close()
	}()

	_ = server.Serve()
}
