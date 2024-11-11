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

func Serve(ctx context.Context, errChan chan string, sftpConfig *SFTPConfig, address, port string, driveLetter string) {
	var listener net.Listener

	listening := false

	listen := func() {
		var err error
		listenAt := fmt.Sprintf("%s:%s", address, port)
		listener, err = net.Listen("tcp", listenAt)
		if err != nil {
			errChan <- fmt.Sprintf("Port is already in use! Failed to listen on %s: %v", listenAt, err)
			return
		}

		listening = true
	}

	listen()

	for !listening {
		retryWait := utils.WaitChan(time.Second * 5)
		select {
		case <-ctx.Done():
			return
		case <-retryWait:
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

			go handleConnection(ctx, errChan, conn, sftpConfig, driveLetter)
		}
	}
}

func handleConnection(ctx context.Context, errChan chan string, conn net.Conn, sftpConfig *SFTPConfig, driveLetter string) {
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
			go handleSFTP(ctx, errChan, channel, driveLetter)
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

func handleSFTP(ctx context.Context, errChan chan string, channel ssh.Channel, driveLetter string) {
	defer channel.Close()

	snapshot, err := snapshots.Snapshot(driveLetter)
	if err != nil {
		errChan <- fmt.Sprintf("failed to initialize snapshot: %v", err)
		return
	}

	sftpHandler, err := NewSftpHandler(ctx, driveLetter, snapshot)
	if err != nil {
		snapshot.Close()
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
