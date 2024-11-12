//go:build windows
// +build windows

package nfs

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/go-git/go-billy/v5/osfs"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/willscott/go-nfs"
	"github.com/willscott/go-nfs/helpers"
	"golang.org/x/sys/windows/registry"
)

func Serve(ctx context.Context, errChan chan string, address, port string, driveLetter string) {
	baseKey, _, err := registry.CreateKey(registry.LOCAL_MACHINE, "Software\\PBSPlus\\Config", registry.QUERY_VALUE)
	if err != nil {
		errChan <- fmt.Sprintf("Unable to create registry key -> %v", err)
		return
	}

	defer baseKey.Close()

	var server string
	if server, _, err = baseKey.GetStringValue("ServerURL"); err != nil {
		errChan <- fmt.Sprintf("Unable to get server url -> %v", err)
		return
	}

	serverUrl, err := url.Parse(server)
	if err != nil {
		errChan <- fmt.Sprintf("failed to parse server IP: %v", err)
		return
	}

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

		listener = &FilteredListener{Listener: listener, allowedIP: serverUrl.Hostname()}
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

	snapshot, err := snapshots.Snapshot(driveLetter)
	if err != nil {
		errChan <- fmt.Sprintf("failed to initialize snapshot: %v", err)
		return
	}
	defer snapshot.Close()

	fs := osfs.New(snapshot.SnapshotPath)
	readOnlyFs := NewROFS(fs)
	nfsHandler := helpers.NewNullAuthHandler(readOnlyFs)

	for {
		done := make(chan struct{})
		go func() {
			err := nfs.Serve(listener, nfsHandler)
			if err != nil {
				errChan <- fmt.Sprintf("NFS server error: %v", err)
			}
			close(done)
		}()

		select {
		case <-ctx.Done():
			listener.Close()
			return
		case <-done:
		}
	}
}
