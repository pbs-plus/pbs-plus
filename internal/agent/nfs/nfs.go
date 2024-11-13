//go:build windows
// +build windows

package nfs

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/url"
	"time"

	"github.com/go-git/go-billy/v5/osfs"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/willscott/go-nfs"
	"github.com/willscott/go-nfs/helpers"
	"golang.org/x/sys/windows/registry"
)

func Serve(ctx context.Context, address, port string, driveLetter string) error {
	baseKey, _, err := registry.CreateKey(registry.LOCAL_MACHINE, "Software\\PBSPlus\\Config", registry.QUERY_VALUE)
	if err != nil {
		return fmt.Errorf("Unable to create registry key -> %v", err)
	}

	defer baseKey.Close()

	var server string
	if server, _, err = baseKey.GetStringValue("ServerURL"); err != nil {
		return fmt.Errorf("Unable to get server url -> %v", err)
	}

	serverUrl, err := url.Parse(server)
	if err != nil {
		return fmt.Errorf("failed to parse server IP: %v", err)
	}

	var listener net.Listener

	listen := func() error {
		var err error
		listenAt := fmt.Sprintf("%s:%s", address, port)
		listener, err = net.Listen("tcp", listenAt)
		if err != nil {
			return fmt.Errorf("Port is already in use! Failed to listen on %s: %v", listenAt, err)
		}

		listener = &FilteredListener{Listener: listener, allowedIP: serverUrl.Hostname()}
		return nil
	}

	err = listen()
	for err != nil {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Second * 5):
			err = listen()
		}
	}

	defer listener.Close()

	snapshot, err := snapshots.Snapshot(driveLetter)
	if err != nil {
		return fmt.Errorf("failed to initialize snapshot: %v", err)
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
				log.Printf("NFS server error: %v\n", err)
			}
			close(done)
		}()

		select {
		case <-ctx.Done():
			listener.Close()
			return nil
		case <-done:
		}
	}
}
