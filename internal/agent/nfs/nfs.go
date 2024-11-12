//go:build windows
// +build windows

package nfs

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/go-git/go-billy/v5/osfs"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
	"github.com/willscott/go-nfs"
	"github.com/willscott/go-nfs/helpers"
)

func Serve(ctx context.Context, errChan chan string, address, port string, driveLetter string) {
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

	snapshot, err := snapshots.Snapshot(driveLetter)
	if err != nil {
		errChan <- fmt.Sprintf("failed to initialize snapshot: %v", err)
		return
	}
	defer snapshot.Close()

	fs := osfs.New(snapshot.SnapshotPath)
	readOnlyFs := NewROFS(fs)
	nfsHandler := helpers.NewNullAuthHandler(readOnlyFs)

	go func() {
		for {
			go func() {
				err := nfs.Serve(listener, nfsHandler)
				if err != nil {
					errChan <- fmt.Sprintf("NFS server error: %v", err)
				}
			}()

			select {
			case <-ctx.Done():
				listener.Close()
				return
			case <-errChan:
			}
		}
	}()
}
