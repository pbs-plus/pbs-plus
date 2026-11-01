//go:build linux

package mountrpc

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/vfs/sessions"
)

func TestMountRPCStopsWhenPeerDoesNotRead(t *testing.T) {
	client, server := net.Pipe()
	defer func() {
		if err := server.Close(); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := callMountRPCConn(ctx, client, "blocked", &BackupArgs{}, &BackupReply{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want deadline exceeded", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("RPC ignored cancellation")
	}
}

func TestStaleCleanupRPCLeavesCurrentMountAlone(t *testing.T) {
	for _, method := range []string{"agent", "s3"} {
		t.Run(method, func(t *testing.T) {
			id := "test-host|" + t.Name()
			m, err := sessions.ReserveMount(context.Background(), id, "current")
			if err != nil {
				t.Fatal(err)
			}
			m.FinishSetup()
			t.Cleanup(func() {
				if err := m.Disconnect(context.Background()); err != nil {
					t.Error(err)
				}
			})
			s := &Service{ctx: context.Background()}
			args := &CleanupArgs{BackupID: t.Name(), TargetHostname: "test-host", MountID: "old"}
			var reply CleanupReply
			if method == "agent" {
				err = s.ARPCCleanup(args, &reply)
			} else {
				err = s.S3Cleanup(args, &reply)
			}
			if err != nil || reply.Status != 200 {
				t.Fatalf("cleanup: %v, status %d", err, reply.Status)
			}
			if !sessions.OwnsMount(id, "current") || m.Context().Err() != nil {
				t.Fatal("stale RPC touched current mount")
			}
		})
	}
}

func TestMountDirectoryCleanupNeverDeletesContents(t *testing.T) {
	path := t.TempDir()
	file := filepath.Join(path, "keep")
	if err := os.WriteFile(file, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := removeMountDirectory(path); err == nil {
		t.Fatal("non-empty mount directory was removed")
	}
	if _, err := os.Stat(file); err != nil {
		t.Fatal(err)
	}
}
