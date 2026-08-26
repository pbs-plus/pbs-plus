//go:build linux

package jobrpc_test

import (
	"context"
	"net/rpc"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/rpc/jobrpc"
	"github.com/pbs-plus/pbs-plus/internal/server/rpc/mountrpc"
)

func TestStartServerUsesDedicatedRPCServer(t *testing.T) {
	ctx := context.Background()
	if err := mountrpc.StartServer(nil, ctx, filepath.Join(t.TempDir(), "mount.sock"), nil); err != nil {
		t.Fatal(err)
	}

	socketPath := filepath.Join(t.TempDir(), "job.sock")
	if err := jobrpc.StartServer(nil, ctx, socketPath, nil, nil); err != nil {
		t.Fatal(err)
	}

	client, err := rpc.Dial("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	err = client.Call("JobRPCService.NotRegistered", struct{}{}, new(struct{}))
	if err == nil || strings.Contains(err.Error(), "can't find service") {
		t.Fatalf("JobRPCService is not registered: %v", err)
	}
}
