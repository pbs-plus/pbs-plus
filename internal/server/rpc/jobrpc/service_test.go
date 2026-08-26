//go:build linux

package jobrpc_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/rpc/jobrpc"
	"github.com/pbs-plus/pbs-plus/internal/server/rpc/mountrpc"
)

func TestStartServerUsesDedicatedRPCServer(t *testing.T) {
	ctx := context.Background()
	if err := mountrpc.StartServer(nil, ctx, filepath.Join(t.TempDir(), "mount.sock"), nil); err != nil {
		t.Fatal(err)
	}
	if err := jobrpc.StartServer(nil, ctx, filepath.Join(t.TempDir(), "job.sock"), nil, nil); err != nil {
		t.Fatal(err)
	}
}
