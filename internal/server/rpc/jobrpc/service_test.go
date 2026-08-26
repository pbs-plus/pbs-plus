//go:build linux

package jobrpc_test

import (
	"context"
	"net/rpc"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/rpc/jobrpc"
	"github.com/pbs-plus/pbs-plus/internal/server/rpc/mountrpc"
)

func TestRunServersRegisterDedicatedServices(t *testing.T) {
	tests := []struct {
		name        string
		serviceName string
		run         func(context.Context, string) error
	}{
		{
			name:        "job",
			serviceName: jobrpc.ServiceName,
			run: func(ctx context.Context, socketPath string) error {
				return jobrpc.RunServer(ctx, socketPath, nil, nil)
			},
		},
		{
			name:        "mount",
			serviceName: mountrpc.ServiceName,
			run: func(ctx context.Context, socketPath string) error {
				return mountrpc.RunServer(ctx, socketPath, nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			socketPath := filepath.Join(t.TempDir(), "rpc.sock")
			errCh := make(chan error, 1)
			go func() { errCh <- tt.run(ctx, socketPath) }()

			var client *rpc.Client
			for deadline := time.After(time.Second); client == nil; {
				connected, err := rpc.Dial("unix", socketPath)
				if err == nil {
					client = connected
					break
				}
				select {
				case <-deadline:
					t.Fatal(err)
				case <-time.After(10 * time.Millisecond):
				}
			}

			err := client.Call(tt.serviceName+".NotRegistered", struct{}{}, new(struct{}))
			if closeErr := client.Close(); closeErr != nil {
				t.Error(closeErr)
			}
			if err == nil || strings.Contains(err.Error(), "can't find service") {
				t.Fatalf("%s is not registered: %v", tt.serviceName, err)
			}

			cancel()
			select {
			case err := <-errCh:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("RPC server did not stop")
			}
		})
	}
}
