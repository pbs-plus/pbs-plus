//go:build linux

package filesystem

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

// TestServeOverInheritedSocket proves the binary speaks the real protocol:
// it builds cmd/plugin-filesystem and describes it through the supervisor.
func TestServeOverInheritedSocket(t *testing.T) {
	executable := filepath.Join(t.TempDir(), "plugin-filesystem")
	build := exec.Command("go", "build", "-o", executable, "github.com/pbs-plus/pbs-plus/cmd/plugin-filesystem")
	build.Env = append(os.Environ(), "GOFLAGS=")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build plugin: %v: %s", err, out)
	}

	supervisor, err := targetplugin.NewSupervisor(2, 1)
	if err != nil {
		t.Fatalf("NewSupervisor: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := supervisor.Run(ctx, PluginID, executable, func(runCtx context.Context, process *targetplugin.Process) error {
		descriptor, err := process.Describe(runCtx)
		if err != nil {
			return err
		}
		want := Descriptor()
		if descriptor.PluginID != want.PluginID || descriptor.Version != want.Version ||
			descriptor.TargetTypes[0] != TargetTypeLocal || descriptor.TargetSchema.Version != schemaVersion {
			t.Fatalf("described = %#v", descriptor)
		}

		health := targetplugin.PluginHealthRequest{Operation: targetplugin.Operation{
			ProtocolVersion: targetplugin.CurrentProtocolVersion,
			ID:              "health-1", IdempotencyKey: "health-1",
			DeadlineUnixMilli: time.Now().Add(time.Minute).UnixMilli(),
			PluginVersion:     Version,
		}}
		var healthResponse targetplugin.PluginHealthResponse
		return process.Invoke(runCtx, targetplugin.MethodPluginHealth, health, &healthResponse)
	}); err != nil {
		t.Fatalf("supervisor run: %v", err)
	}
}
