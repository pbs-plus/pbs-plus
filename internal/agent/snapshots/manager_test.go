package snapshots

import (
	"runtime"
	"strings"
	"testing"
)

func TestSnapshotManagerCreateSnapshotUnavailableOnLinux(t *testing.T) {
	t.Parallel()
	if runtime.GOOS != "linux" {
		t.Skip("Linux-specific behavior")
	}

	_, err := Manager.CreateSnapshot("job", "/")
	if err == nil || !strings.Contains(err.Error(), "unavailable on Linux") {
		t.Fatalf("CreateSnapshot() error = %v, want Linux unavailable error", err)
	}
}
