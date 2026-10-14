//go:build !windows

package agent

import (
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/agent/snapshots"
)

func TestSkipPseudoMount(t *testing.T) {
	t.Parallel()

	pseudo := map[string]struct{}{
		"overlay": {},
		"proc":    {},
	}
	tests := []struct {
		name  string
		mount snapshots.MountEntry
		want  bool
	}{
		{
			name:  "root overlay",
			mount: snapshots.MountEntry{MountPoint: "/", FSType: "overlay"},
			want:  false,
		},
		{
			name:  "nested pseudo filesystem",
			mount: snapshots.MountEntry{MountPoint: "/proc", FSType: "proc"},
			want:  true,
		},
		{
			name:  "nested block filesystem",
			mount: snapshots.MountEntry{MountPoint: "/data", FSType: "ext4"},
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := skipPseudoMount(tt.mount, pseudo); got != tt.want {
				t.Fatalf("skipPseudoMount() = %t, want %t", got, tt.want)
			}
		})
	}
}
