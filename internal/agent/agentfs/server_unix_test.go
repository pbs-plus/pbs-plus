//go:build unix

package agentfs

import (
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/agent/snapshots"
)

func TestAbsRootSnapshot(t *testing.T) {
	s := &Server{snapshot: snapshots.Snapshot{Path: "/run/pbs-plus/snapshots/snap-1", MountPoint: "/"}}

	cases := map[string]string{
		"/":            "/run/pbs-plus/snapshots/snap-1",
		"":             "/run/pbs-plus/snapshots/snap-1",
		"/etc/fstab":   "/run/pbs-plus/snapshots/snap-1/etc/fstab",
		"var/lib/data": "/run/pbs-plus/snapshots/snap-1/var/lib/data",
	}
	for in, want := range cases {
		if got := s.abs(in); got != want {
			t.Fatalf("abs(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestAbsNestedSnapshot(t *testing.T) {
	s := &Server{snapshot: snapshots.Snapshot{Path: "/run/pbs-plus/snapshots/snap-2", MountPoint: "/var/lib/pg"}}

	cases := map[string]string{
		"/var/lib/pg":        "/run/pbs-plus/snapshots/snap-2",
		"/var/lib/pg/base/1": "/run/pbs-plus/snapshots/snap-2/base/1",
		"/var/lib/pgother/x": "/var/lib/pgother/x",
		"/var/lib":           "/var/lib",
		"/etc/fstab":         "/etc/fstab",
	}
	for in, want := range cases {
		if got := s.abs(in); got != want {
			t.Fatalf("abs(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestAbsDirectMode(t *testing.T) {
	s := &Server{snapshot: snapshots.Snapshot{Path: "/", Direct: true}}
	if got := s.abs("/etc/fstab"); got != "/etc/fstab" {
		t.Fatalf("abs = %q", got)
	}
	if got := s.abs("/"); got != "/" {
		t.Fatalf("abs = %q", got)
	}
}
